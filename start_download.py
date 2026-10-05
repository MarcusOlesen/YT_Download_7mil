import argparse
import os
import socket
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone

from terminal_logging import start_terminal_logging

from distributed_core import (
    build_run_dir_index,
    claim_videos,
    create_batch_record,
    create_run,
    download_one,
    ensure_db_ready,
    get_batch_counts,
    release_videos_to_pending,
    update_batch_status,
    update_video_result,
    next_batch_id,
    utc_now,
)
from env_utils import load_env
from worker_common import (
    DbSession,
    compute_lease_heartbeat_interval,
    configure_worker_pipeline,
    finish_run_safely,
    log_event,
    probe_until_clear,
    record_run_error_safely,
    release_blocked,
    resolve_worker_id,
    start_lease_heartbeat,
)

load_env()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Start a distributed download worker."
    )
    parser.add_argument(
        "--db-url",
        default="",
        help="Postgres connection string. Defaults to DATABASE_URL env var.",
    )
    parser.add_argument(
        "--worker-id",
        default="",
        help="Unique worker ID for this machine (optional).",
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Local run directory for this worker (for example D:\\yt_download_run).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Videos per claimed batch.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrent downloads per worker process.",
    )
    parser.add_argument(
        "--overlap-batches",
        type=int,
        default=2,
        help="Number of active batches allowed at once (1 = legacy sequential).",
    )
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=1800,
        help="Seconds before a lease expires.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Max attempts before giving up on a video.",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="Limit batches per run (0 = no limit).",
    )
    parser.add_argument(
        "--retry-failures",
        action="store_true",
        help="Retry failures when attempts remain.",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Skip downloads (yt-dlp test mode).",
    )
    parser.add_argument(
        "--log-dir",
        default="",
        help="Directory for captured terminal logs. Defaults to <run-dir>\\logs.",
    )
    parser.add_argument(
        "--block-threshold",
        type=int,
        default=20,
        help="Consecutive bot blocks before pausing.",
    )
    parser.add_argument(
        "--block-sleep-seconds",
        type=int,
        default=900,
        help="Sleep duration after bot block threshold.",
    )
    return parser.parse_args()



def main():
    args = parse_args()
    if args.overlap_batches < 1:
        raise SystemExit("--overlap-batches must be >= 1.")

    args.worker_id = resolve_worker_id(args.run_dir, args.worker_id)
    start_terminal_logging(
        "start_download",
        args.log_dir or os.path.join(args.run_dir, "logs"),
        args.worker_id,
    )
    db_url = args.db_url or os.getenv("DATABASE_URL", "")
    print(f"Worker ID: {args.worker_id}")
    if not db_url:
        raise SystemExit("Missing --db-url or DATABASE_URL.")

    db = DbSession(db_url)
    db.run(ensure_db_ready, args.batch_size, args.lease_seconds, args.max_attempts)

    try:
        profile = configure_worker_pipeline(args.run_dir, db_url)
    except RuntimeError as exc:
        raise SystemExit(str(exc))
    print(
        "Anti-block profile loaded: "
        f"preset={profile.get('preset')} "
        f"target_titles_per_hour={profile.get('target_titles_per_hour')}"
    )

    run_id = (
        f"run_{args.worker_id}_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    host = socket.gethostname()
    pid = os.getpid()
    db.run(
        create_run,
        run_id,
        args.worker_id,
        "start_download",
        host,
        pid,
        args.run_dir,
        None,
        args.batch_size,
        args.workers,
        args.lease_seconds,
        args.max_attempts,
    )
    log_event(
        db,
        run_id,
        "info",
        (
            "Run started. "
            f"batch_size={args.batch_size} "
            f"workers={args.workers} "
            f"overlap_batches={args.overlap_batches}"
        ),
    )

    batches_dir = os.path.join(args.run_dir, "batches")
    os.makedirs(batches_dir, exist_ok=True)

    run_status = "completed"
    active_batches = {}
    active_batch_order = []
    in_flight = {}
    no_more_to_claim = False
    no_more_logged = False
    max_batches_logged = False
    pending_probe = False
    batches_claimed = 0

    try:
        heartbeat_interval = compute_lease_heartbeat_interval(args.lease_seconds)
        # Files already downloaded anywhere in this run dir, so a video that
        # comes back to this machine is not downloaded twice.
        existing_index = build_run_dir_index(args.run_dir)
        print(f"Found {len(existing_index)} downloaded files in {args.run_dir}.")

        def mark_no_more_videos():
            nonlocal no_more_logged
            if no_more_logged:
                return
            print("No more videos to claim.")
            log_event(db, run_id, "info", "No more videos to claim.")
            no_more_logged = True

        def claim_new_batch():
            nonlocal no_more_to_claim
            nonlocal max_batches_logged
            nonlocal batches_claimed
            if no_more_to_claim:
                return None
            if args.max_batches and batches_claimed >= args.max_batches:
                no_more_to_claim = True
                if not max_batches_logged:
                    print("Reached max batches for this run.")
                    log_event(db, run_id, "info", "Reached max batches for this run.")
                    max_batches_logged = True
                return None

            batch_id = next_batch_id(args.run_dir, args.worker_id)
            ids = db.run(
                claim_videos,
                args.worker_id,
                batch_id,
                args.batch_size,
                args.retry_failures,
                args.lease_seconds,
                args.max_attempts,
            )

            if not ids:
                no_more_to_claim = True
                mark_no_more_videos()
                return None

            batches_claimed += 1

            db.run(create_batch_record, batch_id, args.worker_id, len(ids))

            stop_event, heartbeat_thread = start_lease_heartbeat(
                db_url,
                batch_id,
                args.worker_id,
                args.lease_seconds,
                heartbeat_interval,
                run_id,
            )
            log_event(
                db,
                run_id,
                "info",
                f"Lease heartbeat every {heartbeat_interval}s for batch {batch_id}.",
            )

            batch_dir = os.path.join(batches_dir, batch_id)
            videos_dir = os.path.join(batch_dir, "videos")
            logs_dir = os.path.join(batch_dir, "logs")
            os.makedirs(videos_dir, exist_ok=True)
            os.makedirs(logs_dir, exist_ok=True)

            ids_to_download = []
            for video_id in ids:
                existing_file = existing_index.get(video_id)
                if existing_file and os.path.exists(existing_file):
                    db.run(
                        update_video_result,
                        args.worker_id,
                        {
                            "id": video_id,
                            "status": "skipped",
                            "error": None,
                            "elapsed_sec": 0.0,
                            "output_file": existing_file,
                            "log_path": None,
                        },
                    )
                    continue
                ids_to_download.append(video_id)

            state = {
                "batch_id": batch_id,
                "videos_dir": videos_dir,
                "logs_dir": logs_dir,
                "ids_to_download": ids_to_download,
                "next_index": 0,
                "started_ids": set(),
                "in_flight": 0,
                "blocked_triggered": False,
                "consecutive_blocked": 0,
                "stop_event": stop_event,
                "heartbeat_thread": heartbeat_thread,
            }
            active_batches[batch_id] = state
            active_batch_order.append(batch_id)
            return state

        def maybe_claim_overlap_batch():
            if pending_probe or no_more_to_claim:
                return
            if len(active_batch_order) >= args.overlap_batches:
                return
            if active_batch_order:
                first = active_batches[active_batch_order[0]]
                first_queue_empty = first["next_index"] >= len(first["ids_to_download"])
                if not first_queue_empty:
                    return
            claim_new_batch()

        def submit_ready_work(pool):
            while len(in_flight) < args.workers:
                selected = None
                for batch_id in active_batch_order:
                    state = active_batches[batch_id]
                    if state["blocked_triggered"]:
                        continue
                    if state["next_index"] < len(state["ids_to_download"]):
                        selected = state
                        break
                if selected is None:
                    return

                vid = selected["ids_to_download"][selected["next_index"]]
                selected["next_index"] += 1
                future = pool.submit(
                    download_one,
                    vid,
                    selected["videos_dir"],
                    selected["logs_dir"],
                    args.test_mode,
                )
                in_flight[future] = (selected["batch_id"], vid)
                selected["started_ids"].add(vid)
                selected["in_flight"] += 1


        def finish_batch(state):
            nonlocal pending_probe
            batch_id = state["batch_id"]
            has_unscheduled = state["next_index"] < len(state["ids_to_download"])
            if state["blocked_triggered"] and has_unscheduled:
                not_started = state["ids_to_download"][state["next_index"] :]
                if not_started:
                    db.run(release_videos_to_pending, args.worker_id, not_started)

            state["stop_event"].set()
            state["heartbeat_thread"].join(timeout=10)

            counts = db.run(get_batch_counts, batch_id)
            status_value = "paused" if state["blocked_triggered"] else "downloaded"
            last_error = "bot_check_threshold" if state["blocked_triggered"] else None
            db.run(
                update_batch_status,
                batch_id,
                {
                    "status": status_value,
                    "finished_at": utc_now(),
                    "success": counts.get("success", 0),
                    "failure": counts.get("failure", 0),
                    "skipped": counts.get("skipped", 0),
                    "last_error": last_error,
                },
            )

            print(
                f"{batch_id} done: success={counts.get('success', 0)} "
                f"failure={counts.get('failure', 0)} skipped={counts.get('skipped', 0)}"
            )
            log_event(
                db,
                run_id,
                "info",
                f"Batch {batch_id} done: success={counts.get('success', 0)} "
                f"failure={counts.get('failure', 0)} skipped={counts.get('skipped', 0)}",
            )

            if state["blocked_triggered"]:
                pending_probe = True

            del active_batches[batch_id]
            active_batch_order.remove(batch_id)

        def finalize_ready_batches():
            for batch_id in list(active_batch_order):
                state = active_batches[batch_id]
                has_unscheduled = state["next_index"] < len(state["ids_to_download"])
                if state["in_flight"] > 0:
                    continue
                if has_unscheduled and not state["blocked_triggered"]:
                    continue
                finish_batch(state)

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            while True:
                if not active_batch_order and not no_more_to_claim and not pending_probe:
                    claim_new_batch()

                maybe_claim_overlap_batch()
                submit_ready_work(pool)

                if in_flight:
                    done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                    for future in done:
                        batch_id, vid = in_flight.pop(future)
                        state = active_batches.get(batch_id)
                        if state is None:
                            continue
                        state["in_flight"] = max(0, state["in_flight"] - 1)
                        result = future.result()

                        if result["status"] == "blocked":
                            state["consecutive_blocked"] += 1
                            release_blocked(db, args.worker_id, vid, result)
                            log_event(
                                db,
                                run_id,
                                "warn",
                                (
                                    f"Bot-check detected for {vid} "
                                    f"(streak {state['consecutive_blocked']})."
                                ),
                            )
                            print(
                                f"[BOT-BLOCK] Detected bot-check for {vid} "
                                f"(streak {state['consecutive_blocked']})."
                            )
                        else:
                            state["consecutive_blocked"] = 0
                            db.run(update_video_result, args.worker_id, result)
                            if result["status"] == "success" and result.get("output_file"):
                                existing_index[vid] = result["output_file"]

                        if (
                            not state["blocked_triggered"]
                            and state["consecutive_blocked"] >= args.block_threshold
                        ):
                            state["blocked_triggered"] = True
                            log_event(
                                db,
                                run_id,
                                "error",
                                (
                                    f"Bot-check threshold reached ({args.block_threshold}). "
                                    f"Pausing batch {batch_id}."
                                ),
                            )
                            print(f"[BOT-BLOCK] Threshold reached. Pausing batch {batch_id}.")

                finalize_ready_batches()

                if pending_probe and not active_batch_order and not in_flight:
                    probe_until_clear(
                        db,
                        args.worker_id,
                        args.lease_seconds,
                        args.max_attempts,
                        run_id,
                        args,
                        existing_index,
                    )
                    pending_probe = False
                    if not (args.max_batches and batches_claimed >= args.max_batches):
                        no_more_to_claim = False
                    continue

                if no_more_to_claim and not active_batch_order and not in_flight:
                    break

                if not in_flight:
                    time.sleep(0.1)

    except KeyboardInterrupt:
        run_status = "interrupted"
        record_run_error_safely(db, run_id, "KeyboardInterrupt")
    except Exception as exc:
        run_status = "failed"
        record_run_error_safely(db, run_id, f"Unhandled exception: {exc}")
        raise
    finally:
        for state in list(active_batches.values()):
            try:
                state["stop_event"].set()
            except Exception:
                pass
        for state in list(active_batches.values()):
            try:
                state["heartbeat_thread"].join(timeout=10)
            except Exception:
                pass
        finish_run_safely(db, run_id, run_status)
        db.close()


if __name__ == "__main__":
    main()
