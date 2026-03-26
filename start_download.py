import argparse
import json
import os
import socket
import threading
import time
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone

from scraper_utils import check_dependencies

from distributed_core import (
    build_existing_map,
    claim_videos,
    connect_db,
    create_batch_record,
    create_run,
    download_one,
    extend_batch_leases,
    ensure_db_ready,
    finish_run,
    get_batch_counts,
    log_run_event,
    record_run_error,
    release_blocked_video,
    release_videos_to_pending,
    update_batch_status,
    update_video_result,
    next_batch_id,
    utc_now,
)
from env_utils import load_env

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


def resolve_worker_id(run_dir, worker_id_arg):
    os.makedirs(run_dir, exist_ok=True)
    path = os.path.join(run_dir, "worker_id.txt")
    if worker_id_arg:
        with open(path, "w", encoding="utf-8") as f:
            f.write(worker_id_arg + "\n")
        return worker_id_arg
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            value = f.read().strip()
        if value:
            return value
    value = uuid.uuid4().hex
    with open(path, "w", encoding="utf-8") as f:
        f.write(value + "\n")
    return value




def load_block_state(run_dir, default_wait_seconds):
    os.makedirs(run_dir, exist_ok=True)
    path = os.path.join(run_dir, "block_wait_state.json")
    state = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                state = json.load(f) or {}
        except Exception:
            state = {}
    base_wait = int(state.get("base_wait_seconds", default_wait_seconds))
    next_wait = int(state.get("next_wait_seconds", base_wait))
    state["base_wait_seconds"] = max(1, base_wait)
    state["next_wait_seconds"] = max(1, next_wait)
    state["path"] = path
    return state


def save_block_state(state):
    path = state.get("path")
    if not path:
        return
    tmp_fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump({k: v for k, v in state.items() if k != "path"}, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def compute_lease_heartbeat_interval(lease_seconds):
    interval = max(30, int(lease_seconds * 0.5))
    if interval >= lease_seconds:
        interval = max(1, lease_seconds - 1)
    return interval




def log_event(db_url, run_id, level, message):
    conn = connect_db(db_url)
    log_run_event(conn, run_id, level, message)
    conn.close()


def start_lease_heartbeat(
    db_url, batch_id, worker_id, lease_seconds, interval_seconds, run_id
):
    stop_event = threading.Event()

    def _loop():
        while not stop_event.wait(interval_seconds):
            try:
                conn = connect_db(db_url)
                extend_batch_leases(conn, batch_id, worker_id, lease_seconds)
                conn.close()
            except Exception as exc:
                try:
                    conn = connect_db(db_url)
                    log_run_event(
                        conn,
                        run_id,
                        "warn",
                        f"Lease heartbeat failed for batch {batch_id}: {exc}",
                    )
                    conn.close()
                except Exception:
                    pass

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    return stop_event, thread


def probe_until_clear(db_url, worker_id, lease_seconds, max_attempts, run_id, args):
    probe_batch_id = f"probe_{worker_id}"
    state = load_block_state(args.run_dir, args.block_sleep_seconds)
    current_wait = state.get("next_wait_seconds", args.block_sleep_seconds)

    while True:
        state["last_wait_started_at"] = utc_now()
        save_block_state(state)
        log_event(
            db_url,
            run_id,
            "info",
            f"Bot-check sleep for {current_wait}s before probing.",
        )
        print(f"[BOT-BLOCK] Sleeping {current_wait}s. Current time (UTC): {utc_now()}")
        time.sleep(current_wait)
        state["last_wait_ended_at"] = utc_now()
        state["last_wait_seconds"] = int(current_wait)
        save_block_state(state)

        while True:
            conn = connect_db(db_url)
            ids = claim_videos(
                conn,
                worker_id,
                probe_batch_id,
                1,
                False,
                lease_seconds,
                max_attempts,
            )
            conn.close()

            if not ids:
                log_event(
                    db_url,
                    run_id,
                    "warn",
                    "Probe: no pending videos to test; sleeping again.",
                )
                current_wait = max(1, int(current_wait * 1.5))
                state["next_wait_seconds"] = current_wait
                save_block_state(state)
                break

            video_id = ids[0]
            print(f"[BOT-BLOCK] Probing video {video_id} at {utc_now()}")
            probe_dir = os.path.join(args.run_dir, "probe")
            probe_logs = os.path.join(args.run_dir, "probe_logs")
            os.makedirs(probe_dir, exist_ok=True)
            os.makedirs(probe_logs, exist_ok=True)
            result = download_one(video_id, probe_dir, probe_logs, False)

            if result["status"] == "blocked":
                conn = connect_db(db_url)
                release_blocked_video(conn, worker_id, video_id)
                conn.close()
                log_event(
                    db_url,
                    run_id,
                    "warn",
                    "Probe: bot-check still active; sleeping again.",
                )
                print(f"[BOT-BLOCK] Probe blocked; next sleep {current_wait}s")
                current_wait = max(1, int(current_wait * 1.5))
                state["next_wait_seconds"] = current_wait
                save_block_state(state)
                break

            if result["status"] == "success" and result.get("output_file"):
                conn = connect_db(db_url)
                update_video_result(conn, worker_id, result)
                conn.close()
                next_base = max(1, int(current_wait * 0.8))
                state["base_wait_seconds"] = next_base
                state["next_wait_seconds"] = next_base
                save_block_state(state)
                log_event(
                    db_url,
                    run_id,
                    "info",
                    f"Probe success; resuming downloads. Next base wait={next_base}s.",
                )
                print(f"[BOT-BLOCK] Probe success; resuming. Next base wait {next_base}s")
                return

            # Non-bot error: record and try another probe video immediately
            conn = connect_db(db_url)
            update_video_result(conn, worker_id, result)
            conn.close()
            log_event(
                db_url,
                run_id,
                "warn",
                f"Probe non-bot error for {video_id}; trying another video.",
            )
            print(f"[BOT-BLOCK] Probe non-bot error for {video_id}; trying another.")
            time.sleep(1)


def main():
    args = parse_args()
    if args.overlap_batches < 1:
        raise SystemExit("--overlap-batches must be >= 1.")

    db_url = args.db_url or os.getenv("DATABASE_URL", "")
    args.worker_id = resolve_worker_id(args.run_dir, args.worker_id)
    print(f"Worker ID: {args.worker_id}")
    if not db_url:
        raise SystemExit("Missing --db-url or DATABASE_URL.")

    conn = connect_db(db_url)
    ensure_db_ready(conn, args.batch_size, args.lease_seconds, args.max_attempts)
    conn.close()

    check_dependencies(allow_continue=True)

    run_id = (
        f"run_{args.worker_id}_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    host = socket.gethostname()
    pid = os.getpid()
    conn = connect_db(db_url)
    create_run(
        conn,
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
    log_run_event(
        conn,
        run_id,
        "info",
        (
            "Run started. "
            f"batch_size={args.batch_size} "
            f"workers={args.workers} "
            f"overlap_batches={args.overlap_batches}"
        ),
    )
    conn.close()

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

        def mark_no_more_videos():
            nonlocal no_more_logged
            if no_more_logged:
                return
            print("No more videos to claim.")
            conn = connect_db(db_url)
            log_run_event(conn, run_id, "info", "No more videos to claim.")
            conn.close()
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
                    conn = connect_db(db_url)
                    log_run_event(conn, run_id, "info", "Reached max batches for this run.")
                    conn.close()
                    max_batches_logged = True
                return None

            batch_id = next_batch_id(args.run_dir, args.worker_id)
            conn = connect_db(db_url)
            ids = claim_videos(
                conn,
                args.worker_id,
                batch_id,
                args.batch_size,
                args.retry_failures,
                args.lease_seconds,
                args.max_attempts,
            )
            conn.close()

            if not ids:
                no_more_to_claim = True
                mark_no_more_videos()
                return None

            batches_claimed += 1

            conn = connect_db(db_url)
            create_batch_record(conn, batch_id, args.worker_id, len(ids))
            conn.close()

            stop_event, heartbeat_thread = start_lease_heartbeat(
                db_url,
                batch_id,
                args.worker_id,
                args.lease_seconds,
                heartbeat_interval,
                run_id,
            )
            conn = connect_db(db_url)
            log_run_event(
                conn,
                run_id,
                "info",
                f"Lease heartbeat every {heartbeat_interval}s for batch {batch_id}.",
            )
            conn.close()

            batch_dir = os.path.join(batches_dir, batch_id)
            videos_dir = os.path.join(batch_dir, "videos")
            logs_dir = os.path.join(batch_dir, "logs")
            os.makedirs(videos_dir, exist_ok=True)
            os.makedirs(logs_dir, exist_ok=True)

            existing_map = build_existing_map(videos_dir)
            ids_to_download = []
            for video_id in ids:
                existing_file = existing_map.get(video_id)
                if existing_file:
                    conn = connect_db(db_url)
                    update_video_result(
                        conn,
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
                    conn.close()
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
                    conn = connect_db(db_url)
                    release_videos_to_pending(conn, args.worker_id, not_started)
                    conn.close()

            state["stop_event"].set()
            state["heartbeat_thread"].join(timeout=10)

            conn = connect_db(db_url)
            counts = get_batch_counts(conn, batch_id)
            status_value = "paused" if state["blocked_triggered"] else "downloaded"
            last_error = "bot_check_threshold" if state["blocked_triggered"] else None
            update_batch_status(
                conn,
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
            conn.close()

            print(
                f"{batch_id} done: success={counts.get('success', 0)} "
                f"failure={counts.get('failure', 0)} skipped={counts.get('skipped', 0)}"
            )
            conn = connect_db(db_url)
            log_run_event(
                conn,
                run_id,
                "info",
                f"Batch {batch_id} done: success={counts.get('success', 0)} "
                f"failure={counts.get('failure', 0)} skipped={counts.get('skipped', 0)}",
            )
            conn.close()

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
                            conn = connect_db(db_url)
                            release_blocked_video(conn, args.worker_id, vid)
                            conn.close()
                            log_event(
                                db_url,
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
                            conn = connect_db(db_url)
                            update_video_result(conn, args.worker_id, result)
                            conn.close()

                        if (
                            not state["blocked_triggered"]
                            and state["consecutive_blocked"] >= args.block_threshold
                        ):
                            state["blocked_triggered"] = True
                            log_event(
                                db_url,
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
                        db_url,
                        args.worker_id,
                        args.lease_seconds,
                        args.max_attempts,
                        run_id,
                        args,
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
        conn = connect_db(db_url)
        record_run_error(conn, run_id, "KeyboardInterrupt")
        conn.close()
    except Exception as exc:
        run_status = "failed"
        conn = connect_db(db_url)
        record_run_error(conn, run_id, f"Unhandled exception: {exc}")
        conn.close()
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
        conn = connect_db(db_url)
        finish_run(conn, run_id, run_status)
        conn.close()


if __name__ == "__main__":
    main()



