import argparse
import os
import socket
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone

from distributed_core import (
    build_run_dir_index,
    claim_failed_from_batch,
    create_batch_record,
    create_run,
    download_one,
    ensure_db_ready,
    get_batch_counts,
    get_meta,
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
        description="Re-run failed videos from a specific batch."
    )
    parser.add_argument(
        "--db-url",
        default="",
        help="Postgres connection string. Defaults to DATABASE_URL env var.",
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Batch ID to retry failed videos from.",
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
        "--workers",
        type=int,
        default=4,
        help="Concurrent downloads per batch.",
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



def main():
    args = parse_args()

    db_url = args.db_url or os.getenv("DATABASE_URL", "")
    if not db_url:
        raise SystemExit("Missing --db-url or DATABASE_URL.")

    args.worker_id = resolve_worker_id(args.run_dir, args.worker_id)
    print(f"Worker ID: {args.worker_id}")

    db = DbSession(db_url)
    existing_batch = db.run(get_meta, "batch_size")
    if existing_batch is None:
        raise SystemExit("Database not initialized. Run create_database.py first.")
    db.run(
        ensure_db_ready,
        batch_size=existing_batch,
        lease_seconds=args.lease_seconds,
        max_attempts=args.max_attempts,
    )

    run_id = (
        f"run_{args.worker_id}_retry_{args.batch_id}_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    host = socket.gethostname()
    pid = os.getpid()
    db.run(
        create_run,
        run_id,
        args.worker_id,
        "rerun_failed_batch",
        host,
        pid,
        args.run_dir,
        None,
        None,
        args.workers,
        args.lease_seconds,
        args.max_attempts,
    )
    log_event(
        db,
        run_id,
        "info",
        f"Retry run started for batch {args.batch_id}. workers={args.workers}",
    )

    run_status = "completed"

    try:
        try:
            profile = configure_worker_pipeline(args.run_dir, db_url)
        except RuntimeError as exc:
            raise SystemExit(str(exc))
        print(
            "Anti-block profile loaded: "
            f"preset={profile.get('preset')} "
            f"target_titles_per_hour={profile.get('target_titles_per_hour')}"
        )

        batch_id = next_batch_id(args.run_dir, args.worker_id, prefix="retry")
        existing_index = build_run_dir_index(args.run_dir)

        ids = db.run(
            claim_failed_from_batch,
            args.worker_id,
            args.batch_id,
            batch_id,
            args.lease_seconds,
            args.max_attempts,
        )

        if not ids:
            print(f"No failed videos to retry for batch {args.batch_id}.")
            log_event(db, run_id, "info", f"No failed videos to retry for batch {args.batch_id}.")
            return

        db.run(create_batch_record, batch_id, args.worker_id, len(ids))

        heartbeat_interval = compute_lease_heartbeat_interval(args.lease_seconds)
        stop_event, heartbeat_thread = start_lease_heartbeat(
            db_url,
            batch_id,
            args.worker_id,
            args.lease_seconds,
            heartbeat_interval,
            run_id,
        )
        log_event(db, run_id, "info", f"Lease heartbeat every {heartbeat_interval}s for batch {batch_id}.")

        blocked_triggered = False
        consecutive_blocked = 0
        try:
            batch_dir = os.path.join(args.run_dir, "batches", batch_id)
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

            started_ids = set()
            in_flight = {}
            iterator = iter(ids_to_download)

            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                for _ in range(args.workers):
                    try:
                        vid = next(iterator)
                    except StopIteration:
                        break
                    future = pool.submit(
                        download_one,
                        vid,
                        videos_dir,
                        logs_dir,
                        args.test_mode,
                    )
                    in_flight[future] = vid
                    started_ids.add(vid)

                while in_flight:
                    done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                    for future in done:
                        vid = in_flight.pop(future)
                        result = future.result()

                        if result["status"] == "blocked":
                            consecutive_blocked += 1
                            release_blocked(db, args.worker_id, vid, result)
                            log_event(db, run_id, "warn", f"Bot-check detected for {vid} (streak {consecutive_blocked}).")
                        else:
                            consecutive_blocked = 0
                            db.run(update_video_result, args.worker_id, result)
                            if result["status"] == "success" and result.get("output_file"):
                                existing_index[vid] = result["output_file"]

                        if not blocked_triggered and consecutive_blocked >= args.block_threshold:
                            blocked_triggered = True
                            log_event(db, run_id, "error", f"Bot-check threshold reached ({args.block_threshold}). Pausing batch {batch_id}.")

                        if not blocked_triggered:
                            try:
                                vid = next(iterator)
                            except StopIteration:
                                continue
                            future = pool.submit(
                                download_one,
                                vid,
                                videos_dir,
                                logs_dir,
                                args.test_mode,
                            )
                            in_flight[future] = vid
                            started_ids.add(vid)

            if blocked_triggered:
                not_started = [vid for vid in ids_to_download if vid not in started_ids]
                if not_started:
                    db.run(release_videos_to_pending, args.worker_id, not_started)

        finally:
            stop_event.set()
            heartbeat_thread.join(timeout=10)

        counts = db.run(get_batch_counts, batch_id)
        status_value = "paused" if blocked_triggered else "downloaded"
        last_error = "bot_check_threshold" if blocked_triggered else None
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
            f"Retry batch {batch_id} done: success={counts.get('success', 0)} "
            f"failure={counts.get('failure', 0)} skipped={counts.get('skipped', 0)}",
        )

        if blocked_triggered:
            probe_until_clear(
                db,
                args.worker_id,
                args.lease_seconds,
                args.max_attempts,
                run_id,
                args,
                existing_index,
            )

    except KeyboardInterrupt:
        run_status = "interrupted"
        record_run_error_safely(db, run_id, "KeyboardInterrupt")
    except SystemExit:
        run_status = "failed"
        raise
    except Exception as exc:
        run_status = "failed"
        record_run_error_safely(db, run_id, f"Unhandled exception: {exc}")
        raise
    finally:
        finish_run_safely(db, run_id, run_status)
        db.close()


if __name__ == "__main__":
    main()
