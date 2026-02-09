import argparse
import os
import shutil
import socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from scraper_utils import check_dependencies

from distributed_core import (
    build_existing_map,
    claim_failed_from_batch,
    connect_db,
    create_batch_record,
    create_run,
    download_one,
    extend_batch_leases,
    ensure_db_ready,
    finish_run,
    get_batch_counts,
    get_meta,
    log_run_event,
    record_run_error,
    update_batch_status,
    update_video_result,
    next_batch_id,
    utc_now,
)


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
        default="download_run",
        help="Local run directory for batches/logs/zips.",
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


def compute_lease_heartbeat_interval(lease_seconds):
    interval = max(30, int(lease_seconds * 0.5))
    if interval >= lease_seconds:
        interval = max(1, lease_seconds - 1)
    return interval


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


def main():
    args = parse_args()

    db_url = args.db_url or os.getenv("DATABASE_URL", "")
    if not db_url:
        raise SystemExit("Missing --db-url or DATABASE_URL.")

    args.worker_id = resolve_worker_id(args.run_dir, args.worker_id)
    print(f"Worker ID: {args.worker_id}")

    run_id = (
        f"run_{args.worker_id}_retry_{args.batch_id}_"
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
    log_run_event(
        conn,
        run_id,
        "info",
        f"Retry run started for batch {args.batch_id}. workers={args.workers}",
    )
    conn.close()

    conn = connect_db(db_url)
    existing_batch = get_meta(conn, "batch_size")
    if existing_batch is None:
        conn.close()
        raise SystemExit("Database not initialized. Run create_database.py first.")
    ensure_db_ready(
        conn,
        batch_size=existing_batch,
        lease_seconds=args.lease_seconds,
        max_attempts=args.max_attempts,
    )
    conn.close()

    check_dependencies(allow_continue=True)

    batch_id = next_batch_id(args.run_dir, args.worker_id, prefix="retry")

    run_status = "completed"

    try:
        conn = connect_db(db_url)
        ids = claim_failed_from_batch(
            conn,
            args.worker_id,
            args.batch_id,
            batch_id,
            args.lease_seconds,
            args.max_attempts,
        )
        conn.close()

        if not ids:
            print(f"No failed videos to retry for batch {args.batch_id}.")
            conn = connect_db(db_url)
            log_run_event(
                conn,
                run_id,
                "info",
                f"No failed videos to retry for batch {args.batch_id}.",
            )
            conn.close()
            return

        conn = connect_db(db_url)
        create_batch_record(conn, batch_id, args.worker_id, len(ids))
        conn.close()

        heartbeat_interval = compute_lease_heartbeat_interval(args.lease_seconds)
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

        try:
            batches_dir = os.path.join(args.run_dir, "batches")
            zips_dir = os.path.join(args.run_dir, "zips")
            os.makedirs(batches_dir, exist_ok=True)
            os.makedirs(zips_dir, exist_ok=True)

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

            if ids_to_download:
                with ThreadPoolExecutor(max_workers=args.workers) as pool:
                    futures = {
                        pool.submit(
                            download_one,
                            video_id,
                            videos_dir,
                            logs_dir,
                            args.test_mode,
                        ): video_id
                        for video_id in ids_to_download
                    }
                    for future in as_completed(futures):
                        result = future.result()
                        conn = connect_db(db_url)
                        update_video_result(conn, args.worker_id, result)
                        conn.close()
        finally:
            stop_event.set()
            heartbeat_thread.join(timeout=10)

        conn = connect_db(db_url)
        counts = get_batch_counts(conn, batch_id)
        update_batch_status(
            conn,
            batch_id,
            {
                "status": "downloaded",
                "finished_at": utc_now(),
                "success": counts.get("success", 0),
                "failure": counts.get("failure", 0),
                "skipped": counts.get("skipped", 0),
                "last_error": None,
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
            f"Retry batch {batch_id} done: success={counts.get('success', 0)} "
            f"failure={counts.get('failure', 0)} skipped={counts.get('skipped', 0)}",
        )
        conn.close()

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
        conn = connect_db(db_url)
        finish_run(conn, run_id, run_status)
        conn.close()


if __name__ == "__main__":
    main()
