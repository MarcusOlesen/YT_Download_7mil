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
        default="download_run",
        help="Local run directory for batches/logs/zips.",
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
            time.sleep(1)


def main():
    args = parse_args()

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
        f"Run started. batch_size={args.batch_size} workers={args.workers}",
    )
    conn.close()

    batches_dir = os.path.join(args.run_dir, "batches")
    os.makedirs(batches_dir, exist_ok=True)

    batches_processed = 0
    run_status = "completed"

    try:
        while True:
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
                print("No more videos to claim.")
                conn = connect_db(db_url)
                log_run_event(conn, run_id, "info", "No more videos to claim.")
                conn.close()
                break

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

            blocked_triggered = False
            consecutive_blocked = 0
            try:
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
                                conn = connect_db(db_url)
                                release_blocked_video(conn, args.worker_id, vid)
                                conn.close()
                                log_event(db_url, run_id, "warn", f"Bot-check detected for {vid} (streak {consecutive_blocked}).")
                            else:
                                consecutive_blocked = 0
                                conn = connect_db(db_url)
                                update_video_result(conn, args.worker_id, result)
                                conn.close()

                            if not blocked_triggered and consecutive_blocked >= args.block_threshold:
                                blocked_triggered = True
                                log_event(db_url, run_id, "error", f"Bot-check threshold reached ({args.block_threshold}). Pausing batch {batch_id}.")

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
                        conn = connect_db(db_url)
                        release_videos_to_pending(conn, args.worker_id, not_started)
                        conn.close()

            finally:
                stop_event.set()
                heartbeat_thread.join(timeout=10)

            conn = connect_db(db_url)
            counts = get_batch_counts(conn, batch_id)
            status_value = "paused" if blocked_triggered else "downloaded"
            last_error = "bot_check_threshold" if blocked_triggered else None
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

            if blocked_triggered:
                probe_until_clear(
                    db_url,
                    args.worker_id,
                    args.lease_seconds,
                    args.max_attempts,
                    run_id,
                    args,
                )

            batches_processed += 1
            if args.max_batches and batches_processed >= args.max_batches:
                print("Reached max batches for this run.")
                break

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
