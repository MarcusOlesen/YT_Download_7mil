"""Plumbing shared by start_download.py and rerun_failed_batch.py."""

import json
import os
import tempfile
import threading
import time
import uuid

import psycopg2.extensions

from distributed_core import (
    claim_videos,
    connect_db,
    download_one,
    extend_batch_leases,
    extend_global_cooldown,
    finish_run,
    get_global_cooldown_until,
    log_run_event,
    record_run_error,
    release_blocked_video,
    update_video_result,
    utc_now,
)
from scraper_utils import initialize_worker_pipeline


class DbSession:
    """A reusable database connection.

    The connection is opened on first use and kept open. Any error closes it
    and is raised to the caller unchanged; the next call opens a new one.
    Calls are never retried. A transaction left open by a read-only helper is
    rolled back after each call, so the connection never sits idle in a
    transaction. The lock makes one session safe to share between threads.
    """

    def __init__(self, db_url):
        self.db_url = db_url
        self._conn = None
        self._lock = threading.Lock()

    def run(self, fn, *args, **kwargs):
        with self._lock:
            if self._conn is None or self._conn.closed:
                self._conn = connect_db(self.db_url)
            conn = self._conn
            try:
                result = fn(conn, *args, **kwargs)
                if conn.status != psycopg2.extensions.STATUS_READY:
                    conn.rollback()
            except BaseException:
                self._discard()
                raise
            return result

    def close(self):
        with self._lock:
            self._discard()

    def _discard(self):
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def run_safely(db, description, fn, *args):
    """Best-effort bookkeeping call that must not hide the original error."""
    try:
        return db.run(fn, *args)
    except Exception as exc:
        print(f"[WARN] Could not {description} in the database: {exc}")
        return None


def log_event(db, run_id, level, message):
    db.run(log_run_event, run_id, level, message)


def record_run_error_safely(db, run_id, message):
    run_safely(db, "record the run error", record_run_error, run_id, message)


def finish_run_safely(db, run_id, status):
    run_safely(db, f"mark run {run_id} as {status}", finish_run, run_id, status)


def configure_worker_pipeline(run_dir, db_url):
    # One connection, shared by all download threads, for the cooldown key.
    cooldown_db = DbSession(db_url)

    def _read_shared_cooldown():
        return cooldown_db.run(get_global_cooldown_until)

    def _write_shared_cooldown(cooldown_seconds):
        return cooldown_db.run(extend_global_cooldown, cooldown_seconds)

    return initialize_worker_pipeline(
        run_dir,
        shared_cooldown_reader=_read_shared_cooldown,
        shared_cooldown_writer=_write_shared_cooldown,
    )


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


def start_lease_heartbeat(
    db_url, batch_id, worker_id, lease_seconds, interval_seconds, run_id
):
    stop_event = threading.Event()

    def _loop():
        db = DbSession(db_url)
        try:
            while not stop_event.wait(interval_seconds):
                try:
                    db.run(extend_batch_leases, batch_id, worker_id, lease_seconds)
                except Exception as exc:
                    run_safely(
                        db,
                        "log the heartbeat failure",
                        log_run_event,
                        run_id,
                        "warn",
                        f"Lease heartbeat failed for batch {batch_id}: {exc}",
                    )
        finally:
            db.close()

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    return stop_event, thread


def release_blocked(db, worker_id, video_id, result):
    """Return a bot-blocked video to the queue (see release_blocked_video)."""
    db.run(release_blocked_video, worker_id, video_id, result.get("error") or "bot_check")


def probe_until_clear(db, worker_id, lease_seconds, max_attempts, run_id, args,
                      existing_index=None):
    """Sleep, then try one video at a time until a download gets through.

    The wait grows by 1.5x after each blocked probe and the next base wait is
    0.8x the wait that worked. Waits are kept in block_wait_state.json so a
    restarted worker continues where it left off.
    """
    probe_batch_id = f"probe_{worker_id}"
    state = load_block_state(args.run_dir, args.block_sleep_seconds)
    current_wait = state.get("next_wait_seconds", args.block_sleep_seconds)

    while True:
        state["last_wait_started_at"] = utc_now()
        save_block_state(state)
        log_event(db, run_id, "info", f"Bot-check sleep for {current_wait}s before probing.")
        print(f"[BOT-BLOCK] Sleeping {current_wait}s. Current time (UTC): {utc_now()}")
        time.sleep(current_wait)
        state["last_wait_ended_at"] = utc_now()
        state["last_wait_seconds"] = int(current_wait)
        save_block_state(state)

        while True:
            ids = db.run(
                claim_videos,
                worker_id,
                probe_batch_id,
                1,
                False,
                lease_seconds,
                max_attempts,
            )

            if not ids:
                log_event(db, run_id, "warn", "Probe: no pending videos to test; sleeping again.")
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
                release_blocked(db, worker_id, video_id, result)
                log_event(db, run_id, "warn", "Probe: bot-check still active; sleeping again.")
                current_wait = max(1, int(current_wait * 1.5))
                print(f"[BOT-BLOCK] Probe blocked; next sleep {current_wait}s")
                state["next_wait_seconds"] = current_wait
                save_block_state(state)
                break

            if result["status"] == "success" and result.get("output_file"):
                db.run(update_video_result, worker_id, result)
                if existing_index is not None:
                    existing_index[video_id] = result["output_file"]
                next_base = max(1, int(current_wait * 0.8))
                state["base_wait_seconds"] = next_base
                state["next_wait_seconds"] = next_base
                save_block_state(state)
                log_event(
                    db,
                    run_id,
                    "info",
                    f"Probe success; resuming downloads. Next base wait={next_base}s.",
                )
                print(f"[BOT-BLOCK] Probe success; resuming. Next base wait {next_base}s")
                return

            # Non-bot error: record and try another probe video immediately
            db.run(update_video_result, worker_id, result)
            log_event(db, run_id, "warn", f"Probe non-bot error for {video_id}; trying another video.")
            print(f"[BOT-BLOCK] Probe non-bot error for {video_id}; trying another.")
            time.sleep(1)
