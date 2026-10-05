import os
import re
import shutil
import tempfile
from pathlib import Path
import time
import traceback
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values

from scraper_utils import download_video


DEFAULT_DATASETS = [
    {"name": "ids_ok_sorted", "path": os.path.join("data", "ids_ok_sorted.parquet")},
    {"name": "ids_no_uploadinfo", "path": os.path.join("data", "ids_no_uploadinfo.parquet")},
    {"name": "ids_with_errors", "path": os.path.join("data", "ids_with_errors.parquet")},
]

GLOBAL_COOLDOWN_META_KEY = "global_cooldown_until_utc"

BOT_CHECK_PATTERN = re.compile(
    r"^ERROR: \[youtube\] [A-Za-z0-9_-]{11}: Sign in to confirm you'?re not a bot\b",
    re.IGNORECASE,
)

# A video that hits the bot check this many times is marked as a failure
# instead of going back to pending, so the probe loop cannot get stuck on it.
MAX_BLOCKS_PER_VIDEO = 10

BATCH_STATUS_COLUMNS = {
    "status",
    "started_at",
    "finished_at",
    "success",
    "failure",
    "skipped",
    "zip_path",
    "archive_path",
    "last_error",
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def parse_utc_timestamp(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def strip_ansi(text):
    if not text:
        return text
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def last_non_empty_line(text):
    if not text:
        return ""
    for line in reversed(text.splitlines()):
        if line.strip():
            return line.strip()
    return ""


def is_bot_blocked_log(ytdlp_log):
    log_text = ytdlp_log or ""
    if isinstance(log_text, str) and os.path.exists(log_text):
        try:
            log_text = Path(log_text).read_text(encoding="utf-8", errors="ignore")
        except Exception:
            log_text = ""
    line = last_non_empty_line(strip_ansi(log_text))
    if not line:
        return False
    # Normalize common apostrophe encodings
    line = line.replace("’", "'").replace("‘", "'")
    line = line.replace("???", "'").replace("???", "'")
    line = line.replace("�", "'")
    # Only the start of yt-dlp's message is matched. The hints that follow it
    # (cookie instructions and wiki links) change between yt-dlp releases.
    return bool(BOT_CHECK_PATTERN.match(line))


def atomic_write_text(text, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def connect_db(db_url, autocommit=False):
    conn = psycopg2.connect(db_url)
    conn.autocommit = autocommit
    return conn


def create_schema(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS datasets (
                    name TEXT PRIMARY KEY,
                    path TEXT NOT NULL,
                    total_count BIGINT NOT NULL,
                    priority INTEGER NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    worker_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    total INTEGER NOT NULL,
                    success INTEGER NOT NULL DEFAULT 0,
                    failure INTEGER NOT NULL DEFAULT 0,
                    skipped INTEGER NOT NULL DEFAULT 0,
                    zip_path TEXT,
                    archive_path TEXT,
                    last_error TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    worker_id TEXT NOT NULL,
                    script TEXT NOT NULL,
                    host TEXT,
                    pid INTEGER,
                    run_dir TEXT,
                    archive_dir TEXT,
                    batch_size INTEGER,
                    workers INTEGER,
                    lease_seconds INTEGER,
                    max_attempts INTEGER,
                    started_at TIMESTAMPTZ NOT NULL,
                    finished_at TIMESTAMPTZ,
                    status TEXT NOT NULL,
                    error_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS run_logs (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
                    ts TIMESTAMPTZ NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS videos (
                    id TEXT PRIMARY KEY,
                    dataset_name TEXT NOT NULL,
                    dataset_priority INTEGER NOT NULL,
                    row_index BIGINT NOT NULL,
                    status TEXT NOT NULL,
                    worker_id TEXT,
                    lease_until TIMESTAMPTZ,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    start_time TIMESTAMPTZ,
                    end_time TIMESTAMPTZ,
                    elapsed_sec REAL,
                    batch_id TEXT,
                    output_file TEXT,
                    log_path TEXT,
                    blocked_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            # Databases created before blocked_count existed get the column
            # here. The check avoids taking an exclusive lock on every start.
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'videos'
                  AND column_name = 'blocked_count'
                """
            )
            if cur.fetchone() is None:
                cur.execute(
                    "ALTER TABLE videos ADD COLUMN IF NOT EXISTS "
                    "blocked_count INTEGER NOT NULL DEFAULT 0"
                )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_videos_claim
                ON videos (status, lease_until, dataset_priority, row_index)
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_videos_worker ON videos(worker_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_videos_batch ON videos(batch_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_worker ON runs(worker_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_logs_run ON run_logs(run_id)"
            )


def get_meta(conn, key):
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM meta WHERE key = %s", (key,))
        row = cur.fetchone()
    if row:
        return row[0]
    return None


def set_meta(conn, key, value):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO meta (key, value) VALUES (%s, %s) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, str(value)),
            )


def ensure_meta(conn, key, value):
    existing = get_meta(conn, key)
    if existing is None:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO meta (key, value) VALUES (%s, %s)",
                    (key, str(value)),
                )
        return
    if str(existing) != str(value):
        raise RuntimeError(f"Meta mismatch for {key}.")


def get_global_cooldown_until(conn, key=GLOBAL_COOLDOWN_META_KEY):
    raw = get_meta(conn, key)
    if raw is None:
        return None
    try:
        return parse_utc_timestamp(raw)
    except Exception:
        return None


def extend_global_cooldown(conn, cooldown_seconds, key=GLOBAL_COOLDOWN_META_KEY):
    cooldown_seconds = int(cooldown_seconds or 0)
    if cooldown_seconds <= 0:
        return get_global_cooldown_until(conn, key=key)

    now = datetime.now(timezone.utc)
    requested_until = now + timedelta(seconds=cooldown_seconds)

    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM meta WHERE key = %s FOR UPDATE", (key,))
            row = cur.fetchone()
            existing_until = None
            if row and row[0]:
                try:
                    existing_until = parse_utc_timestamp(row[0])
                except Exception:
                    existing_until = None

            final_until = requested_until
            if existing_until and existing_until > requested_until:
                final_until = existing_until

            cur.execute(
                "INSERT INTO meta (key, value) VALUES (%s, %s) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, final_until.isoformat()),
            )

    return final_until

def create_run(
    conn,
    run_id,
    worker_id,
    script,
    host,
    pid,
    run_dir,
    archive_dir,
    batch_size,
    workers,
    lease_seconds,
    max_attempts,
):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO runs (
                    run_id, worker_id, script, host, pid, run_dir, archive_dir,
                    batch_size, workers, lease_seconds, max_attempts,
                    started_at, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
                """
                , (
                    run_id, worker_id, script, host, pid, run_dir, archive_dir,
                    batch_size, workers, lease_seconds, max_attempts, 'running'
                )
            )


def log_run_event(conn, run_id, level, message):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO run_logs (run_id, ts, level, message) VALUES (%s, now(), %s, %s)",
                (run_id, level, message),
            )


def record_run_error(conn, run_id, message):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE runs SET error_count = error_count + 1, last_error = %s WHERE run_id = %s",
                (message, run_id),
            )
    log_run_event(conn, run_id, 'error', message)


def finish_run(conn, run_id, status):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE runs SET status = %s, finished_at = now() WHERE run_id = %s",
                (status, run_id),
            )



def count_videos(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM videos")
        return cur.fetchone()[0]


def load_dataset(conn, name, path, priority):
    parquet = pq.ParquetFile(path)
    total = parquet.metadata.num_rows

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO datasets (name, path, total_count, priority)
                VALUES (%s, %s, %s, %s)
                """,
                (name, path, total, priority),
            )

    row_index = 0
    for row_group in range(parquet.num_row_groups):
        table = parquet.read_row_group(row_group, columns=["id"])
        ids = table.column(0).to_pylist()
        rows = [
            (video_id, name, priority, row_index + idx, "pending")
            for idx, video_id in enumerate(ids)
            if video_id
        ]
        if rows:
            with conn:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO videos
                        (id, dataset_name, dataset_priority, row_index, status)
                        VALUES %s
                        ON CONFLICT (id) DO NOTHING
                        """,
                        rows,
                        page_size=1000,
                    )
        row_index += len(ids)


def init_db(conn, datasets, batch_size, lease_seconds, max_attempts):
    create_schema(conn)
    ensure_meta(conn, "created_at", utc_now())
    ensure_meta(conn, "batch_size", batch_size)
    ensure_meta(conn, "lease_seconds", lease_seconds)
    ensure_meta(conn, "max_attempts", max_attempts)

    if count_videos(conn) > 0:
        raise RuntimeError("Videos table already populated.")

    for priority, ds in enumerate(datasets):
        if not os.path.exists(ds["path"]):
            raise RuntimeError(f"Missing dataset file: {ds['path']}")
        load_dataset(conn, ds["name"], ds["path"], priority)


def ensure_db_ready(conn, batch_size, lease_seconds, max_attempts):
    create_schema(conn)
    existing_batch = get_meta(conn, "batch_size")
    if existing_batch is None:
        ensure_meta(conn, "batch_size", batch_size)
    elif str(existing_batch) != str(batch_size):
        set_meta(conn, "batch_size", batch_size)
    ensure_meta(conn, "lease_seconds", lease_seconds)
    ensure_meta(conn, "max_attempts", max_attempts)
    if count_videos(conn) == 0:
        raise RuntimeError("Videos table is empty. Run create_database.py once.")


def reap_expired_leases(conn):
    """Release expired leases. Returns (back_to_pending, marked_failed).

    A row that has used all its attempts is marked 'failure' instead of going
    back to 'pending', where claim_videos would never pick it up again.
    max_attempts is read from the meta table; without it every row goes back
    to pending.
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH limits AS (
                    SELECT COALESCE(
                        (SELECT value::integer FROM meta WHERE key = 'max_attempts'),
                        2147483647
                    ) AS max_attempts
                ),
                expired AS (
                    SELECT v.id, v.attempts >= limits.max_attempts AS exhausted
                    FROM videos v, limits
                    WHERE v.status = 'in_progress' AND v.lease_until < now()
                    FOR UPDATE OF v SKIP LOCKED
                )
                UPDATE videos v
                SET status = CASE WHEN expired.exhausted THEN 'failure' ELSE 'pending' END,
                    worker_id = CASE WHEN expired.exhausted THEN v.worker_id END,
                    batch_id = CASE WHEN expired.exhausted THEN v.batch_id END,
                    last_error = CASE WHEN expired.exhausted THEN 'lease_expired' ELSE v.last_error END,
                    end_time = CASE WHEN expired.exhausted THEN now() ELSE v.end_time END,
                    lease_until = NULL
                FROM expired
                WHERE v.id = expired.id
                RETURNING expired.exhausted
                """
            )
            rows = cur.fetchall()
    failed = sum(1 for (exhausted,) in rows if exhausted)
    return len(rows) - failed, failed


def claim_videos(
    conn,
    worker_id,
    batch_id,
    limit,
    retry_failures,
    lease_seconds,
    max_attempts,
):
    status_clause = "status = 'pending'"
    if retry_failures:
        status_clause = "status IN ('pending', 'failure')"

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH claim AS (
                    SELECT id
                    FROM videos
                    WHERE {status_clause}
                      AND attempts < %s
                    ORDER BY dataset_priority ASC, row_index ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE videos v
                SET status = 'in_progress',
                    worker_id = %s,
                    lease_until = now() + (%s * interval '1 second'),
                    start_time = now(),
                    batch_id = %s,
                    last_error = NULL,
                    attempts = attempts + 1
                FROM claim
                WHERE v.id = claim.id
                RETURNING v.id
                """,
                (max_attempts, limit, worker_id, lease_seconds, batch_id),
            )
            rows = cur.fetchall()
    return [row[0] for row in rows]


def claim_failed_from_batch(
    conn,
    worker_id,
    source_batch_id,
    new_batch_id,
    lease_seconds,
    max_attempts,
):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH claim AS (
                    SELECT id
                    FROM videos
                    WHERE batch_id = %s
                      AND status = 'failure'
                      AND attempts < %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE videos v
                SET status = 'in_progress',
                    worker_id = %s,
                    lease_until = now() + (%s * interval '1 second'),
                    start_time = now(),
                    batch_id = %s,
                    last_error = NULL,
                    attempts = attempts + 1
                FROM claim
                WHERE v.id = claim.id
                RETURNING v.id
                """,
                (
                    source_batch_id,
                    max_attempts,
                    worker_id,
                    lease_seconds,
                    new_batch_id,
                ),
            )
            rows = cur.fetchall()
    return [row[0] for row in rows]


def create_batch_record(conn, batch_id, worker_id, total):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO batches
                (batch_id, worker_id, status, created_at, started_at, total)
                VALUES (%s, %s, %s, now(), now(), %s)
                """,
                (batch_id, worker_id, "downloading", total),
            )


def update_batch_status(conn, batch_id, fields):
    if not fields:
        return
    unknown = set(fields) - BATCH_STATUS_COLUMNS
    if unknown:
        raise ValueError(f"Unknown batch columns: {sorted(unknown)}")
    columns = ", ".join(f"{key} = %s" for key in fields)
    values = list(fields.values()) + [batch_id]
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE batches SET {columns} WHERE batch_id = %s",
                values,
            )


def get_batch_counts(conn, batch_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, COUNT(*) FROM videos
            WHERE batch_id = %s
            GROUP BY status
            """,
            (batch_id,),
        )
        rows = cur.fetchall()
    counts = {
        "success": 0,
        "failure": 0,
        "skipped": 0,
        "pending": 0,
        "in_progress": 0,
    }
    for status, count in rows:
        counts[status] = count
    return counts


def build_existing_map(videos_dir):
    existing = {}
    if not os.path.isdir(videos_dir):
        return existing
    for name in os.listdir(videos_dir):
        if name.endswith(".part"):
            continue
        if "." not in name:
            continue
        video_id = name.split(".", 1)[0]
        existing[video_id] = os.path.join(videos_dir, name)
    return existing


def build_run_dir_index(run_dir):
    """Map video ID to file for every finished download under run_dir.

    Covers all batches/<id>/videos folders and the probe folder, so a video
    that comes back to this machine in a later batch is not downloaded again.
    """
    index = {}
    batches_dir = os.path.join(run_dir, "batches")
    if os.path.isdir(batches_dir):
        for entry in os.scandir(batches_dir):
            if entry.is_dir():
                index.update(build_existing_map(os.path.join(entry.path, "videos")))
    index.update(build_existing_map(os.path.join(run_dir, "probe")))
    return index


def find_existing_video(videos_dir, video_id):
    if not os.path.isdir(videos_dir):
        return None
    prefix = f"{video_id}."
    for name in os.listdir(videos_dir):
        if not name.startswith(prefix):
            continue
        if name.endswith(".part"):
            continue
        return os.path.join(videos_dir, name)
    return None


def download_one(video_id, videos_dir, logs_dir, test_mode):
    start_ts = time.time()
    status = "failure"
    error = None
    log_path = None
    output_file = None

    try:
        status, error, ytdlp_log = download_video(
            video_id, videos_dir, test=test_mode
        )
        if is_bot_blocked_log(ytdlp_log):
            status = "blocked"
            error = None
        output_file = find_existing_video(videos_dir, video_id)
        if status != "success" and ytdlp_log:
            log_path = os.path.join(logs_dir, f"{video_id}.log")
            atomic_write_text(ytdlp_log, log_path)
    except Exception as exc:
        error = f"exception: {exc}"
        log_path = os.path.join(logs_dir, f"{video_id}.log")
        atomic_write_text(traceback.format_exc(), log_path)

    elapsed_sec = round(time.time() - start_ts, 3)

    return {
        "id": video_id,
        "status": status,
        "error": error,
        "elapsed_sec": elapsed_sec,
        "output_file": output_file,
        "log_path": log_path,
    }


def release_videos_to_pending(conn, worker_id, ids, decrement_attempts=True):
    if not ids:
        return 0
    attempts_expr = (
        "GREATEST(attempts - 1, 0)" if decrement_attempts else "attempts"
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE videos
                SET status = 'pending',
                    worker_id = NULL,
                    lease_until = NULL,
                    batch_id = NULL,
                    last_error = NULL,
                    start_time = NULL,
                    end_time = NULL,
                    elapsed_sec = NULL,
                    attempts = {attempts_expr}
                WHERE worker_id = %s AND id = ANY(%s)
                """,
                (worker_id, ids),
            )
            return cur.rowcount


def release_blocked_video(conn, worker_id, video_id, reason=None,
                          max_blocks=MAX_BLOCKS_PER_VIDEO):
    """Hand a bot-blocked video back to the queue.

    A bot check is a temporary block on this machine's IP, not a problem with
    the video, so the attempt is given back and the block is counted in
    blocked_count instead. After max_blocks blocks the video is marked
    'failure' so it cannot hold up the probe loop forever.
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE videos
                SET status = CASE WHEN blocked_count + 1 >= %s
                                  THEN 'failure' ELSE 'pending' END,
                    worker_id = CASE WHEN blocked_count + 1 >= %s
                                     THEN worker_id END,
                    batch_id = CASE WHEN blocked_count + 1 >= %s
                                    THEN batch_id END,
                    lease_until = NULL,
                    last_error = %s,
                    start_time = NULL,
                    end_time = NULL,
                    elapsed_sec = NULL,
                    attempts = GREATEST(attempts - 1, 0),
                    blocked_count = blocked_count + 1
                WHERE worker_id = %s AND id = %s
                """,
                (
                    max_blocks,
                    max_blocks,
                    max_blocks,
                    reason or "bot_check",
                    worker_id,
                    video_id,
                ),
            )
            return cur.rowcount


def update_video_result(conn, worker_id, result):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE videos
                SET status = %s,
                    last_error = %s,
                    end_time = now(),
                    elapsed_sec = %s,
                    output_file = %s,
                    log_path = %s,
                    lease_until = NULL
                WHERE id = %s AND worker_id = %s
                """,
                (
                    result["status"],
                    result.get("error"),
                    result.get("elapsed_sec"),
                    result.get("output_file"),
                    result.get("log_path"),
                    result["id"],
                    worker_id,
                ),
            )


def extend_batch_leases(conn, batch_id, worker_id, lease_seconds):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE videos
                SET lease_until = now() + (%s * interval '1 second')
                WHERE batch_id = %s
                  AND worker_id = %s
                  AND status = 'in_progress'
                """,
                (lease_seconds, batch_id, worker_id),
            )
            return cur.rowcount

def zip_batch(batch_dir, zip_path):
    import zipfile

    if os.path.exists(zip_path):
        return zip_path

    os.makedirs(os.path.dirname(zip_path), exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        dir=os.path.dirname(zip_path), suffix=".tmp"
    )
    os.close(fd)
    try:
        with zipfile.ZipFile(
            temp_path, "w", compression=zipfile.ZIP_STORED
        ) as zf:
            parent = os.path.dirname(batch_dir)
            for root, _, files in os.walk(batch_dir):
                for name in files:
                    full_path = os.path.join(root, name)
                    arcname = os.path.relpath(full_path, parent)
                    zf.write(full_path, arcname)
        os.replace(temp_path, zip_path)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    return zip_path


def copy_zip_to_archive(zip_path, archive_dir):
    if not archive_dir:
        return None
    os.makedirs(archive_dir, exist_ok=True)
    archive_path = os.path.join(archive_dir, os.path.basename(zip_path))
    fd, temp_path = tempfile.mkstemp(dir=archive_dir, suffix=".tmp")
    os.close(fd)
    try:
        shutil.copy2(zip_path, temp_path)
        os.replace(temp_path, archive_path)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    return archive_path

def validate_archive_dir(archive_dir):
    if not archive_dir:
        raise ValueError("archive_dir is required.")
    os.makedirs(archive_dir, exist_ok=True)
    if not os.path.isdir(archive_dir):
        raise RuntimeError(f"Archive path is not a directory: {archive_dir}")
    fd, temp_path = tempfile.mkstemp(dir=archive_dir, suffix=".tmp")
    os.close(fd)
    os.unlink(temp_path)
    return archive_dir

def next_batch_id(run_dir, worker_id, prefix=""):
    os.makedirs(run_dir, exist_ok=True)
    suffix = f"_{prefix}" if prefix else ""
    counter_path = os.path.join(run_dir, f"batch_counter{suffix}.txt")
    try:
        with open(counter_path, "r", encoding="utf-8") as f:
            counter = int(f.read().strip())
    except Exception:
        counter = 0
    counter += 1
    fd, temp_path = tempfile.mkstemp(dir=run_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(str(counter) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, counter_path)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    prefix_str = f"_{prefix}" if prefix else ""
    return f"{worker_id}{prefix_str}_{counter:05d}"




