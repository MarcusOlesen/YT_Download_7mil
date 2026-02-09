import argparse
import glob
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone

from distributed_core import connect_db, reap_expired_leases


def parse_args():
    parser = argparse.ArgumentParser(
        description="Monitor Postgres and create timestamped backups."
    )
    parser.add_argument(
        "--db-url",
        default="",
        help="Postgres connection string. Defaults to DATABASE_URL env var.",
    )
    parser.add_argument(
        "--backup-dir",
        default="db_backups",
        help="Directory to store timestamped pg_dump files.",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=60,
        help="Minutes between backups.",
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=3,
        help="Number of backups to keep (current + two older).",
    )
    parser.add_argument(
        "--pg-dump-path",
        default="",
        help="Path to pg_dump if not on PATH.",
    )
    parser.add_argument(
        "--reap",
        action="store_true",
        help="Reap expired leases before each backup.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single backup and exit.",
    )
    return parser.parse_args()


def check_db_live(db_url):
    conn = connect_db(db_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("SELECT now()")
        server_time = cur.fetchone()[0]
    conn.close()
    return server_time


def resolve_pg_dump(pg_dump_path):
    if pg_dump_path:
        return pg_dump_path
    resolved = shutil.which("pg_dump")
    if not resolved:
        raise RuntimeError("pg_dump not found on PATH.")
    return resolved


def run_backup(db_url, backup_dir, pg_dump_path):
    os.makedirs(backup_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    final_path = os.path.join(backup_dir, f"yt_downloads_{ts}.dump")
    temp_path = final_path + ".tmp"

    cmd = [pg_dump_path, "-F", "c", "-b", "-f", temp_path, db_url]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "pg_dump failed.")

    os.replace(temp_path, final_path)
    return final_path


def rotate_backups(backup_dir, keep):
    files = sorted(glob.glob(os.path.join(backup_dir, "*.dump")))
    if len(files) <= keep:
        return []
    to_delete = files[: len(files) - keep]
    for path in to_delete:
        os.remove(path)
    return to_delete


def main():
    args = parse_args()
    db_url = args.db_url or os.getenv("DATABASE_URL", "")
    if not db_url:
        raise SystemExit("Missing --db-url or DATABASE_URL.")

    pg_dump_path = resolve_pg_dump(args.pg_dump_path)

    while True:
        server_time = check_db_live(db_url)
        print(f"[{datetime.now(timezone.utc).isoformat()}] DB live. Server time: {server_time}")

        if args.reap:
            conn = connect_db(db_url)
            reaped = reap_expired_leases(conn)
            conn.close()
            print(f"Reaped {reaped} expired leases.")

        backup_path = run_backup(db_url, args.backup_dir, pg_dump_path)
        print(f"Backup created: {backup_path}")

        removed = rotate_backups(args.backup_dir, args.keep)
        for path in removed:
            print(f"Removed old backup: {path}")

        if args.once:
            break

        time.sleep(args.interval_minutes * 60)


if __name__ == "__main__":
    main()
