import os
import argparse
import shutil
import tempfile
import time
from distributed_core import (
    connect_db,
    update_batch_status,
    zip_batch,
    copy_zip_to_archive,
    validate_archive_dir,
    utc_now,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Start an archiver worker.")
    parser.add_argument(
        "--run-dir",
        default="download_run",
        help="Local run directory containing batch folders.",
    )
    parser.add_argument(
        "--archive-dir",
        required=True,
        help="Directory where final zips will be stored.",
    )
    parser.add_argument(
        "--db-url",
        default="",
        help="Postgres connection string. Defaults to DATABASE_URL env var.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=10,
        help="Seconds between scanning for new batches.",
    )
    parser.add_argument(
        "--keep-batch-dir",
        action="store_true",
        help="Keep batch folder after archiving.",
    )
    parser.add_argument(
        "--keep-local-zip",
        action="store_true",
        help="Keep local zip after copying to archive.",
    )
    return parser.parse_args()

def find_ready_batches(batches_dir, db_url):
    """Return list of (batch_id, batch_dir) ready to archive (status = 'downloaded')."""
    ready = []
    if not os.path.isdir(batches_dir):
        return ready

    # Connect to DB and find batches marked 'downloaded'
    if db_url:
        try:
            conn = connect_db(db_url)
            with conn.cursor() as cur:
                cur.execute("SELECT batch_id FROM batches WHERE status = 'downloaded'")
                rows = cur.fetchall()
            conn.close()
        except Exception as e:
            print(f"Warning: Could not query DB for downloaded batches: {e}")
            rows = []
        db_batch_ids = {row[0] for row in rows}
    else:
        db_batch_ids = set()

    # Only consider batches that exist locally
    for name in os.listdir(batches_dir):
        batch_dir = os.path.join(batches_dir, name)
        if not os.path.isdir(batch_dir):
            continue
        if name in db_batch_ids:
            ready.append((name, batch_dir))
    return ready

def archive_batch(batch_id, batch_dir, zips_dir, archive_dir, db_url, keep_batch_dir, keep_local_zip):
    try:
        # update DB status to 'archiving'
        if db_url:
            try:
                conn = connect_db(db_url)
                update_batch_status(conn, batch_id, {"status": "archiving",})
                conn.close()
            except Exception as e:
                print(f"Warning: Failed to update DB for batch {batch_id}: {e}")

        start_time = time.time()
        zip_path = os.path.join(zips_dir, f"{batch_id}.zip")
        zip_path = zip_batch(batch_dir, zip_path)
        zip_duration = time.time() - start_time
                
        # Cleanup batch folder
        start_time = time.time()
        if not keep_batch_dir:
            shutil.rmtree(batch_dir, ignore_errors=True)
        cleanup_duration = time.time() - start_time
        
        start_time = time.time()
        archive_path = copy_zip_to_archive(zip_path, archive_dir)
        copy_duration = time.time() - start_time

        # Cleanup local zip if successfully copied
        start_time = time.time()
        if not keep_local_zip and archive_path:
            try:
                if os.path.getsize(zip_path) == os.path.getsize(archive_path):
                    os.remove(zip_path)
            except Exception:
                pass
        cleanup_duration += time.time() - start_time

        if db_url:
            try:
                conn = connect_db(db_url)
                update_batch_status(
                    conn,
                    batch_id,
                    {
                        "status": "done",
                        "finished_at": utc_now(),
                        "zip_path": zip_path,
                        "archive_path": archive_path,
                        "last_error": None,
                    },
                )
                conn.close()
            except Exception as e:
                print(f"Warning: Failed to update DB for batch {batch_id}: {e}")


        print(f"Archived batch {batch_id} successfully: zip_time={zip_duration:.2f}s, copy_time={copy_duration:.2f}s, cleanup_time={cleanup_duration:.2f}s")

    except Exception as e:
        print(f"Error archiving batch {batch_id}: {e}")


def main():
    args = parse_args()

    archive_dir = validate_archive_dir(args.archive_dir)
    batches_dir = os.path.join(args.run_dir, "batches")
    zips_dir = os.path.join(args.run_dir, "zips")
    os.makedirs(batches_dir, exist_ok=True)
    os.makedirs(zips_dir, exist_ok=True)

    print("Archiver started, watching for downloaded batches...")

    while True:
        ready_batches = find_ready_batches(batches_dir, args.db_url or os.getenv("DATABASE_URL", ""))
        if not ready_batches:
            time.sleep(args.poll_interval)
            continue

        for batch_id, batch_dir in ready_batches:
            archive_batch(
                batch_id,
                batch_dir,
                zips_dir,
                archive_dir,
                args.db_url or os.getenv("DATABASE_URL", ""),
                args.keep_batch_dir,
                args.keep_local_zip,
            )
        time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()