import argparse
import os

import psycopg2


def connect_db(db_url):
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    return conn


def fetch_meta(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT key, value FROM meta")
        rows = cur.fetchall()
    return {row[0]: row[1] for row in rows}


def fetch_status_counts(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT status, COUNT(*) FROM videos GROUP BY status")
        rows = cur.fetchall()
    counts = {
        "pending": 0,
        "in_progress": 0,
        "success": 0,
        "failure": 0,
        "skipped": 0,
    }
    for status, count in rows:
        counts[status] = count
    return counts


def fetch_dataset_progress(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.name, d.priority, d.total_count,
                   SUM(CASE WHEN v.status IN ('success','failure','skipped') THEN 1 ELSE 0 END) AS done
            FROM datasets d
            LEFT JOIN videos v ON v.dataset_name = d.name
            GROUP BY d.name, d.priority, d.total_count
            ORDER BY d.priority ASC
            """
        )
        rows = cur.fetchall()
    return rows


def fetch_active_workers(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT worker_id, COUNT(*)
            FROM videos
            WHERE status = 'in_progress'
            GROUP BY worker_id
            ORDER BY COUNT(*) DESC
            """
        )
        rows = cur.fetchall()
    return rows


def fetch_recent_batches(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT batch_id, worker_id, status, created_at, finished_at,
                   success, failure, skipped, zip_path
            FROM batches
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return rows


def parse_args():
    parser = argparse.ArgumentParser(
        description="Show progress for distributed downloads."
    )
    parser.add_argument(
        "--db-url",
        default="",
        help="Postgres connection string. Defaults to DATABASE_URL env var.",
    )
    parser.add_argument(
        "--tail",
        type=int,
        default=5,
        help="Show the last N batches.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_url = args.db_url or os.getenv("DATABASE_URL", "")
    if not db_url:
        raise SystemExit("Missing --db-url or DATABASE_URL.")

    conn = connect_db(db_url)

    counts = fetch_status_counts(conn)
    total = sum(counts.values())
    done = counts["success"] + counts["failure"] + counts["skipped"]
    remaining = max(0, total - done)

    print("Distributed status")
    print(f"Total videos: {total}")
    print(
        "Done: {done} | Remaining: {remaining} | "
        "In progress: {in_progress} | Pending: {pending}"
        .format(
            done=done,
            remaining=remaining,
            in_progress=counts["in_progress"],
            pending=counts["pending"],
        )
    )
    print(
        "Success: {success} | Failure: {failure} | Skipped: {skipped}"
        .format(
            success=counts["success"],
            failure=counts["failure"],
            skipped=counts["skipped"],
        )
    )

    print("Datasets:")
    for name, _, total_count, done_count in fetch_dataset_progress(conn):
        done_count = int(done_count or 0)
        print(f" {name}: \t{done_count}/{total_count}")

    workers = fetch_active_workers(conn)
    if workers:
        print("Active workers:")
        for worker_id, count in workers:
            print(f" {worker_id}: \t{count}")
    else:
        print("Active workers: none")

    recent = fetch_recent_batches(conn, args.tail)
    if recent:
        print("Recent batches:")
        for row in recent:
            batch_id, worker_id, status, _, _, success, failure, skipped, zip_path = row
            print(
                " {batch_id}: \tworker={worker} status={status} success={success} "
                "failure={failure} skipped={skipped}"
                .format(
                    batch_id=batch_id,
                    worker=worker_id,
                    status=status,
                    success=success,
                    failure=failure,
                    skipped=skipped,
                    zip_path=zip_path,
                )
            )

    conn.close()

    # divider 
    print("="*75)

if __name__ == "__main__":
    main()
