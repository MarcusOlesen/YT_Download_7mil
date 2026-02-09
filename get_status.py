import argparse
import os

import psycopg2

# Hardcode dataset totals to avoid reading the datasets table.
DATASET_TOTALS = [
    ("ids_ok_sorted", 5727606),
    ("ids_no_uploadinfo", 1768),
    ("ids_with_errors", 1264355),
]


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


def fetch_dataset_totals(conn):
    if DATASET_TOTALS:
        return [(name, idx, total) for idx, (name, total) in enumerate(DATASET_TOTALS)]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, priority, total_count FROM datasets ORDER BY priority ASC"
        )
        rows = cur.fetchall()
    return rows


def fetch_dataset_done(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dataset_name,
                   SUM(CASE WHEN status IN ('success','failure','skipped')
                            THEN 1 ELSE 0 END) AS done
            FROM videos
            GROUP BY dataset_name
            """
        )
        rows = cur.fetchall()
    return {row[0]: int(row[1] or 0) for row in rows}


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
            SELECT batch_id, worker_id, status, created_at, finished_at, total, last_error
            FROM batches
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return rows


def fetch_batch_counts(conn, batch_ids):
    if not batch_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT batch_id, status, COUNT(*)
            FROM videos
            WHERE batch_id = ANY(%s)
            GROUP BY batch_id, status
            """,
            (batch_ids,),
        )
        rows = cur.fetchall()
    counts = {}
    for batch_id, status, count in rows:
        bucket = counts.setdefault(
            batch_id,
            {
                "pending": 0,
                "in_progress": 0,
                "success": 0,
                "failure": 0,
                "skipped": 0,
            },
        )
        bucket[status] = count
    return counts


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
    overall_pct = (done / total * 100.0) if total else 0.0

    print("Distributed status")
    print(f"Total videos: {total}")
    print(
        "Done: {done} ({pct:.2f}%) | Remaining: {remaining} | "
        "In progress: {in_progress} | Pending: {pending}"
        .format(
            done=done,
            pct=overall_pct,
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

    totals = fetch_dataset_totals(conn)
    done_map = fetch_dataset_done(conn)
    if totals:
        print("Datasets:")
        for name, _, total_count in totals:
            total_count = int(total_count or 0)
            done_count = int(done_map.get(name, 0))
            pct = (done_count / total_count * 100.0) if total_count else 0.0
            print(f" {name}: \t{done_count}/{total_count} ({pct:.2f}%)")

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
        batch_ids = [row[0] for row in recent]
        counts_by_batch = fetch_batch_counts(conn, batch_ids)
        for row in recent:
            batch_id, worker_id, status, _, _, total_count, last_error = row
            counts = counts_by_batch.get(
                batch_id,
                {
                    "pending": 0,
                    "in_progress": 0,
                    "success": 0,
                    "failure": 0,
                    "skipped": 0,
                },
            )
            print(
                " {batch_id}: \tworker={worker} status={status} success={success} "
                "failure={failure} skipped={skipped}"
                .format(
                    batch_id=batch_id,
                    worker=worker_id,
                    status=status,
                    success=counts["success"],
                    failure=counts["failure"],
                    skipped=counts["skipped"],
                )
            )

    conn.close()

    # divider
    print("=" * 75)

if __name__ == "__main__":
    main()

