import argparse
import os

import psycopg2


def connect_db(db_url):
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    return conn


def fetch_runs(conn, limit, worker_id):
    base = (
        "SELECT run_id, worker_id, script, status, started_at, finished_at, "
        "error_count, last_error "
        "FROM runs "
    )
    params = []
    if worker_id:
        base += "WHERE worker_id = %s "
        params.append(worker_id)
    base += "ORDER BY started_at DESC LIMIT %s"
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(base, params)
        rows = cur.fetchall()
    return rows


def fetch_run_logs(conn, run_id, limit):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts, level, message
            FROM run_logs
            WHERE run_id = %s
            ORDER BY ts DESC
            LIMIT %s
            """,
            (run_id, limit),
        )
        rows = cur.fetchall()
    return rows


def parse_args():
    parser = argparse.ArgumentParser(
        description="Show run history and error logs."
    )
    parser.add_argument(
        "--db-url",
        default="",
        help="Postgres connection string. Defaults to DATABASE_URL env var.",
    )
    parser.add_argument(
        "--tail",
        type=int,
        default=10,
        help="Show the last N runs.",
    )
    parser.add_argument(
        "--worker-id",
        default="",
        help="Optional worker ID filter.",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Show logs for a specific run ID.",
    )
    parser.add_argument(
        "--log-tail",
        type=int,
        default=50,
        help="Show the last N log entries for --run-id.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_url = args.db_url or os.getenv("DATABASE_URL", "")
    if not db_url:
        raise SystemExit("Missing --db-url or DATABASE_URL.")

    conn = connect_db(db_url)

    if args.run_id:
        logs = fetch_run_logs(conn, args.run_id, args.log_tail)
        if not logs:
            print(f"No logs found for run {args.run_id}.")
        else:
            print(f"Run logs for {args.run_id} (latest first):")
            for ts, level, message in logs:
                print(f"[{ts}] {level.upper()}: {message}")
        conn.close()
        return

    runs = fetch_runs(conn, args.tail, args.worker_id)
    if not runs:
        print("No runs found.")
        conn.close()
        return

    print("Recent runs:")
    for run_id, worker_id, script, status, started_at, finished_at, error_count, last_error in runs:
        print(
            f"{run_id} | worker={worker_id} | script={script} | status={status} | "
            f"start={started_at} | end={finished_at} | errors={error_count}"
        )
        if last_error:
            print(f"  last_error: {last_error}")

    conn.close()


if __name__ == "__main__":
    main()
