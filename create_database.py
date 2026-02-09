import argparse
import os

from distributed_core import (
    DEFAULT_DATASETS,
    connect_db,
    init_db,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Initialize the Postgres database and load IDs."
    )
    parser.add_argument(
        "--db-url",
        default="",
        help="Postgres connection string. Defaults to DATABASE_URL env var.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size to store in DB metadata.",
    )
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=1800,
        help="Lease duration to store in DB metadata.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Max attempts to store in DB metadata.",
    )
    parser.add_argument(
        "--ids-ok",
        default=DEFAULT_DATASETS[0]["path"],
        help="Parquet file with prioritized ids.",
    )
    parser.add_argument(
        "--ids-no-upload",
        default=DEFAULT_DATASETS[1]["path"],
        help="Parquet file with ids missing upload info.",
    )
    parser.add_argument(
        "--ids-errors",
        default=DEFAULT_DATASETS[2]["path"],
        help="Parquet file with ids that had errors.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_url = args.db_url or os.getenv("DATABASE_URL", "")
    if not db_url:
        raise SystemExit("Missing --db-url or DATABASE_URL.")

    datasets = [
        {"name": "ids_ok_sorted", "path": args.ids_ok},
        {"name": "ids_no_uploadinfo", "path": args.ids_no_upload},
        {"name": "ids_with_errors", "path": args.ids_errors},
    ]

    conn = connect_db(db_url)
    init_db(conn, datasets, args.batch_size, args.lease_seconds, args.max_attempts)
    conn.close()
    print("Database initialized.")


if __name__ == "__main__":
    main()
