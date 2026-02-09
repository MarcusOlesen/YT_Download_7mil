import argparse
import os

import psycopg2


def parse_args():
    parser = argparse.ArgumentParser(
        description="Reset the downloader database (DESTRUCTIVE)."
    )
    parser.add_argument(
        "--db-url",
        default="",
        help="Postgres connection string. Defaults to DATABASE_URL env var.",
    )
    return parser.parse_args()


def get_db_identity(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT current_database(), inet_server_addr(), inet_server_port()"
        )
        row = cur.fetchone()
    return row


def confirm_reset(db_name):
    print("\n!!! DANGER: This will DELETE ALL downloader state !!!")
    print("Database:", db_name)
    print("This cannot be undone. Ensure backups exist.\n")

    entered = input("Type the database name to continue: ").strip()
    if entered != db_name:
        print("Database name mismatch. Aborting.")
        return False

    entered = input("Type RESET to continue: ").strip()
    if entered != "RESET":
        print("Confirmation failed. Aborting.")
        return False

    entered = input("Type DELETE ALL to permanently erase data: ").strip()
    if entered != "DELETE ALL":
        print("Final confirmation failed. Aborting.")
        return False

    return True


def reset_tables(conn):
    tables = [
        "run_logs",
        "runs",
        "batches",
        "videos",
        "datasets",
        "meta",
    ]
    with conn:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")


def main():
    args = parse_args()
    db_url = args.db_url or os.getenv("DATABASE_URL", "")
    if not db_url:
        raise SystemExit("Missing --db-url or DATABASE_URL.")

    conn = psycopg2.connect(db_url)
    db_name, addr, port = get_db_identity(conn)
    conn.close()

    print("DB host:", addr, "port:", port)

    if not confirm_reset(db_name):
        return

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    reset_tables(conn)
    conn.close()

    print("\nDatabase reset complete.")
    print("Next: run create_database.py to load IDs.")


if __name__ == "__main__":
    main()
