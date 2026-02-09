import argparse
import csv
import json
import os
import sys

import psycopg2


def connect_db(db_url):
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    return conn


def parse_id_list(text):
    if not text:
        return []
    cleaned = text.replace(",", " ")
    return [part.strip() for part in cleaned.split() if part.strip()]


def load_ids(args):
    ids = []
    if args.id:
        ids.extend(args.id)
    if args.ids:
        ids.extend(parse_id_list(args.ids))
    if args.ids_file:
        if args.ids_file == "-":
            content = sys.stdin.read()
        else:
            with open(args.ids_file, "r", encoding="utf-8") as f:
                content = f.read()
        ids.extend(parse_id_list(content))

    if not ids:
        raise SystemExit("No IDs provided. Use --id, --ids, or --ids-file.")

    seen = set()
    ordered = []
    for vid in ids:
        if vid in seen:
            continue
        seen.add(vid)
        ordered.append(vid)
    return ordered


def fetch_info(conn, ids):
    results = {}
    chunk_size = 500
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        placeholders = ",".join(["%s"] * len(chunk))
        query = (
            "SELECT v.id, v.status, v.batch_id, v.worker_id, v.attempts, v.last_error, "
            "v.start_time, v.end_time, v.elapsed_sec, v.output_file, v.log_path, "
            "v.dataset_name, v.dataset_priority, v.row_index, v.lease_until, "
            "b.status AS batch_status, b.created_at AS batch_created_at, "
            "b.finished_at AS batch_finished_at, b.zip_path, b.archive_path "
            "FROM videos v "
            "LEFT JOIN batches b ON v.batch_id = b.batch_id "
            f"WHERE v.id IN ({placeholders})"
        )
        with conn.cursor() as cur:
            cur.execute(query, chunk)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
        for row in rows:
            results[row[0]] = dict(zip(cols, row))
    return results


def output_results(results, ids, fmt, out_path):
    records = []
    for vid in ids:
        if vid in results:
            record = {"found": True, **results[vid]}
        else:
            record = {"found": False, "id": vid}
        records.append(record)

    if fmt == "json":
        payload = json.dumps(records, ensure_ascii=True, indent=2, default=str)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(payload)
        else:
            print(payload)
        return

    if fmt == "jsonl":
        lines = [json.dumps(rec, ensure_ascii=True, default=str) for rec in records]
        payload = "\n".join(lines)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(payload + "\n")
        else:
            print(payload)
        return

    if fmt == "csv":
        fieldnames = sorted({key for rec in records for key in rec.keys()})
        if out_path:
            out_f = open(out_path, "w", newline="", encoding="utf-8")
            close_after = True
        else:
            out_f = sys.stdout
            close_after = False
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
        if close_after:
            out_f.close()
        return

    raise SystemExit(f"Unknown format: {fmt}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Lookup video status and paths from the shared Postgres DB."
    )
    parser.add_argument(
        "--db-url",
        default="",
        help="Postgres connection string. Defaults to DATABASE_URL env var.",
    )
    parser.add_argument(
        "--id",
        action="append",
        help="Single video ID (repeatable).",
    )
    parser.add_argument(
        "--ids",
        default="",
        help="Comma/space-separated list of IDs.",
    )
    parser.add_argument(
        "--ids-file",
        default="",
        help="Path to file with IDs (comma/space-separated or one per line). Use '-' for stdin.",
    )
    parser.add_argument(
        "--format",
        default="jsonl",
        choices=["jsonl", "json", "csv"],
        help="Output format.",
    )
    parser.add_argument(
        "--out",
        default="",
        help="Optional output file path (otherwise prints to stdout).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_url = args.db_url or os.getenv("DATABASE_URL", "")
    if not db_url:
        raise SystemExit("Missing --db-url or DATABASE_URL.")

    ids = load_ids(args)
    conn = connect_db(db_url)
    results = fetch_info(conn, ids)
    conn.close()
    output_results(results, ids, args.format, args.out)


if __name__ == "__main__":
    main()
