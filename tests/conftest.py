"""Shared fixtures.

Database tests need a disposable Postgres and are skipped unless
TEST_DATABASE_URL is set. Nothing here reads .env or DATABASE_URL. Each test
gets a new, uniquely named schema that is dropped afterwards, so nothing
outside that schema is touched.
"""

import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

TEST_DB_URL = os.environ.get("TEST_DATABASE_URL", "")


@pytest.fixture
def schema_dsn():
    """DSN whose search_path is a fresh schema with the downloader tables."""
    if not TEST_DB_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2.extensions import make_dsn

    from distributed_core import create_schema

    name = f"test_{uuid.uuid4().hex[:12]}"
    admin = psycopg2.connect(TEST_DB_URL)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{name}"')
    dsn = make_dsn(TEST_DB_URL, options=f"-c search_path={name}")
    try:
        conn = psycopg2.connect(dsn)
        create_schema(conn)
        conn.close()
        yield dsn
    finally:
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA "{name}" CASCADE')
        admin.close()


@pytest.fixture
def connect(schema_dsn):
    import psycopg2

    conns = []

    def _connect():
        conn = psycopg2.connect(schema_dsn)
        conns.append(conn)
        return conn

    yield _connect
    for conn in conns:
        conn.close()


def insert_pending(conn, ids, priority=0):
    with conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO videos (id, dataset_name, dataset_priority, row_index, status) "
            "VALUES (%s, 'test', %s, %s, 'pending')",
            [(video_id, priority, idx) for idx, video_id in enumerate(ids)],
        )
