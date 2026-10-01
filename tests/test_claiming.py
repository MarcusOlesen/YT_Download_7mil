"""Concurrency tests for claim_videos and reap_expired_leases.

These tests need a disposable Postgres and only run when TEST_DATABASE_URL is
set. They never read .env or DATABASE_URL. All tables are created in a fresh,
uniquely named schema that is dropped afterwards, so nothing outside that
schema is touched.

    TEST_DATABASE_URL=postgresql://postgres@localhost:5432/yt_test python -m pytest tests
"""

import os
import sys
import threading
import uuid
from pathlib import Path

import pytest

TEST_DB_URL = os.environ.get("TEST_DATABASE_URL", "")
if not TEST_DB_URL:
    pytest.skip("TEST_DATABASE_URL is not set", allow_module_level=True)

psycopg2 = pytest.importorskip("psycopg2")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from distributed_core import claim_videos, create_schema, reap_expired_leases  # noqa: E402

LEASE_SECONDS = 600
MAX_ATTEMPTS = 3


@pytest.fixture
def schema():
    name = f"test_claiming_{uuid.uuid4().hex[:12]}"
    admin = psycopg2.connect(TEST_DB_URL)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{name}"')
    try:
        conn = _connect(name)
        create_schema(conn)
        conn.close()
        yield name
    finally:
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA "{name}" CASCADE')
        admin.close()


def _connect(schema_name):
    return psycopg2.connect(TEST_DB_URL, options=f"-c search_path={schema_name}")


def _insert_pending(schema_name, count):
    ids = [f"vid{i:08d}" for i in range(count)]
    conn = _connect(schema_name)
    with conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO videos (id, dataset_name, dataset_priority, row_index, status) "
            "VALUES (%s, 'test', 0, %s, 'pending')",
            [(video_id, idx) for idx, video_id in enumerate(ids)],
        )
    conn.close()
    return ids


def test_concurrent_claimers_never_claim_same_id(schema):
    all_ids = _insert_pending(schema, 3000)
    n_claimers = 12
    claim_size = 7
    barrier = threading.Barrier(n_claimers)
    claimed = [[] for _ in range(n_claimers)]
    errors = []

    def claimer(idx):
        try:
            conn = _connect(schema)
            barrier.wait()
            while True:
                ids = claim_videos(
                    conn,
                    f"worker_{idx}",
                    f"batch_{idx}",
                    claim_size,
                    False,
                    LEASE_SECONDS,
                    MAX_ATTEMPTS,
                )
                if not ids:
                    break
                claimed[idx].extend(ids)
            conn.close()
        except Exception as exc:  # surfaced in the main thread below
            errors.append(exc)

    threads = [threading.Thread(target=claimer, args=(i,)) for i in range(n_claimers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    flat = [video_id for ids in claimed for video_id in ids]
    assert len(flat) == len(set(flat)), "a video ID was claimed more than once"
    assert set(flat) == set(all_ids)
    # More than one claimer must have won rows, otherwise the test proved nothing.
    assert sum(1 for ids in claimed if ids) > 1

    conn = _connect(schema)
    with conn.cursor() as cur:
        cur.execute("SELECT id, worker_id, attempts, status FROM videos")
        rows = cur.fetchall()
    conn.close()
    owner = {video_id: f"worker_{i}" for i, ids in enumerate(claimed) for video_id in ids}
    for video_id, worker_id, attempts, status in rows:
        assert status == "in_progress"
        assert attempts == 1
        assert worker_id == owner[video_id]


def test_reaper_reclaims_expired_leases(schema):
    _insert_pending(schema, 20)
    conn = _connect(schema)

    first = claim_videos(conn, "worker_a", "batch_a", 20, False, LEASE_SECONDS, MAX_ATTEMPTS)
    assert len(first) == 20
    expired, live = first[:10], first[10:]

    # Simulate worker_a dying: half of its leases run out.
    with conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE videos SET lease_until = now() - interval '1 second' WHERE id = ANY(%s)",
            (expired,),
        )

    # claim_videos only picks 'pending' rows, so expired in_progress rows
    # stay invisible to other workers until the reaper runs.
    assert claim_videos(conn, "worker_b", "batch_b", 20, False, LEASE_SECONDS, MAX_ATTEMPTS) == []

    assert reap_expired_leases(conn) == len(expired)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, worker_id, lease_until, batch_id FROM videos WHERE id = ANY(%s)",
            (expired,),
        )
        for status, worker_id, lease_until, batch_id in cur.fetchall():
            assert (status, worker_id, lease_until, batch_id) == ("pending", None, None, None)

    second = claim_videos(conn, "worker_b", "batch_b", 20, False, LEASE_SECONDS, MAX_ATTEMPTS)
    assert sorted(second) == sorted(expired)

    with conn.cursor() as cur:
        cur.execute("SELECT id, worker_id, attempts FROM videos")
        rows = {video_id: (worker_id, attempts) for video_id, worker_id, attempts in cur.fetchall()}
    conn.close()

    for video_id in expired:
        assert rows[video_id] == ("worker_b", 2)
    for video_id in live:
        assert rows[video_id] == ("worker_a", 1)
