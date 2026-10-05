"""Concurrency tests for claim_videos and reap_expired_leases.

Needs a disposable Postgres; see conftest.py.

    TEST_DATABASE_URL=postgresql://postgres@localhost:5432/yt_test python -m pytest tests
"""

import threading

from conftest import insert_pending
from distributed_core import claim_videos, reap_expired_leases

LEASE_SECONDS = 600
MAX_ATTEMPTS = 3


def test_concurrent_claimers_never_claim_same_id(connect):
    all_ids = [f"vid{i:08d}" for i in range(3000)]
    insert_pending(connect(), all_ids)
    n_claimers = 12
    claim_size = 7
    barrier = threading.Barrier(n_claimers)
    claimed = [[] for _ in range(n_claimers)]
    errors = []
    conns = [connect() for _ in range(n_claimers)]

    def claimer(idx):
        try:
            barrier.wait()
            while True:
                ids = claim_videos(
                    conns[idx],
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

    with connect().cursor() as cur:
        cur.execute("SELECT id, worker_id, attempts, status FROM videos")
        rows = cur.fetchall()
    owner = {video_id: f"worker_{i}" for i, ids in enumerate(claimed) for video_id in ids}
    for video_id, worker_id, attempts, status in rows:
        assert status == "in_progress"
        assert attempts == 1
        assert worker_id == owner[video_id]


def test_reaper_reclaims_expired_leases(connect):
    conn = connect()
    insert_pending(conn, [f"vid{i:08d}" for i in range(20)])

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

    assert reap_expired_leases(conn) == (len(expired), 0)

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

    for video_id in expired:
        assert rows[video_id] == ("worker_b", 2)
    for video_id in live:
        assert rows[video_id] == ("worker_a", 1)
