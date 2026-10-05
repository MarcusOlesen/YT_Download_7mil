"""Rows must never end up 'pending' with no attempts left (unclaimable).

Needs a disposable Postgres; see conftest.py.
"""

from conftest import insert_pending
from distributed_core import (
    MAX_BLOCKS_PER_VIDEO,
    claim_videos,
    create_schema,
    reap_expired_leases,
    release_blocked_video,
    set_meta,
)

LEASE_SECONDS = 600
MAX_ATTEMPTS = 3


def _row(conn, video_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts, blocked_count, last_error, worker_id FROM videos WHERE id = %s",
            (video_id,),
        )
        row = cur.fetchone()
    conn.rollback()
    return row


def _expire(conn, video_id):
    with conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE videos SET lease_until = now() - interval '1 second' WHERE id = %s",
            (video_id,),
        )


def test_reaper_fails_rows_that_used_their_last_attempt(connect):
    conn = connect()
    set_meta(conn, "max_attempts", MAX_ATTEMPTS)
    insert_pending(conn, ["crashy"])

    for attempt in range(1, MAX_ATTEMPTS + 1):
        assert claim_videos(conn, "w", "b", 1, False, LEASE_SECONDS, MAX_ATTEMPTS) == ["crashy"]
        _expire(conn, "crashy")
        if attempt < MAX_ATTEMPTS:
            assert reap_expired_leases(conn) == (1, 0)
            assert _row(conn, "crashy")[:2] == ("pending", attempt)

    assert reap_expired_leases(conn) == (0, 1)
    status, attempts, _, last_error, worker_id = _row(conn, "crashy")
    assert (status, attempts, last_error, worker_id) == ("failure", MAX_ATTEMPTS, "lease_expired", "w")


def test_bot_block_refunds_the_attempt_and_counts_the_block(connect):
    conn = connect()
    insert_pending(conn, ["blocked"])

    # More blocks than max_attempts: the video must stay claimable.
    for block in range(1, MAX_ATTEMPTS + 2):
        assert claim_videos(conn, "w", "probe_w", 1, False, LEASE_SECONDS, MAX_ATTEMPTS) == ["blocked"]
        assert release_blocked_video(conn, "w", "blocked") == 1
        assert _row(conn, "blocked") == ("pending", 0, block, "bot_check", None)

    assert claim_videos(conn, "w", "b", 1, False, LEASE_SECONDS, MAX_ATTEMPTS) == ["blocked"]


def test_repeatedly_blocked_video_is_failed_not_stranded(connect):
    conn = connect()
    insert_pending(conn, ["always_blocked"])

    for _ in range(MAX_BLOCKS_PER_VIDEO):
        assert claim_videos(conn, "w", "probe_w", 1, False, LEASE_SECONDS, MAX_ATTEMPTS) == ["always_blocked"]
        release_blocked_video(conn, "w", "always_blocked", "rate_limit: HTTP Error 429")

    status, attempts, blocked_count, last_error, _ = _row(conn, "always_blocked")
    assert (status, attempts, blocked_count) == ("failure", 0, MAX_BLOCKS_PER_VIDEO)
    assert last_error.startswith("rate_limit")
    assert claim_videos(conn, "w", "b", 1, False, LEASE_SECONDS, MAX_ATTEMPTS) == []


def test_create_schema_adds_blocked_count_to_old_tables(connect):
    conn = connect()
    with conn, conn.cursor() as cur:
        cur.execute("ALTER TABLE videos DROP COLUMN blocked_count")
    insert_pending(conn, ["old_row"])

    create_schema(conn)
    create_schema(conn)  # second run is a no-op

    assert _row(conn, "old_row")[2] == 0
