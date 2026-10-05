"""End-to-end runs of start_download.main and rerun_failed_batch.main.

YouTube is replaced by a fake downloader; everything else (claiming, batches,
lease heartbeat, result writes, bot-check pause and probe, run logging) runs
for real against Postgres. Needs a disposable Postgres; see conftest.py.
"""

import os
import sys
import threading

import pytest

import distributed_core
import env_utils
import worker_common

# The worker scripts call load_env() on import. Keep them away from .env.
env_utils.load_env = lambda: False

import rerun_failed_batch  # noqa: E402
import start_download  # noqa: E402
from conftest import insert_pending  # noqa: E402

BOT_LINE = "ERROR: [youtube] {id}: Sign in to confirm you're not a bot. Use --cookies"


class FakeYouTube:
    """Stands in for scraper_utils.download_video."""

    def __init__(self, failing=(), blocked_calls=0):
        self.failing = set(failing)
        self.blocked_calls = blocked_calls  # the first N calls hit a bot check
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self, video_id, download_dir, test=False):
        with self.lock:
            self.calls.append(video_id)
            blocked = len(self.calls) <= self.blocked_calls
        if blocked:
            return "failure", "generic_error: bot", BOT_LINE.format(id=video_id)
        if video_id in self.failing:
            return "failure", "generic_error: boom", f"ERROR: [youtube] {video_id}: boom"
        os.makedirs(download_dir, exist_ok=True)
        with open(os.path.join(download_dir, f"{video_id}.mp4"), "w") as f:
            f.write("x")
        return "success", None, ""


@pytest.fixture
def worker_env(monkeypatch, schema_dsn):
    monkeypatch.setattr(
        worker_common,
        "initialize_worker_pipeline",
        lambda run_dir, **kwargs: {"preset": "test", "target_titles_per_hour": 0},
    )
    saved = sys.stdout, sys.stderr  # start_download tees the terminal to a file
    yield schema_dsn
    sys.stdout, sys.stderr = saved


def _ids(n, prefix="v"):
    # Real IDs are 11 characters; the bot-check pattern relies on that.
    return [f"{prefix}{i:010d}" for i in range(n)]


def _videos(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, status, attempts, blocked_count, output_file, batch_id FROM videos ORDER BY id"
        )
        rows = {r[0]: r[1:] for r in cur.fetchall()}
    conn.rollback()
    return rows


def _run_status(conn, script):
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM runs WHERE script = %s", (script,))
        rows = [r[0] for r in cur.fetchall()]
    conn.rollback()
    return rows


def _run(main, monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["prog"] + argv)
    main()


def test_download_run_processes_every_video(worker_env, connect, monkeypatch, tmp_path):
    conn = connect()
    ids = _ids(23)
    insert_pending(conn, ids)
    failing = {ids[4], ids[17]}
    fake = FakeYouTube(failing=failing)
    monkeypatch.setattr(distributed_core, "download_video", fake)

    run_dir = tmp_path / "run_a"
    # A file from an earlier batch on this machine: must be skipped, not downloaded.
    old = run_dir / "batches" / "old_batch" / "videos"
    old.mkdir(parents=True)
    (old / f"{ids[9]}.mp4").write_text("x")

    _run(start_download.main, monkeypatch, [
        "--db-url", worker_env, "--run-dir", str(run_dir), "--worker-id", "w1",
        "--batch-size", "5", "--workers", "3",
    ])

    rows = _videos(conn)
    assert sorted(fake.calls) == sorted(set(ids) - {ids[9]})
    for video_id, (status, attempts, blocked, output_file, batch_id) in rows.items():
        assert attempts == 1
        assert blocked == 0
        if video_id in failing:
            assert status == "failure"
        elif video_id == ids[9]:
            assert status == "skipped"
            assert output_file == str(old / f"{ids[9]}.mp4")
        else:
            assert status == "success"
            assert os.path.exists(output_file)
            assert batch_id in output_file
    assert _run_status(conn, "start_download") == ["completed"]

    with conn.cursor() as cur:
        cur.execute("SELECT count(*), sum(success), sum(failure), sum(skipped) FROM batches")
        assert cur.fetchone() == (5, 20, 2, 1)
    conn.rollback()

    # Retry the failures of one batch once the fake stops failing.
    failed_batch = rows[ids[4]][4]
    fake.failing.clear()
    _run(rerun_failed_batch.main, monkeypatch, [
        "--db-url", worker_env, "--run-dir", str(run_dir), "--worker-id", "w1",
        "--batch-id", failed_batch, "--workers", "2",
    ])
    rows = _videos(conn)
    assert rows[ids[4]][:2] == ("success", 2)
    assert rows[ids[17]][0] == "failure"  # different batch, untouched
    assert _run_status(conn, "rerun_failed_batch") == ["completed"]


def test_bot_check_pauses_probes_and_resumes(worker_env, connect, monkeypatch, tmp_path):
    conn = connect()
    ids = _ids(12)
    insert_pending(conn, ids)
    # The first 4 downloads hit the bot check; then the block lifts.
    fake = FakeYouTube(blocked_calls=4)
    monkeypatch.setattr(distributed_core, "download_video", fake)
    run_dir = tmp_path / "run_b"

    _run(start_download.main, monkeypatch, [
        "--db-url", worker_env, "--run-dir", str(run_dir), "--worker-id", "w2",
        "--batch-size", "6", "--workers", "1", "--overlap-batches", "1",
        "--block-threshold", "3", "--block-sleep-seconds", "1",
    ])

    rows = _videos(conn)
    assert all(status == "success" for status, *_ in rows.values()), rows
    # Blocks do not use up attempts; they are counted separately.
    assert all(attempts == 1 for _, attempts, *_ in rows.values())
    assert sum(blocked for _, _, blocked, *_ in rows.values()) == 4
    assert (run_dir / "block_wait_state.json").exists()

    with conn.cursor() as cur:
        cur.execute("SELECT status, last_error FROM batches ORDER BY created_at LIMIT 1")
        assert cur.fetchone() == ("paused", "bot_check_threshold")
        cur.execute("SELECT message FROM run_logs ORDER BY id")
        messages = [r[0] for r in cur.fetchall()]
    conn.rollback()
    assert any("Bot-check threshold reached (3)" in m for m in messages)
    assert any(m.startswith("Probe success") for m in messages)
    assert _run_status(conn, "start_download") == ["completed"]


def test_db_session_reconnects_after_a_broken_connection(schema_dsn, connect):
    db = worker_common.DbSession(schema_dsn)
    pid = db.run(lambda conn: conn.get_backend_pid())
    # Kill the session's backend, as a dropped SSH tunnel would.
    admin = connect()
    with admin, admin.cursor() as cur:
        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
    with pytest.raises(Exception):
        db.run(distributed_core.get_meta, "batch_size")
    # The error is not retried, but the next call gets a fresh connection.
    assert db.run(lambda conn: conn.get_backend_pid()) != pid
    # A read-only helper must not leave the connection idle in a transaction.
    db.run(distributed_core.get_meta, "batch_size")
    assert db.run(lambda conn: conn.status) == 1  # STATUS_READY
    db.close()
