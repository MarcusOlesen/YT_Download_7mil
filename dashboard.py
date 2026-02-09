import html
import os
import subprocess
import sys
import threading
import time

import psycopg2
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:YouTube@localhost:5432/yt_downloads",
)
DEFAULT_ARCHIVE_DIR = r"O:\ARTS_SoMe-Influence\YT_Download_all_videos\archive"
DEFAULT_BACKUP_DIR = r"O:\ARTS_SoMe-Influence\YT_Download_all_videos\DB_backup"
DEFAULT_RUN_DIR = "download_run"


STATE_LOCK = threading.Lock()
STATE = {
    "host": None,
    "workers": {},
}

app = FastAPI()


def connect_db(db_url):
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    return conn


def to_iso(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


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


def fetch_dataset_progress(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.name, d.priority, d.total_count,
                   SUM(CASE WHEN v.status IN ('success','failure','skipped') THEN 1 ELSE 0 END) AS done
            FROM datasets d
            LEFT JOIN videos v ON v.dataset_name = d.name
            GROUP BY d.name, d.priority, d.total_count
            ORDER BY d.priority ASC
            """
        )
        rows = cur.fetchall()
    return rows


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
            SELECT batch_id, worker_id, status, created_at, finished_at,
                   success, failure, skipped, zip_path
            FROM batches
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return rows


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


def process_info(entry):
    if not entry:
        return None
    proc = entry.get("process")
    if not proc:
        return None
    return {
        "pid": proc.pid,
        "alive": proc.poll() is None,
        "returncode": proc.poll(),
        "started_at": entry.get("started_at"),
        "worker_id": entry.get("worker_id"),
        "cmd": entry.get("cmd"),
    }


def render_dashboard():
    def esc(value):
        return html.escape(str(value))

    html_template = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>YT Distributed Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    h1 { margin-bottom: 5px; }
    .section { border: 1px solid #ccc; padding: 12px; margin: 12px 0; }
    .grid { display: grid; grid-template-columns: 1fr; gap: 12px; }
    label { display: block; margin-top: 8px; font-weight: bold; }
    input[type="text"], input[type="number"] { width: 100%; padding: 6px; display: block; }
    button { margin-top: 8px; padding: 6px 12px; }
    pre { background: #f6f6f6; padding: 10px; overflow: auto; }
    .status { font-size: 0.9em; color: #555; }
  </style>
</head>
<body>
  <h1>YT Distributed Dashboard</h1>
  <div class="status">Auto-refreshes every 10 seconds</div>

  <div class="section">
    <h2>Database / Host</h2>
    <form id="host-form">
      <label>Database URL</label>
      <input type="text" name="db_url" value="__DB_URL__" />
      <label>Backup Directory</label>
      <input type="text" name="backup_dir" value="__BACKUP_DIR__" />
      <label>Backup Interval (minutes)</label>
      <input type="number" name="interval_minutes" value="60" />
      <label>Keep Backups</label>
      <input type="number" name="keep" value="3" />
      <label>pg_dump Path (optional)</label>
      <input type="text" name="pg_dump_path" value="" />
      <div class="option-line">
        <input type="checkbox" name="reap" checked />
        <span>Reap expired leases</span>
      </div>
      <button type="button" onclick="startHost()">Start Host</button>
      <button type="button" onclick="stopHost()">Stop Host</button>
    </form>
    <pre id="host-status"></pre>
  </div>

  <div class="section">
    <h2>Worker</h2>
    <form id="worker-form">
      <label>Database URL</label>
      <input type="text" name="db_url" value="__DB_URL__" />
      <label>Archive Directory</label>
      <input type="text" name="archive_dir" value="__ARCHIVE_DIR__" />
      <label>Run Directory</label>
      <input type="text" name="run_dir" value="__RUN_DIR__" />
      <label>Worker ID (optional)</label>
      <input type="text" name="worker_id" value="" />
      <div class="grid">
        <div>
          <label>Batch Size</label>
          <input type="number" name="batch_size" value="1000" />
        </div>
        <div>
          <label>Workers</label>
          <input type="number" name="workers" value="4" />
        </div>
      </div>
      <div class="grid">
        <div>
          <label>Lease Seconds</label>
          <input type="number" name="lease_seconds" value="1800" />
        </div>
        <div>
          <label>Max Attempts</label>
          <input type="number" name="max_attempts" value="3" />
        </div>
      </div>
      <label>Max Batches (0 = unlimited)</label>
      <input type="number" name="max_batches" value="0" />
      <div class="option-line">
        <input type="checkbox" name="retry_failures" />
        <span>Retry failures</span>
      </div>
      <div class="option-line">
        <input type="checkbox" name="keep_batch_dir" />
        <span>Keep batch directories</span>
      </div>
      <div class="option-line">
        <input type="checkbox" name="keep_local_zip" />
        <span>Keep local zips</span>
      </div>
      <div class="option-line">
        <input type="checkbox" name="test_mode" />
        <span>Test mode</span>
      </div>
      <button type="button" onclick="startWorker()">Start Worker</button>
    </form>
    <pre id="worker-status"></pre>
  </div>

  <div class="section">
    <h2>Processes</h2>
    <pre id="process-status"></pre>
    <label>Stop Worker (key)</label>
    <input type="text" id="stop-worker-key" value="" />
    <button type="button" onclick="stopWorker()">Stop Worker</button>
  </div>

  <div class="section">
    <h2>Status Overview</h2>
    <pre id="status-overview"></pre>
  </div>

  <div class="section">
    <h2>Run Report</h2>
    <div class="grid">
      <div>
        <label>Worker ID Filter (optional)</label>
        <input type="text" id="run-worker-id" value="" />
      </div>
      <div>
        <label>Run Count</label>
        <input type="number" id="run-tail" value="10" />
      </div>
    </div>
    <button type="button" onclick="refreshRuns()">Refresh Runs</button>
    <pre id="run-report"></pre>
    <label>Run ID for Logs</label>
    <input type="text" id="run-id" value="" />
    <label>Log Tail</label>
    <input type="number" id="log-tail" value="50" />
    <button type="button" onclick="fetchRunLogs()">Fetch Logs</button>
    <pre id="run-logs"></pre>
  </div>

  <script>
    async function fetchJson(url, options) {
      const res = await fetch(url, options);
      let data = null;
      try {
        data = await res.json();
      } catch (err) {
        data = { error: "Non-JSON response" };
      }
      if (!res.ok && data && !data.error) {
        data.error = res.statusText || "Request failed";
      }
      return data;
    }

    async function postForm(url, formId) {
      const form = document.getElementById(formId);
      const data = new FormData(form);
      return fetchJson(url, { method: "POST", body: data });
    }

    async function startHost() {
      const result = await postForm("/api/start_host", "host-form");
      document.getElementById("host-status").textContent = JSON.stringify(result, null, 2);
      refreshProcesses();
    }

    async function stopHost() {
      const result = await fetchJson("/api/stop_host", { method: "POST" });
      document.getElementById("host-status").textContent = JSON.stringify(result, null, 2);
      refreshProcesses();
    }

    async function startWorker() {
      const result = await postForm("/api/start_worker", "worker-form");
      document.getElementById("worker-status").textContent = JSON.stringify(result, null, 2);
      refreshProcesses();
    }

    async function stopWorker() {
      const key = document.getElementById("stop-worker-key").value;
      const data = new FormData();
      data.append("worker_key", key);
      const result = await fetchJson("/api/stop_worker", { method: "POST", body: data });
      document.getElementById("worker-status").textContent = JSON.stringify(result, null, 2);
      refreshProcesses();
    }

    async function refreshProcesses() {
      const data = await fetchJson("/api/processes");
      document.getElementById("process-status").textContent = JSON.stringify(data, null, 2);
    }

    async function refreshStatus() {
      const dbUrl = document.querySelector("#host-form input[name='db_url']").value;
      const data = await fetchJson(`/api/status?db_url=${encodeURIComponent(dbUrl)}&tail=5`);
      document.getElementById("status-overview").textContent = JSON.stringify(data, null, 2);
    }

    async function refreshRuns() {
      const dbUrl = document.querySelector("#host-form input[name='db_url']").value;
      const workerId = document.getElementById("run-worker-id").value;
      const tail = document.getElementById("run-tail").value;
      const data = await fetchJson(`/api/runs?db_url=${encodeURIComponent(dbUrl)}&tail=${tail}&worker_id=${encodeURIComponent(workerId)}`);
      document.getElementById("run-report").textContent = JSON.stringify(data, null, 2);
    }

    async function fetchRunLogs() {
      const dbUrl = document.querySelector("#host-form input[name='db_url']").value;
      const runId = document.getElementById("run-id").value;
      const tail = document.getElementById("log-tail").value;
      const data = await fetchJson(`/api/run_logs?db_url=${encodeURIComponent(dbUrl)}&run_id=${encodeURIComponent(runId)}&tail=${tail}`);
      document.getElementById("run-logs").textContent = JSON.stringify(data, null, 2);
    }

    refreshProcesses();
    refreshStatus();
    refreshRuns();
    setInterval(refreshStatus, 10000);
    setInterval(refreshProcesses, 10000);
  </script>
</body>
</html>
    """
    return (
        html_template
        .replace("__DB_URL__", esc(DEFAULT_DB_URL))
        .replace("__ARCHIVE_DIR__", esc(DEFAULT_ARCHIVE_DIR))
        .replace("__BACKUP_DIR__", esc(DEFAULT_BACKUP_DIR))
        .replace("__RUN_DIR__", esc(DEFAULT_RUN_DIR))
    )


@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(render_dashboard())


@app.get("/api/processes")
def get_processes():
    with STATE_LOCK:
        host_info = process_info(STATE.get("host"))
        workers = {
            key: process_info(info)
            for key, info in STATE.get("workers", {}).items()
        }
    return JSONResponse({"host": host_info, "workers": workers})


@app.post("/api/start_host")
async def start_host(request: Request):
    form = await request.form()
    db_url = str(form.get("db_url", "")).strip()
    backup_dir = str(form.get("backup_dir", "")).strip()
    interval_minutes = str(form.get("interval_minutes", "60")).strip()
    keep = str(form.get("keep", "3")).strip()
    pg_dump_path = str(form.get("pg_dump_path", "")).strip()
    reap = form.get("reap") is not None

    if not db_url:
        return JSONResponse({"error": "Missing db_url"}, status_code=400)

    cmd = [
        sys.executable,
        "host_database.py",
        "--backup-dir",
        backup_dir,
        "--interval-minutes",
        interval_minutes,
        "--keep",
        keep,
    ]
    if pg_dump_path:
        cmd.extend(["--pg-dump-path", pg_dump_path])
    if reap:
        cmd.append("--reap")

    env = os.environ.copy()
    env["DATABASE_URL"] = db_url

    with STATE_LOCK:
        existing = STATE.get("host")
        if existing and existing.get("process") and existing["process"].poll() is None:
            return JSONResponse({"error": "Host already running."}, status_code=409)
        proc = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        STATE["host"] = {
            "process": proc,
            "cmd": cmd,
            "started_at": time.time(),
        }

    return JSONResponse({"status": "started", "pid": proc.pid, "cmd": cmd})


@app.post("/api/stop_host")
def stop_host():
    with STATE_LOCK:
        entry = STATE.get("host")
        if not entry or not entry.get("process"):
            return JSONResponse({"status": "not running"})
        proc = entry["process"]
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
        STATE["host"] = None
    return JSONResponse({"status": "stopped"})


@app.post("/api/start_worker")
async def start_worker(request: Request):
    form = await request.form()
    db_url = str(form.get("db_url", "")).strip()
    archive_dir = str(form.get("archive_dir", "")).strip()
    run_dir = str(form.get("run_dir", "")).strip()
    worker_id = str(form.get("worker_id", "")).strip()
    batch_size = str(form.get("batch_size", "1000")).strip()
    workers = str(form.get("workers", "4")).strip()
    lease_seconds = str(form.get("lease_seconds", "1800")).strip()
    max_attempts = str(form.get("max_attempts", "3")).strip()
    max_batches = str(form.get("max_batches", "0")).strip()
    retry_failures = form.get("retry_failures") is not None
    keep_batch_dir = form.get("keep_batch_dir") is not None
    keep_local_zip = form.get("keep_local_zip") is not None
    test_mode = form.get("test_mode") is not None

    if not db_url:
        return JSONResponse({"error": "Missing db_url"}, status_code=400)
    if not archive_dir:
        return JSONResponse({"error": "Missing archive_dir"}, status_code=400)

    cmd = [
        sys.executable,
        "start_download.py",
        "--archive-dir",
        archive_dir,
        "--run-dir",
        run_dir,
        "--workers",
        workers,
        "--batch-size",
        batch_size,
        "--lease-seconds",
        lease_seconds,
        "--max-attempts",
        max_attempts,
        "--max-batches",
        max_batches,
    ]
    if worker_id:
        cmd.extend(["--worker-id", worker_id])
    if retry_failures:
        cmd.append("--retry-failures")
    if keep_batch_dir:
        cmd.append("--keep-batch-dir")
    if keep_local_zip:
        cmd.append("--keep-local-zip")
    if test_mode:
        cmd.append("--test-mode")

    env = os.environ.copy()
    env["DATABASE_URL"] = db_url

    with STATE_LOCK:
        key = worker_id or f"worker_{int(time.time())}"
        if key in STATE["workers"]:
            existing = STATE["workers"][key]["process"]
            if existing.poll() is None:
                return JSONResponse(
                    {"error": f"Worker {key} already running."},
                    status_code=409,
                )
        proc = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        STATE["workers"][key] = {
            "process": proc,
            "cmd": cmd,
            "started_at": time.time(),
            "worker_id": worker_id or key,
        }

    return JSONResponse({"status": "started", "pid": proc.pid, "key": key})


@app.post("/api/stop_worker")
async def stop_worker(request: Request):
    form = await request.form()
    worker_key = str(form.get("worker_key", "")).strip()
    if not worker_key:
        return JSONResponse({"error": "Missing worker_key"}, status_code=400)

    with STATE_LOCK:
        entry = STATE["workers"].get(worker_key)
        if not entry or not entry.get("process"):
            return JSONResponse({"status": "not running"})
        proc = entry["process"]
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
        STATE["workers"].pop(worker_key, None)
    return JSONResponse({"status": "stopped", "worker_key": worker_key})


@app.get("/api/status")
def get_status(db_url: str, tail: int = 5):
    if not db_url:
        return JSONResponse({"error": "Missing db_url"}, status_code=400)
    try:
        conn = connect_db(db_url)
        meta = fetch_meta(conn)
        counts = fetch_status_counts(conn)
        total = sum(counts.values())
        done = counts["success"] + counts["failure"] + counts["skipped"]
        remaining = max(0, total - done)
        datasets = []
        for name, priority, total_count, done_count in fetch_dataset_progress(conn):
            datasets.append(
                {
                    "name": name,
                    "priority": priority,
                    "total": int(total_count),
                    "done": int(done_count or 0),
                }
            )
        workers = [
            {"worker_id": worker_id, "in_progress": count}
            for worker_id, count in fetch_active_workers(conn)
        ]
        batches = []
        for row in fetch_recent_batches(conn, tail):
            (
                batch_id,
                worker_id,
                status,
                created_at,
                finished_at,
                success,
                failure,
                skipped,
                zip_path,
            ) = row
            batches.append(
                {
                    "batch_id": batch_id,
                    "worker_id": worker_id,
                    "status": status,
                    "created_at": to_iso(created_at),
                    "finished_at": to_iso(finished_at),
                    "success": success,
                    "failure": failure,
                    "skipped": skipped,
                    "zip_path": zip_path,
                }
            )
        conn.close()
        return JSONResponse(
            {
                "counts": counts,
                "total": total,
                "done": done,
                "remaining": remaining,
                "meta": meta,
                "datasets": datasets,
                "active_workers": workers,
                "recent_batches": batches,
            }
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/runs")
def get_runs(db_url: str, tail: int = 10, worker_id: str = ""):
    if not db_url:
        return JSONResponse({"error": "Missing db_url"}, status_code=400)
    try:
        conn = connect_db(db_url)
        rows = fetch_runs(conn, tail, worker_id.strip())
        conn.close()
        runs = []
        for row in rows:
            (
                run_id,
                run_worker_id,
                script,
                status,
                started_at,
                finished_at,
                error_count,
                last_error,
            ) = row
            runs.append(
                {
                    "run_id": run_id,
                    "worker_id": run_worker_id,
                    "script": script,
                    "status": status,
                    "started_at": to_iso(started_at),
                    "finished_at": to_iso(finished_at),
                    "error_count": error_count,
                    "last_error": last_error,
                }
            )
        return JSONResponse({"runs": runs})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/run_logs")
def get_run_logs(db_url: str, run_id: str, tail: int = 50):
    if not db_url:
        return JSONResponse({"error": "Missing db_url"}, status_code=400)
    if not run_id:
        return JSONResponse({"error": "Missing run_id"}, status_code=400)
    try:
        conn = connect_db(db_url)
        rows = fetch_run_logs(conn, run_id, tail)
        conn.close()
        logs = [
            {"ts": to_iso(ts), "level": level, "message": message}
            for ts, level, message in rows
        ]
        return JSONResponse({"run_id": run_id, "logs": logs})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
