import html
import os
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone

import psycopg2
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from env_utils import load_env

load_env()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:YouTube@localhost:5432/yt_downloads",
)
DEFAULT_ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
DEFAULT_BACKUP_DIR = os.path.join(BASE_DIR, "DB_backup")
DEFAULT_RUN_DIR = os.path.join(BASE_DIR, "run_local")
LOG_BUFFER_SIZE = 5000

STATE_LOCK = threading.Lock()
STATE = {
    "processes": {},
}

app = FastAPI()


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def to_iso(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def parse_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def parse_int(value, default, minimum=None):
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    if minimum is not None and parsed < minimum:
        return minimum
    return parsed


def connect_db(db_url):
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    return conn


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
        "blocked": 0,
    }
    for status, count in rows:
        counts[str(status)] = int(count)
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
            SELECT batch_id, worker_id, status, created_at, started_at, finished_at,
                   total, success, failure, skipped, last_error, zip_path, archive_path
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


def fetch_recent_failures(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, worker_id, batch_id, end_time, attempts, last_error
            FROM videos
            WHERE status = 'failure' AND COALESCE(last_error, '') <> ''
            ORDER BY end_time DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return rows


def fetch_bot_events(conn, limit):
    patterns = [
        "%bot-check%",
        "%bot block%",
        "%not a bot%",
        "%probe%",
        "%bot-block%",
    ]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts, run_id, level, message
            FROM run_logs
            WHERE lower(message) LIKE ANY(%s)
            ORDER BY ts DESC
            LIMIT %s
            """,
            (patterns, limit),
        )
        run_log_rows = cur.fetchall()

        cur.execute(
            """
            SELECT batch_id, worker_id, status, finished_at, last_error
            FROM batches
            WHERE status = 'paused' OR last_error = 'bot_check_threshold'
            ORDER BY finished_at DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        paused_rows = cur.fetchall()

    run_events = [
        {
            "ts": to_iso(ts),
            "source": "run_log",
            "run_id": run_id,
            "level": level,
            "message": message,
        }
        for ts, run_id, level, message in run_log_rows
    ]

    paused_events = [
        {
            "ts": to_iso(finished_at),
            "source": "batch",
            "batch_id": batch_id,
            "worker_id": worker_id,
            "level": "error",
            "message": f"Batch {batch_id} paused ({status}) reason={last_error or 'n/a'}",
        }
        for batch_id, worker_id, status, finished_at, last_error in paused_rows
    ]

    merged = run_events + paused_events
    merged.sort(key=lambda item: item.get("ts") or "", reverse=True)
    return merged[:limit]


def _capture_output(entry):
    proc = entry.get("process")
    stream = proc.stdout if proc else None
    if not stream:
        return

    for raw_line in iter(stream.readline, ""):
        if raw_line == "":
            break
        line = raw_line.rstrip("\r\n")
        with entry["log_lock"]:
            entry["logs"].append({"ts": utc_now_iso(), "line": line})

    try:
        stream.close()
    except Exception:
        pass

    rc = proc.poll() if proc else None
    if rc is not None:
        with entry["log_lock"]:
            entry["logs"].append(
                {
                    "ts": utc_now_iso(),
                    "line": f"[process exited with return code {rc}]",
                }
            )


def start_process(key, kind, cmd, env, meta=None):
    with STATE_LOCK:
        existing = STATE["processes"].get(key)
        if existing and existing["process"].poll() is None:
            raise RuntimeError(f"Process '{key}' is already running.")

        proc = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        entry = {
            "key": key,
            "kind": kind,
            "process": proc,
            "cmd": cmd,
            "meta": meta or {},
            "started_at": utc_now_iso(),
            "started_epoch": time.time(),
            "logs": deque(maxlen=LOG_BUFFER_SIZE),
            "log_lock": threading.Lock(),
        }
        STATE["processes"][key] = entry

        reader = threading.Thread(target=_capture_output, args=(entry,), daemon=True)
        entry["reader"] = reader
        reader.start()

    return entry


def process_snapshot(entry, include_logs=0):
    proc = entry.get("process")
    alive = proc.poll() is None

    snapshot = {
        "key": entry.get("key"),
        "kind": entry.get("kind"),
        "pid": proc.pid,
        "alive": alive,
        "returncode": None if alive else proc.poll(),
        "started_at": entry.get("started_at"),
        "uptime_seconds": round(time.time() - float(entry.get("started_epoch", time.time())), 1),
        "cmd": entry.get("cmd"),
        "meta": entry.get("meta") or {},
    }

    with entry["log_lock"]:
        if entry["logs"]:
            snapshot["last_log"] = entry["logs"][-1]
        else:
            snapshot["last_log"] = None
        if include_logs:
            snapshot["logs"] = list(entry["logs"])[-include_logs:]

    return snapshot


def stop_process_by_key(key):
    with STATE_LOCK:
        entry = STATE["processes"].get(key)

    if not entry:
        return None

    proc = entry["process"]
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except Exception:
            proc.kill()

    return process_snapshot(entry, include_logs=10)


def render_dashboard():
    def esc(value):
        return html.escape(str(value))

    html_template = """
<!DOCTYPE html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>YT Distributed Dashboard</title>
  <style>
    :root {
      --bg: #f7f9fc;
      --panel: #ffffff;
      --line: #d9e0ea;
      --ink: #1a2433;
      --muted: #5b6b80;
      --accent: #005f73;
      --warn: #9b2226;
      --ok: #2a9d8f;
    }
    * { box-sizing: border-box; }
    body { margin: 0; padding: 20px; font-family: \"Segoe UI\", Tahoma, sans-serif; background: var(--bg); color: var(--ink); }
    h1 { margin: 0 0 6px 0; font-size: 26px; }
    h2 { margin: 0 0 10px 0; font-size: 18px; }
    h3 { margin: 0 0 8px 0; font-size: 15px; }
    .muted { color: var(--muted); font-size: 13px; }
    .row { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 14px; margin-top: 14px; }
    .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px; }
    .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    label { display: block; font-size: 12px; color: var(--muted); margin-top: 8px; }
    input, select { width: 100%; padding: 7px; border: 1px solid var(--line); border-radius: 7px; }
    button { margin-top: 10px; padding: 8px 12px; border: 1px solid var(--line); border-radius: 8px; cursor: pointer; background: #f1f4f8; }
    button.primary { background: var(--accent); color: white; border-color: var(--accent); }
    button.danger { background: var(--warn); color: white; border-color: var(--warn); }
    .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; }
    .metric { border: 1px solid var(--line); border-radius: 8px; padding: 10px; background: #fafcff; }
    .metric .k { color: var(--muted); font-size: 12px; }
    .metric .v { font-size: 20px; font-weight: 600; }
    .progress { width: 100%; height: 10px; border-radius: 5px; background: #e4ebf3; overflow: hidden; margin-top: 8px; }
    .progress > div { height: 100%; background: var(--ok); }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { border: 1px solid var(--line); padding: 5px 6px; text-align: left; vertical-align: top; }
    th { background: #f2f6fb; }
    pre { margin: 0; border: 1px solid var(--line); border-radius: 8px; background: #f8fbff; padding: 10px; max-height: 280px; overflow: auto; font-size: 12px; }
  </style>
</head>
<body>
  <h1>YT Distributed Dashboard</h1>
  <div class=\"muted\">Auto-refresh: every 10 seconds. Uses current script and DB schema.</div>

  <div class=\"row\">
    <div class=\"panel\">
      <h2>Global Settings</h2>
      <label>Database URL</label>
      <input id=\"global-db-url\" value=\"__DB_URL__\" />
      <div class=\"muted\" style=\"margin-top:8px;\">Status/runs panels read this value.</div>
    </div>

    <div class=\"panel\">
      <h2>Host Maintenance</h2>
      <form id=\"host-form\">
        <label>Database URL</label>
        <input name=\"db_url\" value=\"__DB_URL__\" />
        <label>Backup Directory</label>
        <input name=\"backup_dir\" value=\"__BACKUP_DIR__\" />
        <div class=\"grid2\">
          <div>
            <label>Interval Minutes</label>
            <input name=\"interval_minutes\" type=\"number\" value=\"60\" />
          </div>
          <div>
            <label>Keep Backups</label>
            <input name=\"keep\" type=\"number\" value=\"3\" />
          </div>
        </div>
        <label>pg_dump path (optional)</label>
        <input name=\"pg_dump_path\" value=\"\" />
        <label><input type=\"checkbox\" name=\"reap\" checked /> Reap expired leases</label>
        <button class=\"primary\" type=\"button\" onclick=\"startHost()\">Start Host</button>
      </form>
      <button class=\"danger\" type=\"button\" onclick=\"stopByKey('host')\">Stop Host</button>
      <pre id=\"host-result\"></pre>
    </div>

    <div class=\"panel\">
      <h2>Downloader Worker</h2>
      <form id=\"download-form\">
        <label>Database URL</label>
        <input name=\"db_url\" value=\"__DB_URL__\" />
        <label>Run Directory (required)</label>
        <input name=\"run_dir\" value=\"__RUN_DIR__\" />
        <label>Worker ID (optional)</label>
        <input name=\"worker_id\" value=\"\" />
        <div class=\"grid2\">
          <div><label>Workers</label><input name=\"workers\" type=\"number\" value=\"6\" /></div>
          <div><label>Batch Size</label><input name=\"batch_size\" type=\"number\" value=\"1000\" /></div>
        </div>
        <div class=\"grid2\">
          <div><label>Overlap Batches</label><input name=\"overlap_batches\" type=\"number\" value=\"2\" /></div>
          <div><label>Max Batches (0 unlimited)</label><input name=\"max_batches\" type=\"number\" value=\"0\" /></div>
        </div>
        <div class=\"grid2\">
          <div><label>Lease Seconds</label><input name=\"lease_seconds\" type=\"number\" value=\"1800\" /></div>
          <div><label>Max Attempts</label><input name=\"max_attempts\" type=\"number\" value=\"3\" /></div>
        </div>
        <div class=\"grid2\">
          <div><label>Block Threshold</label><input name=\"block_threshold\" type=\"number\" value=\"20\" /></div>
          <div><label>Block Sleep Seconds</label><input name=\"block_sleep_seconds\" type=\"number\" value=\"900\" /></div>
        </div>
        <label><input type=\"checkbox\" name=\"retry_failures\" /> Retry failures</label>
        <label><input type=\"checkbox\" name=\"test_mode\" /> Test mode</label>
        <button class=\"primary\" type=\"button\" onclick=\"startDownloader()\">Start Downloader</button>
      </form>
      <pre id=\"download-result\"></pre>
    </div>

    <div class=\"panel\">
      <h2>Archiver Worker</h2>
      <form id=\"archiver-form\">
        <label>Name (for process key)</label>
        <input name=\"name\" value=\"main\" />
        <label>Database URL</label>
        <input name=\"db_url\" value=\"__DB_URL__\" />
        <label>Run Directory</label>
        <input name=\"run_dir\" value=\"__RUN_DIR__\" />
        <label>Archive Directory</label>
        <input name=\"archive_dir\" value=\"__ARCHIVE_DIR__\" />
        <label>Poll Interval (seconds)</label>
        <input name=\"poll_interval\" type=\"number\" value=\"10\" />
        <label><input type=\"checkbox\" name=\"keep_batch_dir\" /> Keep batch folders</label>
        <label><input type=\"checkbox\" name=\"keep_local_zip\" /> Keep local zips</label>
        <button class=\"primary\" type=\"button\" onclick=\"startArchiver()\">Start Archiver</button>
      </form>
      <pre id=\"archiver-result\"></pre>
    </div>
  </div>

  <div class=\"row\">
    <div class=\"panel\">
      <h2>Managed Processes</h2>
      <div class=\"grid2\">
        <div>
          <label>Process key for logs</label>
          <select id=\"log-key\"></select>
        </div>
        <div>
          <label>Process key to stop</label>
          <input id=\"stop-key\" value=\"\" />
        </div>
      </div>
      <button type=\"button\" onclick=\"refreshProcesses()\">Refresh Processes</button>
      <button class=\"danger\" type=\"button\" onclick=\"stopByInput()\">Stop Selected</button>
      <div style=\"margin-top:10px; overflow:auto;\"><table id=\"process-table\"></table></div>
      <h3 style=\"margin-top:12px;\">Live Process Logs</h3>
      <pre id=\"process-logs\"></pre>
    </div>

    <div class=\"panel\">
      <h2>Status Overview</h2>
      <div class=\"metrics\" id=\"metrics\"></div>
      <div class=\"progress\"><div id=\"progress-bar\" style=\"width:0%;\"></div></div>
      <div class=\"muted\" id=\"progress-text\" style=\"margin:6px 0 10px 0;\"></div>

      <h3>Dataset Progress</h3>
      <div style=\"overflow:auto;\"><table id=\"datasets-table\"></table></div>
      <h3 style=\"margin-top:12px;\">Active Workers (DB)</h3>
      <div style=\"overflow:auto;\"><table id=\"workers-table\"></table></div>
      <h3 style=\"margin-top:12px;\">Recent Batches</h3>
      <div style=\"overflow:auto;\"><table id=\"batches-table\"></table></div>
    </div>
  </div>

  <div class=\"row\">
    <div class=\"panel\">
      <h2>Errors</h2>
      <div style=\"overflow:auto;\"><table id=\"failures-table\"></table></div>
    </div>
    <div class=\"panel\">
      <h2>Bot-Block Signals</h2>
      <div style=\"overflow:auto;\"><table id=\"bot-table\"></table></div>
    </div>
  </div>

  <div class=\"row\">
    <div class=\"panel\">
      <h2>Runs + Run Logs</h2>
      <div class=\"grid2\">
        <div>
          <label>Worker filter (optional)</label>
          <input id=\"runs-worker\" value=\"\" />
        </div>
        <div>
          <label>Run count</label>
          <input id=\"runs-tail\" type=\"number\" value=\"10\" />
        </div>
      </div>
      <button type=\"button\" onclick=\"refreshRuns()\">Refresh Runs</button>
      <div style=\"margin-top:8px; overflow:auto;\"><table id=\"runs-table\"></table></div>

      <div class=\"grid2\" style=\"margin-top:10px;\">
        <div>
          <label>Run ID for logs</label>
          <input id=\"run-id\" value=\"\" />
        </div>
        <div>
          <label>Log tail</label>
          <input id=\"run-log-tail\" type=\"number\" value=\"100\" />
        </div>
      </div>
      <button type=\"button\" onclick=\"refreshRunLogs()\">Fetch Run Logs</button>
      <pre id=\"run-logs\"></pre>
    </div>
  </div>

  <script>
    function getDbUrl() {
      return document.getElementById("global-db-url").value || "";
    }

    function formToObject(formId) {
      const form = document.getElementById(formId);
      const obj = {};
      for (const el of form.elements) {
        if (!el.name) continue;
        if (el.type === "checkbox") {
          obj[el.name] = el.checked;
        } else {
          obj[el.name] = el.value;
        }
      }
      return obj;
    }

    async function fetchJson(url, options = {}) {
      const res = await fetch(url, options);
      let data = null;
      try {
        data = await res.json();
      } catch (_err) {
        data = { error: "Non-JSON response" };
      }
      if (!res.ok && data && !data.error) {
        data.error = res.statusText || "Request failed";
      }
      return data;
    }

    async function postJson(url, body) {
      return fetchJson(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body || {}),
      });
    }

    function renderTable(tableId, columns, rows) {
      const table = document.getElementById(tableId);
      if (!rows || rows.length === 0) {
        table.innerHTML = "<tr><td>No rows</td></tr>";
        return;
      }
      const thead = "<tr>" + columns.map((c) => `<th>${c.label}</th>`).join("") + "</tr>";
      const body = rows
        .map((row) => {
          return "<tr>" + columns.map((c) => `<td>${(row[c.key] ?? "").toString().replace(/</g, "&lt;")}</td>`).join("") + "</tr>";
        })
        .join("");
      table.innerHTML = thead + body;
    }

    async function startHost() {
      const body = formToObject("host-form");
      if (!body.db_url) body.db_url = getDbUrl();
      const data = await postJson("/api/start_host", body);
      document.getElementById("host-result").textContent = JSON.stringify(data, null, 2);
      refreshProcesses();
    }

    async function startDownloader() {
      const body = formToObject("download-form");
      if (!body.db_url) body.db_url = getDbUrl();
      const data = await postJson("/api/start_downloader", body);
      document.getElementById("download-result").textContent = JSON.stringify(data, null, 2);
      refreshProcesses();
    }

    async function startArchiver() {
      const body = formToObject("archiver-form");
      if (!body.db_url) body.db_url = getDbUrl();
      const data = await postJson("/api/start_archiver", body);
      document.getElementById("archiver-result").textContent = JSON.stringify(data, null, 2);
      refreshProcesses();
    }

    async function stopByKey(key) {
      const data = await postJson("/api/stop_process", { key: key });
      document.getElementById("host-result").textContent = JSON.stringify(data, null, 2);
      refreshProcesses();
    }

    async function stopByInput() {
      const key = document.getElementById("stop-key").value;
      if (!key) return;
      const data = await postJson("/api/stop_process", { key: key });
      document.getElementById("download-result").textContent = JSON.stringify(data, null, 2);
      refreshProcesses();
    }

    async function refreshProcesses() {
      const data = await fetchJson("/api/processes?tail=1");
      const rows = (data.processes || []).map((p) => ({
        key: p.key,
        kind: p.kind,
        pid: p.pid,
        alive: p.alive,
        returncode: p.returncode,
        uptime_seconds: p.uptime_seconds,
        worker: p.meta && p.meta.worker_id ? p.meta.worker_id : "",
        last_log: p.last_log ? p.last_log.line : "",
      }));
      renderTable(
        "process-table",
        [
          { key: "key", label: "key" },
          { key: "kind", label: "kind" },
          { key: "pid", label: "pid" },
          { key: "alive", label: "alive" },
          { key: "returncode", label: "return" },
          { key: "uptime_seconds", label: "uptime(s)" },
          { key: "worker", label: "worker" },
          { key: "last_log", label: "last log line" },
        ],
        rows
      );

      const select = document.getElementById("log-key");
      const current = select.value;
      const keys = (data.processes || []).map((p) => p.key);
      select.innerHTML = keys.map((k) => `<option value=\"${k.replace(/\"/g, "") }\">${k}</option>`).join("");
      if (current && keys.includes(current)) {
        select.value = current;
      }
      if (!document.getElementById("stop-key").value && keys.length) {
        document.getElementById("stop-key").value = keys[0];
      }

      refreshProcessLogs();
    }

    async function refreshProcessLogs() {
      const key = document.getElementById("log-key").value;
      if (!key) {
        document.getElementById("process-logs").textContent = "No tracked process selected.";
        return;
      }
      const data = await fetchJson(`/api/process_logs?key=${encodeURIComponent(key)}&tail=200`);
      const lines = (data.logs || []).map((r) => `[${r.ts}] ${r.line}`);
      document.getElementById("process-logs").textContent = lines.join("\n") || "No output yet.";
    }

    function renderMetrics(statusData) {
      const counts = statusData.counts || {};
      const metrics = [
        { k: "Total", v: statusData.total || 0 },
        { k: "Done", v: statusData.done || 0 },
        { k: "Pending", v: counts.pending || 0 },
        { k: "In Progress", v: counts.in_progress || 0 },
        { k: "Success", v: counts.success || 0 },
        { k: "Failure", v: counts.failure || 0 },
        { k: "Skipped", v: counts.skipped || 0 },
      ];
      const html = metrics
        .map((m) => `<div class=\"metric\"><div class=\"k\">${m.k}</div><div class=\"v\">${m.v}</div></div>`)
        .join("");
      document.getElementById("metrics").innerHTML = html;

      const pct = statusData.progress_pct || 0;
      document.getElementById("progress-bar").style.width = `${pct.toFixed(2)}%`;
      document.getElementById("progress-text").textContent = `Progress: ${pct.toFixed(2)}% | Remaining: ${statusData.remaining || 0}`;
    }

    async function refreshStatus() {
      const dbUrl = getDbUrl();
      const data = await fetchJson(`/api/status?db_url=${encodeURIComponent(dbUrl)}&tail=10`);
      if (data.error) {
        document.getElementById("progress-text").textContent = `Status error: ${data.error}`;
        return;
      }

      renderMetrics(data);

      renderTable(
        "datasets-table",
        [
          { key: "name", label: "dataset" },
          { key: "done", label: "done" },
          { key: "total", label: "total" },
          { key: "pct", label: "pct" },
        ],
        (data.datasets || []).map((d) => ({
          name: d.name,
          done: d.done,
          total: d.total,
          pct: `${d.pct.toFixed(2)}%`,
        }))
      );

      renderTable(
        "workers-table",
        [
          { key: "worker_id", label: "worker" },
          { key: "in_progress", label: "in_progress" },
        ],
        data.active_workers || []
      );

      renderTable(
        "batches-table",
        [
          { key: "batch_id", label: "batch" },
          { key: "worker_id", label: "worker" },
          { key: "status", label: "status" },
          { key: "total", label: "total" },
          { key: "success", label: "success" },
          { key: "failure", label: "failure" },
          { key: "skipped", label: "skipped" },
          { key: "created_at", label: "created_at" },
        ],
        data.recent_batches || []
      );

      renderTable(
        "failures-table",
        [
          { key: "id", label: "video_id" },
          { key: "worker_id", label: "worker" },
          { key: "batch_id", label: "batch" },
          { key: "attempts", label: "attempts" },
          { key: "end_time", label: "end_time" },
          { key: "last_error", label: "error" },
        ],
        data.recent_failures || []
      );

      renderTable(
        "bot-table",
        [
          { key: "ts", label: "ts" },
          { key: "source", label: "source" },
          { key: "level", label: "level" },
          { key: "message", label: "message" },
        ],
        data.bot_events || []
      );
    }

    async function refreshRuns() {
      const dbUrl = getDbUrl();
      const workerId = document.getElementById("runs-worker").value;
      const tail = document.getElementById("runs-tail").value;
      const data = await fetchJson(`/api/runs?db_url=${encodeURIComponent(dbUrl)}&tail=${encodeURIComponent(tail)}&worker_id=${encodeURIComponent(workerId)}`);
      if (data.error) {
        document.getElementById("run-logs").textContent = `Run fetch error: ${data.error}`;
        return;
      }
      renderTable(
        "runs-table",
        [
          { key: "run_id", label: "run_id" },
          { key: "worker_id", label: "worker" },
          { key: "script", label: "script" },
          { key: "status", label: "status" },
          { key: "error_count", label: "errors" },
          { key: "started_at", label: "started_at" },
          { key: "finished_at", label: "finished_at" },
          { key: "last_error", label: "last_error" },
        ],
        data.runs || []
      );
    }

    async function refreshRunLogs() {
      const dbUrl = getDbUrl();
      const runId = document.getElementById("run-id").value;
      const tail = document.getElementById("run-log-tail").value;
      if (!runId) {
        document.getElementById("run-logs").textContent = "Enter a run ID first.";
        return;
      }
      const data = await fetchJson(`/api/run_logs?db_url=${encodeURIComponent(dbUrl)}&run_id=${encodeURIComponent(runId)}&tail=${encodeURIComponent(tail)}`);
      if (data.error) {
        document.getElementById("run-logs").textContent = `Run log error: ${data.error}`;
        return;
      }
      const lines = (data.logs || []).map((l) => `[${l.ts}] ${String(l.level || "").toUpperCase()}: ${l.message}`);
      document.getElementById("run-logs").textContent = lines.join("\n") || "No logs found.";
    }

    async function refreshAll() {
      await Promise.allSettled([
        refreshProcesses(),
        refreshStatus(),
        refreshRuns(),
      ]);
    }

    document.getElementById("log-key").addEventListener("change", refreshProcessLogs);
    refreshAll();
    setInterval(refreshAll, 10000);
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
def get_processes(tail: int = 0):
    tail = parse_int(tail, 0, minimum=0)
    with STATE_LOCK:
        entries = list(STATE["processes"].values())
    rows = [process_snapshot(entry, include_logs=tail) for entry in entries]
    rows.sort(key=lambda item: item.get("started_at") or "", reverse=True)
    return JSONResponse({"processes": rows})


@app.get("/api/process_logs")
def get_process_logs(key: str, tail: int = 200):
    tail = parse_int(tail, 200, minimum=1)
    with STATE_LOCK:
        entry = STATE["processes"].get(key)
    if not entry:
        return JSONResponse({"error": "Unknown process key."}, status_code=404)
    with entry["log_lock"]:
        logs = list(entry["logs"])[-tail:]
    return JSONResponse({"key": key, "logs": logs})


@app.post("/api/stop_process")
async def stop_process(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    key = str(payload.get("key", "")).strip()
    if not key:
        return JSONResponse({"error": "Missing process key."}, status_code=400)

    snapshot = stop_process_by_key(key)
    if snapshot is None:
        return JSONResponse({"error": "Unknown process key."}, status_code=404)
    return JSONResponse({"status": "stopped", "process": snapshot})


@app.post("/api/stop_host")
def stop_host_compat():
    snapshot = stop_process_by_key("host")
    if snapshot is None:
        return JSONResponse({"status": "not running"})
    return JSONResponse({"status": "stopped", "process": snapshot})


@app.post("/api/start_host")
async def start_host(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    db_url = str(payload.get("db_url", DEFAULT_DB_URL)).strip() or DEFAULT_DB_URL
    backup_dir = str(payload.get("backup_dir", DEFAULT_BACKUP_DIR)).strip() or DEFAULT_BACKUP_DIR
    interval_minutes = parse_int(payload.get("interval_minutes", 60), 60, minimum=1)
    keep = parse_int(payload.get("keep", 3), 3, minimum=1)
    pg_dump_path = str(payload.get("pg_dump_path", "")).strip()
    reap = parse_bool(payload.get("reap"), default=True)

    cmd = [
        sys.executable,
        "backup_and_reap.py",
        "--backup-dir",
        backup_dir,
        "--interval-minutes",
        str(interval_minutes),
        "--keep",
        str(keep),
    ]
    if pg_dump_path:
        cmd.extend(["--pg-dump-path", pg_dump_path])
    if reap:
        cmd.append("--reap")

    env = os.environ.copy()
    env["DATABASE_URL"] = db_url

    try:
        entry = start_process(
            key="host",
            kind="host",
            cmd=cmd,
            env=env,
            meta={"db_url": db_url},
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)

    return JSONResponse({"status": "started", "process": process_snapshot(entry)})


@app.post("/api/start_downloader")
async def start_downloader(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    db_url = str(payload.get("db_url", DEFAULT_DB_URL)).strip() or DEFAULT_DB_URL
    run_dir = str(payload.get("run_dir", DEFAULT_RUN_DIR)).strip()
    worker_id = str(payload.get("worker_id", "")).strip()

    if not run_dir:
        return JSONResponse({"error": "run_dir is required."}, status_code=400)

    workers = parse_int(payload.get("workers", 4), 4, minimum=1)
    batch_size = parse_int(payload.get("batch_size", 1000), 1000, minimum=1)
    overlap_batches = parse_int(payload.get("overlap_batches", 2), 2, minimum=1)
    lease_seconds = parse_int(payload.get("lease_seconds", 1800), 1800, minimum=10)
    max_attempts = parse_int(payload.get("max_attempts", 3), 3, minimum=1)
    max_batches = parse_int(payload.get("max_batches", 0), 0, minimum=0)
    block_threshold = parse_int(payload.get("block_threshold", 20), 20, minimum=1)
    block_sleep_seconds = parse_int(payload.get("block_sleep_seconds", 900), 900, minimum=1)
    retry_failures = parse_bool(payload.get("retry_failures"), default=False)
    test_mode = parse_bool(payload.get("test_mode"), default=False)

    cmd = [
        sys.executable,
        "start_download.py",
        "--run-dir",
        run_dir,
        "--workers",
        str(workers),
        "--batch-size",
        str(batch_size),
        "--overlap-batches",
        str(overlap_batches),
        "--lease-seconds",
        str(lease_seconds),
        "--max-attempts",
        str(max_attempts),
        "--max-batches",
        str(max_batches),
        "--block-threshold",
        str(block_threshold),
        "--block-sleep-seconds",
        str(block_sleep_seconds),
    ]

    if worker_id:
        cmd.extend(["--worker-id", worker_id])
    if retry_failures:
        cmd.append("--retry-failures")
    if test_mode:
        cmd.append("--test-mode")

    key = f"download:{worker_id}" if worker_id else f"download:{int(time.time())}"

    env = os.environ.copy()
    env["DATABASE_URL"] = db_url

    try:
        entry = start_process(
            key=key,
            kind="downloader",
            cmd=cmd,
            env=env,
            meta={
                "db_url": db_url,
                "worker_id": worker_id or "",
                "run_dir": run_dir,
            },
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)

    return JSONResponse({"status": "started", "process": process_snapshot(entry)})


@app.post("/api/start_archiver")
async def start_archiver(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    name = str(payload.get("name", "main")).strip() or "main"
    db_url = str(payload.get("db_url", DEFAULT_DB_URL)).strip() or DEFAULT_DB_URL
    run_dir = str(payload.get("run_dir", DEFAULT_RUN_DIR)).strip()
    archive_dir = str(payload.get("archive_dir", DEFAULT_ARCHIVE_DIR)).strip()
    poll_interval = parse_int(payload.get("poll_interval", 10), 10, minimum=1)
    keep_batch_dir = parse_bool(payload.get("keep_batch_dir"), default=False)
    keep_local_zip = parse_bool(payload.get("keep_local_zip"), default=False)

    if not run_dir:
        return JSONResponse({"error": "run_dir is required."}, status_code=400)
    if not archive_dir:
        return JSONResponse({"error": "archive_dir is required."}, status_code=400)

    cmd = [
        sys.executable,
        "start_archiver.py",
        "--run-dir",
        run_dir,
        "--archive-dir",
        archive_dir,
        "--poll-interval",
        str(poll_interval),
    ]
    if keep_batch_dir:
        cmd.append("--keep-batch-dir")
    if keep_local_zip:
        cmd.append("--keep-local-zip")

    key = f"archiver:{name}"

    env = os.environ.copy()
    env["DATABASE_URL"] = db_url

    try:
        entry = start_process(
            key=key,
            kind="archiver",
            cmd=cmd,
            env=env,
            meta={
                "db_url": db_url,
                "run_dir": run_dir,
                "archive_dir": archive_dir,
            },
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)

    return JSONResponse({"status": "started", "process": process_snapshot(entry)})


@app.post("/api/start_worker")
async def start_worker_compat(request: Request):
    return await start_downloader(request)


@app.post("/api/stop_worker")
async def stop_worker_compat(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    worker_key = str(payload.get("worker_key", "")).strip()
    if not worker_key:
        return JSONResponse({"error": "Missing worker_key"}, status_code=400)

    snapshot = stop_process_by_key(worker_key)
    if snapshot is None:
        return JSONResponse({"status": "not running", "worker_key": worker_key})

    return JSONResponse({"status": "stopped", "worker_key": worker_key, "process": snapshot})


@app.get("/api/status")
def get_status(db_url: str = "", tail: int = 10, run_tail: int = 10, failure_tail: int = 20, bot_tail: int = 20):
    db_url = (db_url or DEFAULT_DB_URL).strip() or DEFAULT_DB_URL
    tail = parse_int(tail, 10, minimum=1)
    run_tail = parse_int(run_tail, 10, minimum=1)
    failure_tail = parse_int(failure_tail, 20, minimum=1)
    bot_tail = parse_int(bot_tail, 20, minimum=1)

    try:
        conn = connect_db(db_url)

        meta = fetch_meta(conn)
        counts = fetch_status_counts(conn)
        total = int(sum(int(v) for v in counts.values()))
        done = int(counts.get("success", 0) + counts.get("failure", 0) + counts.get("skipped", 0))
        remaining = max(0, total - done)
        progress_pct = (done / total * 100.0) if total else 0.0

        datasets = []
        for name, priority, total_count, done_count in fetch_dataset_progress(conn):
            total_count = int(total_count or 0)
            done_count = int(done_count or 0)
            datasets.append(
                {
                    "name": name,
                    "priority": int(priority),
                    "total": total_count,
                    "done": done_count,
                    "pct": (done_count / total_count * 100.0) if total_count else 0.0,
                }
            )

        workers = [
            {"worker_id": worker_id, "in_progress": int(count)}
            for worker_id, count in fetch_active_workers(conn)
        ]

        recent_batches = []
        for row in fetch_recent_batches(conn, tail):
            (
                batch_id,
                worker_id,
                status,
                created_at,
                started_at,
                finished_at,
                total_count,
                success,
                failure,
                skipped,
                last_error,
                zip_path,
                archive_path,
            ) = row
            recent_batches.append(
                {
                    "batch_id": batch_id,
                    "worker_id": worker_id,
                    "status": status,
                    "created_at": to_iso(created_at),
                    "started_at": to_iso(started_at),
                    "finished_at": to_iso(finished_at),
                    "total": int(total_count or 0),
                    "success": int(success or 0),
                    "failure": int(failure or 0),
                    "skipped": int(skipped or 0),
                    "last_error": last_error,
                    "zip_path": zip_path,
                    "archive_path": archive_path,
                }
            )

        recent_runs = []
        for row in fetch_runs(conn, run_tail, ""):
            (
                run_id,
                worker_id,
                script,
                status,
                started_at,
                finished_at,
                error_count,
                last_error,
            ) = row
            recent_runs.append(
                {
                    "run_id": run_id,
                    "worker_id": worker_id,
                    "script": script,
                    "status": status,
                    "started_at": to_iso(started_at),
                    "finished_at": to_iso(finished_at),
                    "error_count": int(error_count or 0),
                    "last_error": last_error,
                }
            )

        recent_failures = [
            {
                "id": video_id,
                "worker_id": worker_id,
                "batch_id": batch_id,
                "end_time": to_iso(end_time),
                "attempts": int(attempts or 0),
                "last_error": last_error,
            }
            for video_id, worker_id, batch_id, end_time, attempts, last_error in fetch_recent_failures(conn, failure_tail)
        ]

        bot_events = fetch_bot_events(conn, bot_tail)

        conn.close()

        return JSONResponse(
            {
                "counts": counts,
                "total": total,
                "done": done,
                "remaining": remaining,
                "progress_pct": progress_pct,
                "meta": meta,
                "datasets": datasets,
                "active_workers": workers,
                "recent_batches": recent_batches,
                "recent_runs": recent_runs,
                "recent_failures": recent_failures,
                "bot_events": bot_events,
            }
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/runs")
def get_runs(db_url: str = "", tail: int = 10, worker_id: str = ""):
    db_url = (db_url or DEFAULT_DB_URL).strip() or DEFAULT_DB_URL
    tail = parse_int(tail, 10, minimum=1)

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
                    "error_count": int(error_count or 0),
                    "last_error": last_error,
                }
            )

        return JSONResponse({"runs": runs})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/run_logs")
def get_run_logs(db_url: str = "", run_id: str = "", tail: int = 100):
    db_url = (db_url or DEFAULT_DB_URL).strip() or DEFAULT_DB_URL
    run_id = run_id.strip()
    tail = parse_int(tail, 100, minimum=1)

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



