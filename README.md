# YouTube Distributed Downloader

This folder contains a distributed, batch-based YouTube downloader for very
large ID lists. It uses a shared Postgres database to coordinate work across
multiple machines and avoid duplicate downloads.

## What it does

- Reads video IDs from three Parquet files in priority order.
- Loads all IDs into Postgres once (init step).
- Workers claim batches atomically and download in parallel.
- Each worker writes local batch folders/zips and can be stopped/restarted safely.
- Run history and error logs are stored in the DB for auditing.

## Design overview

- A single Postgres database is the source of truth for all workers.
- Each worker claims IDs using transactional locks, so no two workers download
  the same video.
- Leases prevent permanent lockups if a worker crashes; expired leases are
  reclaimed.
- Local output lives on each worker under its `--run-dir`.

## Files and layout

- `create_database.py` - initialize the Postgres schema and load IDs (run once).
- `reset_database.py` - destructive full reset (drops all downloader tables).
- `host_database.py` - monitoring + timestamped backups (run on DB host).
- `start_download.py` - worker process (run on each download machine).
- `rerun_failed_batch.py` - retry failed videos from a specific batch.
- `get_status.py` - status summary (Postgres).
- `utilities/get_video_info.py` - lookup video status/paths/logs by ID.
- `utilities/get_run_report.py` - report run history and error logs.
- `start_archiver.py` - archive local zips to a shared archive folder.
- `dashboard.py` - (WIP) web dashboard to start/stop host and workers, view status. 
- `distributed_core.py` - shared logic used by the scripts above.
- `scraper_utils.py` - download helpers.
- `data/ids_ok_sorted.parquet`, `data/ids_no_uploadinfo.parquet`,
  `data/ids_with_errors.parquet` are the input ID lists.

First data-file has highest priority and contains IDs where we have metadata sorted by upload date.
Second data-file has IDs with metadata but no upload info. Third data-file has IDs that previously had errors when trying to get metadata.

During a run, each worker writes under its local run directory, for example:

- `download_run_workerA/batches/WORKER_BATCH_ID/`
- `download_run_workerA/zips/WORKER_BATCH_ID.zip`

## Setup

1) Install Python 3.10+.
   - Windows: https://www.python.org/downloads/

2) Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
python -m pip install -U --pre "yt-dlp[default]"
```

3) Install ffmpeg and ensure it is on PATH.
   - Windows builds (choose one):
     - https://www.gyan.dev/ffmpeg/builds/
     - https://github.com/BtbN/FFmpeg-Builds/releases
   - Steps (Windows):
     1. Download a zip and extract it (e.g., `C:\tools\ffmpeg`).
     2. Add `C:\tools\ffmpeg\bin` to your PATH.
     3. Verify in PowerShell:

   - Alternatively download via 

```powershell
winget install -e --id Gyan.FFmpeg
```

Verify installation:
```powershell
ffmpeg -version
ffprobe -version
```
NOTE: shell needs to be restarted for the PATH change to take effect  


4) Install Postgres client tools on the DB host (for `pg_dump`).
   - Windows installer: https://www.postgresql.org/download/windows/
   - Linux (Debian/Ubuntu): `sudo apt-get install postgresql-client`
   - macOS (Homebrew): `brew install postgresql`

Verify `pg_dump` is available:

```powershell
pg_dump --version
```

I had to manually add it to my PATH on Windows:
```powershell
setx PATH "$env:PATH;C:\Program Files\PostgreSQL\18\bin"
```

If you move the Parquet files elsewhere, pass custom paths:

```powershell
python create_database.py --ids-ok path\to\ids_ok_sorted.parquet `
  --ids-no-upload path\to\ids_no_uploadinfo.parquet `
  --ids-errors path\to\ids_with_errors.parquet
```

5) Install a JavaScript runtime/engine could be dino   or node.js. To install dino on Windows, run:
  
```powershell
irm https://deno.land/install.ps1 | iex
```

Check dependencies:

```powershell
python -c "from scraper_utils import check_dependencies; check_dependencies()"
``` 

## Initialize the database (run once)

Set `DATABASE_URL` (or pass `--db-url`) and load IDs into Postgres:

```powershell
$env:DATABASE_URL="postgresql://postgres:YouTube@localhost:5432/yt_downloads"
python create_database.py --batch-size 1000
```

Notes:
- Run this once per database. Re-running will fail if the videos table is already populated.
- If you need to reinitialize from scratch, create a new empty database.

## Reset database (destructive)

This deletes all downloader tables and state from the database. Use only
if you want a clean slate. The script asks for multiple confirmations.

```powershell
$env:DATABASE_URL="postgresql://postgres:YouTube@localhost/yt_downloads"
python reset_database.py
```

After this, run `create_database.py` again to reload IDs.

## Host monitoring + backups

Run this on the database host (or anywhere with `pg_dump` access). It creates
timestamped backups and keeps the newest 3 files (current + two older). When
`--reap` is enabled, each loop reclaims expired leases, runs a backup, rotates
old dumps, then sleeps for `--interval-minutes`.

```powershell
$env:DATABASE_URL="postgresql://postgres:YouTube@localhost:5432/yt_downloads"
python host_database.py --backup-dir "O:\ARTS_SoMe-Influence\YT_Download_all_videos\DB_backup" --interval-minutes 60 --reap
```

One-time backup:

```powershell
python host_database.py --backup-dir "O:\ARTS_SoMe-Influence\YT_Download_all_videos\DB_backup" --once
```

Tips:
- Use Windows Task Scheduler to run `host_database.py` at boot or on a schedule.
- Keep backups on a different drive or network share.

## Run workers

Each machine should use a unique `--worker-id` and its own local `--run-dir`:

You can change `--batch-size` between runs; only new batches use the new size,
and the DB metadata is updated automatically. This should only be done if necessary. 

```powershell
$env:DATABASE_URL="postgresql://postgres:YouTube@localhost:5432/yt_downloads"
python start_download.py --workers 8
```

If you omit `--worker-id`, a unique ID is generated and stored in `worker_id.txt`
inside the `--run-dir` folder, and reused on future runs. If you pass
`--worker-id`, it is written to `worker_id.txt` so the same name is reused
automatically next time.

Batch IDs are sequential per worker: `workerA_00001`, `workerA_00002`, etc.
Retry batches use `workerA_retry_00001`. If you want shorter batch names,
choose a shorter `--worker-id`.

## Run archiver
Zip batch folders created by workers and copy the zips to a shared archive (local zips are deleted by default):

```powershell
python start_archiver.py --run-dir download_run_workerA --archive-dir "O:\ARTS_SoMe-Influence\YT_Download_all_videos\archive"
```

Keep local zips / files after archiving:

```powershell
python start_archiver.py --run-dir download_run_workerA --archive-dir "archive" --keep-batch-dir --keep-local-zip
```

## Re-run failed batch

Retry only the failed videos from a specific batch ID. This creates a new
retry batch and leaves the original batch record intact.

```powershell
python rerun_failed_batch.py --batch-id BATCH_ID --worker-id workerA --run-dir download_run_workerA --workers 8
```

## Status reporting

View current progress without affecting state:

```powershell
python get_status.py
```

## Dashboard 

Run a local web dashboard to start/stop the host process, start workers, and
see live status/run reports:

```powershell
python dashboard.py
```

Then open `http://localhost:8000` in a browser. The form fields default to the
archive/backup paths shown earlier in this README.

Notes:
- The dashboard starts processes only on the machine where it is running.
- It is a local tool with no auth; use it on a trusted network.


## Utilities

Lookup video info for one ID:

```powershell
python utilities/get_video_info.py --id VIDEO_ID
```

Lookup multiple IDs (comma-separated):

```powershell
python utilities/get_video_info.py --ids "id1,id2,id3" --format json
```

Lookup IDs from a file and write CSV:

```powershell
python utilities/get_video_info.py --ids-file ids.txt --format csv --out info.csv
```

Run history and logs:

```powershell
python utilities/get_run_report.py --tail 10
python utilities/get_run_report.py --run-id RUN_ID --log-tail 50
```

## Notes

- Archive directory must exist or be creatable; scripts verify write access.
- `--archive-dir` is required for all downloads; workers will refuse to start without it.
- Local zips are deleted by default after a successful archive copy; use
  `--keep-local-zip` to retain them.
- Postgres is the source of truth for distributed progress and resume state.
- Leases are set per video on claim and expire after `--lease-seconds`.
  `host_database.py --reap` only resets expired leases; active downloads are not touched.
- Workers extend leases periodically during active batches so long runs are safe.
- Per-video failure logs are written under each batch folder in `logs/`.
- Batch folders are deleted after zipping unless `--keep-batch-dir` is set.
