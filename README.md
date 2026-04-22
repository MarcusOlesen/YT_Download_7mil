# YouTube Distributed Downloader

Distributed YouTube downloader coordinated by a shared Postgres database.

This project is designed for large ID lists and multi-machine execution. The database is the single source of truth, so workers can run in parallel without downloading the same video twice.

## Core idea

- Store all target video IDs in Postgres once.
- Workers claim work atomically and download locally.
- Progress, retries, and failures are tracked centrally.
- Workers are resumable and safe to restart.

## Database hosting

The current deployment hosts Postgres on **UCloud**, but any reachable Postgres instance works.

Important: create a `.env` file in the repository root with a valid `DATABASE_URL`. All scripts default to this variable unless `--db-url` is provided.

Example:

```text
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require
```

## Project layout

- `create_database.py`: creates schema and loads IDs from parquet files.
- `start_download.py`: primary downloader worker.
- `rerun_failed_batch.py`: retry failed videos from one batch.
- `get_status.py`: quick progress overview.
- `backup_and_reap.py`: optional lease reaper + timestamped DB backups.
- `reset_database.py`: destructive reset of downloader tables.
- `start_archiver.py`: optional zip/archive worker (not required for downloading).
- `dashboard.py`: local dashboard (work in progress).
- `setup_worker.py`: one-time worker auth/profile setup.
- `refresh_cookies.py`: refresh cookies for an existing worker profile.
- `utilities/get_video_info.py`: inspect DB state for specific IDs.
- `utilities/get_run_report.py`: inspect run history and logs.
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

If you move the Parquet files elsewhere, pass custom paths:

```powershell
python create_database.py --ids-ok path\to\ids_ok_sorted.parquet `
  --ids-no-upload path\to\ids_no_uploadinfo.parquet `
  --ids-errors path\to\ids_with_errors.parquet
```

5) Install a JavaScript runtime/engine could be dino or node.js. To install dino on Windows, run:
  
```powershell
irm https://deno.land/install.ps1 | iex
```

Check dependencies:

```powershell
python -c "from scraper_utils import check_dependencies; check_dependencies()"
```

6) Run one-time worker setup for each machine/run-dir.

This creates `worker_profile.json` and machine-specific YouTube cookies used by the anti-block pipeline.

```powershell
python setup_worker.py --run-dir "D:\yt_download_worker_a" --preset normal
```

If cookies expire later, refresh them without changing your run-dir:

```powershell
python refresh_cookies.py --run-dir "D:\yt_download_worker_a"
```

## Start UCloud Database

To run the central Postgres database we use a persistent storage folder on **UCloud** and start PostgreSQL automatically using an initialization script. This job should be running 24/7.

### Prerequisites

Before starting the database job you must:

* Have an **SSH key registered in UCloud**
* Have access to the shared folder:

```
/SDU_data/youtubeDB
```

This folder contains the database files and the initialization script.



### Start the database job

1. Open **UCloud** and start a new **Ubuntu job**.

2. Machine type
   Choose the **smallest available machine**, for example:

```
u1-standard-h-1
```

This is sufficient because the database workload is very light.

3. Storage
   Attach the folder:

```
/SDU_data/youtubeDB
```

4. Initialization script
   Enable **Initialization** and select:

```
/SDU_data/youtubeDB/db_init.sh
```

This script automatically starts PostgreSQL from the persistent storage directory.

5. Start the job.

After the job starts, the initialization script will run automatically.

Wait **about 1 minute** for the script to complete.

At this point the **Postgres database is running and ready to accept connections.**

## Connect from Remote Machine

Because **Eduroam blocks direct connections to self-hosted databases**, we must connect through an **SSH tunnel**.

This section is mainly relevant when working from **DATALAB or other restricted networks**.

### 1. Get the SSH command from UCloud

Open the running Ubuntu job and look at the ðŸ”‘**SSH tab**.

You will see something like:

```
ssh ucloud@ssh.cloud.sdu.dk -p 1234
```

The **port number (`1234`) changes for every job**, so write it down.

See:

![image](img/ssh.png)

### 2. Create the SSH tunnel from your local machine

Run the following command locally (need to install auto ssh):

```bash
while ($true) {
    ssh -v -N `
      -o ServerAliveInterval=60 `
      -o ServerAliveCountMax=3 `
      -o TCPKeepAlive=yes `
      -o ExitOnForwardFailure=yes `
      -i C:\Users\usr\.ssh\id_key `
      -L 15432:localhost:5432 `
      ucloud@ssh.cloud.sdu.dk -p 1234

    Start-Sleep -Seconds 5
}
```

Where:

* `C:\Users\usr\.ssh\id_key` is the location of your SSH private key
* `1234` is the port number shown in the UCloud SSH tab

This command creates a **tunnel from your local port `15432` to the database port `5432` on the UCloud VM**.

Once the tunnel is running, the database can be accessed locally via:

```
localhost:15432
```


If you for whatever reason need to query the database directly you can access `psql` like so:

```
psql -h localhost -p 15432 -U postgres -d youtubedb
```

### 3. Configure the project

If it does not already exist, copy the `.env` file from:

```
O:\ARTS_SoMe-Influence\YT_Download_all_videos\.env
```

into the root of this repository.

All project scripts read the database connection from this file (otherwise you have to specify this with `--db-url` when using any script connecting to the database).

After the SSH tunnel is active and `.env` is present, **all scripts will work normally**.


## Run downloads

`--run-dir` is required. Set it explicitly for each worker machine, for example on `D:\`.

Important: `start_download.py` now expects a prepared worker profile in the run dir.
Run `setup_worker.py` once per machine/run-dir before starting workers.

Single worker example:

```powershell
python start_download.py --run-dir "D:\yt_download_worker_a" --workers 8
```

This also writes the script's terminal output to a timestamped file under `D:\yt_download_worker_a\logs` by default. Override that location with `--log-dir`.

Legacy sequential behavior:

```powershell
python start_download.py --run-dir "D:\yt_download_worker_a" --workers 8 --overlap-batches 1
```

Recommended multi-machine practice:

- Use a unique `--worker-id` per machine (or let each machine persist one in its own run dir).
- Use a unique `--run-dir` per machine.
- Keep all workers pointed to the same Postgres database.

What goes into `--run-dir`:

- `worker_id.txt`
- `block_wait_state.json`
- `logs/start_download_<worker_id>_<timestamp>.log`
- `batches/<batch_id>/videos`
- `batches/<batch_id>/logs`
- `probe/` and `probe_logs/` when bot-check probing is active

## Retry failed videos from a batch

```powershell
python rerun_failed_batch.py --batch-id BATCH_ID --run-dir "D:\yt_download_worker_a" --workers 8
```

This creates a retry batch and processes only entries that currently have `status='failure'` in the source batch.

## Backup and Reap
Reap expired leases + save backups (up to 3) every `--interval-minutes`:

```powershell
python backup_and_reap.py --backup-dir "O:\ARTS_SoMe-Influence\YT_Download_all_videos\DB_backup" --interval-minutes 60 --reap
```

This also writes the script's terminal output to a timestamped file under `O:\ARTS_SoMe-Influence\YT_Download_all_videos\DB_backup\logs` by default. Override that location with `--log-dir`.

One-time backup:

```powershell
python backup_and_reap.py --backup-dir "O:\ARTS_SoMe-Influence\YT_Download_all_videos\DB_backup" --once
```

One-time reap:

```powershell
python backup_and_reap.py --reap
```

## Status and monitoring

Quick status snapshot:

```powershell
python get_status.py
```

`start_download.py` and `backup_and_reap.py` now capture their terminal output internally, so external `Tee-Object` is only needed for other scripts.

Eventually this could be made as a nice overview in the dashboard script but for now this is a WIP

## PowerShell supervisor

`supervise_workers.ps1` can keep the SSH tunnel and selected long-running workers alive. The SSH port, SSH key path, and forwarded local DB port are all parameters, so you do not have to hardcode them.

For easier day-to-day use, you can launch it through `start_supervisor.bat`. The first run creates `supervise_workers.config.psd1` from `supervise_workers.config.example.psd1`. After that, edit the config file instead of rewriting a long command line.

Typical flow:

```powershell
python setup_worker.py --run-dir "D:\yt_download_worker_a" --preset normal
.\start_supervisor.bat
```

If the config file already exists and you just want to edit it:

```powershell
.\start_supervisor.bat -EditConfig
```

Useful settings to change in `supervise_workers.config.psd1`:

- `KeyPath`
- `SshPort`
- `LocalDbPort`
- `RunDir`
- `DownloadArgs`
- `StartBackupAndReap`
- `BackupDir`
- `BackupArgs`
- `StartArchiver`
- `ArchiveDir`

Downloader only:

```powershell
.\supervise_workers.ps1 `
  -KeyPath "C:\Users\usr\.ssh\id_key" `
  -SshPort 1234 `
  -StartDownloader `
  -RunDir "D:\yt_download_worker_a" `
  -DownloadArgs @("--workers", "8")
```

Downloader plus backup/reap:

```powershell
.\supervise_workers.ps1 `
  -KeyPath "C:\Users\usr\.ssh\id_key" `
  -SshPort 1234 `
  -StartDownloader `
  -RunDir "D:\yt_download_worker_a" `
  -DownloadArgs @("--workers", "8") `
  -StartBackupAndReap `
  -BackupDir "O:\ARTS_SoMe-Influence\YT_Download_all_videos\DB_backup" `
  -BackupArgs @("--interval-minutes", "60", "--reap")
```

Notes:

- `backup_and_reap.py` is optional. It starts only when `StartBackupAndReap = $true` in the config, or when you pass `-StartBackupAndReap` directly to `supervise_workers.ps1`.
- If you change the forwarded local DB port, the supervisor rewrites `DATABASE_URL` for child processes to use `localhost:<LocalDbPort>` unless you pass `-DatabaseUrl` explicitly.
- Use `-StartArchiver` and `-ArchiveDir` if you also want to supervise the archiver.
- Use `-ValidateOnly` to check the configuration without starting any processes.
- `start_supervisor.bat -ValidateOnly` runs the launcher plus config loading without starting the supervised processes.

## Utility scripts

Run these from the repository root using module form:

1. `utilities.get_video_info` - inspect DB status, paths, attempts, errors, and batch linkage for one or many IDs.

```powershell
python -m utilities.get_video_info --id VIDEO_ID
python -m utilities.get_video_info --ids "id1,id2,id3" --format json
python -m utilities.get_video_info --ids-file ids.txt --format csv --out info.csv
```

2. `utilities.get_run_report` - inspect recent runs or logs for one specific run ID.

```powershell
python -m utilities.get_run_report --tail 10
python -m utilities.get_run_report --run-id RUN_ID --log-tail 100
```

## Create the UCloud Database Setup From Scratch

If the database environment must be rebuilt, follow these steps.

### 1. Create persistent storage

Create a folder in UCloud storage, for example:

```
/SDU_data/DB
```

Start a new **Ubuntu job** using this folder.

Inside the job environment the folder will appear as:

```
/work/DB
```

### 2. Install PostgreSQL

Update the package manager and install PostgreSQL:

```bash
sudo apt update -y
sudo apt install postgresql postgresql-contrib -y
```

Note:
The version installed here is **PostgreSQL 16**.
This version number may change in future Ubuntu releases and is **IMPORTANT** for the paths.


### 3. Move the database cluster to persistent storage

Copy the default cluster to the persistent folder:

```bash
sudo rsync -av /var/lib/postgresql/16/main/ /work/DB/
```

Allow the postgres user to access the shared storage group:

```bash
sudo usermod -a -G ucloud postgres
```

### 4. Fix permissions (very important)

PostgreSQL requires that it **owns all database files**.

```bash
sudo chown postgres:postgres /work/DB
sudo chmod 700 /work/DB
```

Permissions must be **700 or 750** and owned by `postgres`.


### 5. Copy configuration files

Switch to the postgres user (`sudo -i -u postgres`) and copy the config files:

```bash
cp /etc/postgresql/16/main/postgresql.conf /work/DB/
cp /etc/postgresql/16/main/pg_hba.conf /work/DB/
cp /etc/postgresql/16/main/pg_ident.conf /work/DB/
mkdir /work/DB/conf.d
```

### 6. Update paths in `postgresql.conf`

Edit the config (still as posgres user):

```bash
nano /work/DB/postgresql.conf
```

Look for lines similar to:

```
data_directory = '/var/lib/postgresql/16/main'
hba_file = '/etc/postgresql/16/main/pg_hba.conf'
ident_file = '/etc/postgresql/16/main/pg_ident.conf'
```

Change them to:

```
data_directory = '/work/DB'
hba_file = '/work/DB/pg_hba.conf'
ident_file = '/work/DB/pg_ident.conf'
```

Save and exit:

```
CTRL + O
ENTER
CTRL + X
```

### 7. Update access rules

Edit:

```
/work/DB/pg_hba.conf
```

Find lines like:

```
local   all   all                 peer
host    all   all   127.0.0.1/32  scram-sha-256
host    all   all   ::1/128       scram-sha-256
```

Change `scram-sha-256` to:

```
trust
```

This allows connections without a password.

This is safe because the database is **only reachable through the SSH tunnel**, which requires an SSH key.

### 8. Start PostgreSQL

Start the server manually:

```bash
sudo -u postgres /usr/lib/postgresql/16/bin/postgres \
-D /work/DB \
-c config_file='/work/DB/postgresql.conf'
```
Then you need to create a database with psql

Once confirmed working, the same command should be executed automatically by the initialization script (`db_init.sh`) whenever a new UCloud job starts.

## Initialize the database (run only once after database is newly created)

With `.env` in place, initialize tables and load IDs:

```powershell
python create_database.py --batch-size 1000
```

You can override input parquet paths if needed:

```powershell
python create_database.py --ids-ok path\to\ids_ok_sorted.parquet `
  --ids-no-upload path\to\ids_no_uploadinfo.parquet `
  --ids-errors path\to\ids_with_errors.parquet
```

### Troubleshooting

If PostgreSQL does not start, the most common causes are:

* Incorrect paths in `postgresql.conf`
* Missing `conf.d` directory
* Incorrect permissions
* Files not owned by `postgres`

Verify ownership:

```bash
ls -l /work/DB
```

All database files and directories must be owned by:

```
postgres:postgres
```

Downloader troubleshooting matrix:

| Symptom | Likely cause | Action |
|---|---|---|
| Worker exits with missing worker profile | `setup_worker.py` was not run for this `--run-dir` | Run `python setup_worker.py --run-dir ...` once, then restart worker |
| Worker exits with anti-block dependency errors | Missing `deno`, `yt_dlp_ejs`, or `bgutil-ytdlp-pot-provider` | Install requirements and rerun setup |
| Worker exits with invalid/missing cookies | Cookie file missing, expired, or corrupted | Run `python refresh_cookies.py --run-dir ...` |
| Logs show `Global cooldown active` | Another worker (or this one) hit rate-limit and wrote shared cooldown to DB meta | Wait for cooldown to expire; workers resume automatically |
| Logs show shared cooldown read/write warning | Temporary DB issue while handling cooldown key | Worker continues with local pacing (soft mode); inspect DB/tunnel stability |

## Optional components

### Archiver (optional)

`start_archiver.py` watches downloaded local batch folders, zips them, and copies them to a shared archive directory. It was originally used to move local dowloads to shared drive after download completion.

If you are useing a external harddrive, you can skip it.

```powershell
python start_archiver.py --run-dir "D:\yt_download_worker_a" --archive-dir "O:\ARTS_SoMe-Influence\YT_Download_all_videos\archive"
```

### Dashboard (WIP)

`dashboard.py` is still work in progress. Use CLI scripts above as the primary and supported workflow.

## Reset database (destructive)

```powershell
python reset_database.py
```

Use only when you need to wipe downloader state and start over.

## Notes

- Keep `DATABASE_URL` valid in `.env` on every machine that runs scripts.
- Use `backup_and_reap.py --reap` (or another scheduled process) so expired leases are reclaimed.
- Avoid sharing a single `--run-dir` across multiple machines.
- Archive flow is optional; downloader does not require it.
