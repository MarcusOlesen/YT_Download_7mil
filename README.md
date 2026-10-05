# YouTube Distributed Downloader

A distributed downloader for a research dataset of up to seven million YouTube videos, known only by their video IDs. There is no feed or bulk export for this, so every video has to be fetched one at a time with [yt-dlp](https://github.com/yt-dlp/yt-dlp), from several machines in parallel. The hard parts are coordination and failure handling: no video should be downloaded twice, a crashed machine must not lose or block work, and YouTube's bot checks should slow a worker down instead of burning through the list. The approach is a shared Postgres table used as the work queue. Workers claim batches atomically with `FOR UPDATE SKIP LOCKED`, hold time-limited leases that they keep extending while they work, and write each result back. A separate reaper returns abandoned work, and each worker backs off on its own when it detects bot checks.

Operational details for the current deployment (UCloud, SSH tunnel, backups, supervisor config) are in [docs/operations.md](docs/operations.md).

## Architecture

```mermaid
flowchart LR
    subgraph DBHOST["Database host (UCloud VM)"]
        PG[("Postgres<br/>videos, batches, runs,<br/>run_logs, meta")]
    end

    subgraph M1["Worker machine 1"]
        SUP1["supervise_workers.ps1<br/>restarts children on exit"]
        T1["ssh -N -L 15432:localhost:5432"]
        W1["start_download.py<br/>thread pool + lease heartbeat"]
        RD1[("run dir<br/>worker_id.txt, worker_profile.json,<br/>auth/cookies, block_wait_state.json,<br/>batches/&lt;id&gt;/videos")]
        BR["backup_and_reap.py (optional)<br/>reap expired leases + pg_dump"]
        SUP1 --> T1
        SUP1 --> W1
        SUP1 -.-> BR
        W1 --> RD1
        W1 -->|"localhost:15432"| T1
        BR --> T1
    end

    subgraph MN["Worker machine N"]
        SUPN["supervise_workers.ps1"]
        TN["ssh tunnel"]
        WN["start_download.py"]
        RDN[("own run dir")]
        SUPN --> TN
        SUPN --> WN
        WN --> RDN
        WN --> TN
    end

    T1 ==>|SSH| PG
    TN ==>|SSH| PG
    W1 -->|"yt-dlp, per-machine cookies, PO tokens"| YT(["YouTube"])
    WN --> YT
```

Each worker machine runs its own supervisor, tunnel and downloader, and keeps all local state in its own run dir. The only shared state is the Postgres database. The reaper/backup process only needs to run in one place. The SSH tunnel is there because the eduroam network blocks direct connections to self-hosted databases.

## How it works

### Life of a video

```mermaid
flowchart LR
    P(["pending"]) -->|"1 claim"| IP(["in_progress"])
    IP -->|"2 downloaded"| S(["success"])
    IP -->|"3 nothing to download"| K(["skipped"])
    IP -->|"4 gave up"| F(["failure"])
    IP -.->|"5 released"| P
    F -.->|"6 retry"| IP
```

| # | When | `attempts` |
|---|---|---|
| 1 | A worker claims the video as part of a batch. | +1 |
| 2 | The file was downloaded. | unchanged |
| 3 | The video is permanently unavailable (private, removed), or this machine already has the file. | unchanged |
| 4 | A download error after both player clients were tried. Also: the lease ran out on the last attempt, or the video hit the bot check 10 times. | unchanged |
| 5 | Bot check (attempt given back, `blocked_count` + 1). Batch paused before the video started (attempt given back). Lease ran out and the reaper reset it (attempt kept). | see left |
| 6 | Only with `--retry-failures` or `rerun_failed_batch.py`, and only while `attempts < max_attempts`. | +1 |

### Step by step

1. **Loading.** [`create_database.py`](create_database.py) creates the schema and streams IDs from three Parquet files into the `videos` table as `pending`, each row tagged with a dataset priority and row index ([`load_dataset`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L395-L432), [`videos` schema](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L192-L234)).
2. **Claiming.** A worker takes the next batch (default 1000) in a single statement: a CTE selects `pending` rows with `attempts < max_attempts` in priority order with `FOR UPDATE SKIP LOCKED`, and the `UPDATE` sets `status = 'in_progress'`, `worker_id`, `batch_id`, `lease_until = now() + lease_seconds`, and increments `attempts` ([`claim_videos`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L505-L546)). The batch gets a row in `batches` and its own directory in the run dir ([`claim_new_batch`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/start_download.py#L218-L307)).
3. **Holding the lease.** While the batch is active, a background thread extends the lease for all of the batch's `in_progress` rows every half lease period (minimum 30 s) ([`start_lease_heartbeat`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/worker_common.py#L161-L193), [`extend_batch_leases`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L818-L831)).
4. **Downloading.** IDs whose file already exists anywhere in this machine's run dir are marked `skipped` without a download ([`build_run_dir_index`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L659-L672), [`start_download.py#L274-L290`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/start_download.py#L274-L290)). The rest go to a thread pool. With `--overlap-batches 2` (the default) the next batch is claimed as soon as the current one has nothing left to schedule, so the pool does not drain between batches ([`maybe_claim_overlap_batch`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/start_download.py#L309-L345)). Each download is paced, tried with two player clients, and classified ([`download_video`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/scraper_utils.py#L671-L761)).
5. **Writing the result.** `success`, `failure` or `skipped` is written with the error text, elapsed time, output path and log path, and the lease is cleared. The `UPDATE` is guarded by `worker_id`, so a worker whose row was reaped and handed to someone else cannot overwrite it ([`update_video_result`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L791-L815)).
6. **Bot checks.** A result is `blocked` when the last line of the yt-dlp log starts with the "Sign in to confirm you're not a bot" error ([`is_bot_blocked_log`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L80-L96)), or when rate-limit retries run out. Blocked videos go back to `pending` with their attempt given back and `blocked_count` increased ([`release_blocked_video`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L750-L788)). After 20 consecutive blocks the batch is paused, its unscheduled videos go back to `pending`, and the worker enters the backoff loop described below ([`finish_batch`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/start_download.py#L348-L392), [`start_download.py#L422-L475`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/start_download.py#L422-L475)).
7. **Crashes.** If a worker dies, its rows stay `in_progress` until their lease runs out. [`backup_and_reap.py --reap`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/backup_and_reap.py#L107-L142) then resets them to `pending`, or marks them `failure` (`last_error = 'lease_expired'`) if that was their last attempt ([`reap_expired_leases`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L464-L502)). The attempt is not given back.
8. **Retries.** `failure` rows with attempts left are claimed again only when a worker runs with `--retry-failures`, or through [`rerun_failed_batch.py`](rerun_failed_batch.py), which re-claims the failures of one batch into a new retry batch ([`claim_failed_from_batch`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L549-L590)).

Every worker process also writes a row to `runs` (host, pid, settings, final status) and events to `run_logs` (batch summaries, bot-check streaks, heartbeat failures, backoff sleeps) ([`create_run` / `log_run_event`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L329-L385)). [`utilities/get_run_report.py`](utilities/get_run_report.py) and [`get_status.py`](get_status.py) read them, and the terminal output of each worker is also copied to a log file in its run dir ([`terminal_logging.py`](terminal_logging.py)).

## Design decisions

**Postgres as the coordination store.** The `videos` table is the queue and the record at the same time: it holds the work order (dataset priority, row index), the claim state (worker, batch, lease), the retry state (attempts, blocks, last error) and the outcome (output file, log path, elapsed time). The state per video has to be stored and queried anyway, so one system is simpler to run than a message queue plus a database, and Postgres was already available. Progress and debugging are plain SQL queries, and workers need nothing besides a Postgres connection. Any reachable Postgres works; the code uses no extensions.

**Leases and lease extension.** A claim is a lease, not ownership: `lease_until` says how long the claim is valid (default 30 minutes). Leases are taken and extended per batch, with one `UPDATE` for all the batch's rows, to keep the number of round trips over the SSH tunnel low. A batch can therefore take much longer than one lease period without being reaped, and a dead worker's rows become reclaimable at the first reaper pass after their lease ends. `lease_seconds` and `max_attempts` are stored in the `meta` table when the database is created, and a worker started with different values refuses to start ([`ensure_db_ready`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L451-L461)), so all workers apply the same policy.

**Reaping is a separate process.** The reaper was added to reclaim videos lost by workers that died. `claim_videos` only picks `pending` rows, so an expired `in_progress` row is invisible to other workers until the reaper resets it. Keeping this in one separate process means there is one place that changes expired rows, and the claim query stays simple. The cost is that a reaper has to run somewhere.

**At-least-once processing.** `FOR UPDATE SKIP LOCKED` means two workers never hold the same row at the same time: concurrent claimers skip rows that another transaction has locked instead of waiting for them. The result write is guarded by `worker_id`, so a stale worker cannot overwrite a row that was reaped and re-claimed. Before downloading, a worker skips any video whose file is already in its own run dir. What the design does not prevent is a second download on another machine after a lease expires: if a worker is still downloading when its lease runs out (for example because the tunnel was down and the heartbeat failed), the reaper can hand the video to another worker, and both download it. Occasional duplicate files are acceptable for this dataset; deduplication, if needed, happens downstream.

**Retry policy.** `attempts` is incremented when a video is claimed, not when it fails, so a crash or an expired lease also uses an attempt. The default is `max_attempts = 3`. Errors are classified ([`scraper_utils.py#L48-L67`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/scraper_utils.py#L48-L67), [`#L725-L756`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/scraper_utils.py#L725-L756)):

- Permanent errors (private, removed, terminated account) are recorded as `skipped` and never retried.
- Format and other errors are retried once within the attempt with a second player client, then recorded as `failure`.
- Rate-limit errors trigger a cooldown (see below) and a retry, up to a per-preset limit, then count as `blocked`.
- Real failures are not retried by default. The goal is to get as many videos as possible, so time goes to videos that have not been tried yet rather than to ones that already failed. A worker picks failures up only with `--retry-failures`, or an operator retries one batch with `rerun_failed_batch.py`.
- A bot check is a temporary block on the machine's IP caused by the request pattern, not a problem with the video. The video goes back to `pending` with its attempt given back, and the block is counted in `blocked_count`, so it stays visible which videos ran into blocks. The worker's streak of consecutive blocks is what triggers the backoff. After 10 blocks a video is marked `failure`, so the probe loop cannot get stuck on one video.

**Bot-block backoff with probing.** Bot checks were not expected (the project was supposed to be whitelisted), so this is a simple workaround rather than a tuned mechanism. There are two parts.

- *Shared rate-limit cooldown.* When yt-dlp reports a rate limit (HTTP 429, "try again later"), the worker sets `global_cooldown_until_utc` in the `meta` table, taking the later of the existing and the new deadline under `SELECT ... FOR UPDATE` ([`extend_global_cooldown`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/distributed_core.py#L298-L327)). Every worker checks this key before each request and waits until it has passed ([`scraper_utils.py#L502-L584`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/scraper_utils.py#L502-L584)). If the database cannot be reached, the worker falls back to its local cooldown. Requests are also paced per process to a target rate set by the preset, with random jitter.
- *Per-worker sleep and probe.* After 20 consecutive bot checks in a batch, the worker stops scheduling from that batch, claims no new batches, waits until all active work has finished, and then loops: sleep, claim one video, try it ([`probe_until_clear`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/worker_common.py#L201-L279)). If the probe is blocked, the wait grows by 1.5x and it sleeps again. If the probe succeeds, the next base wait becomes 0.8x the wait that worked, and normal downloading resumes. A non-bot error on the probe is recorded and another video is tried at once. The factors 1.5x and 0.8x were picked by hand and have not been tested against data. The waits are written to `block_wait_state.json` in the run dir ([`load_block_state` / `save_block_state`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/worker_common.py#L127-L158)), so a worker restarted by the supervisor during a block continues from the wait it had reached instead of starting again at 15 minutes.

**Per-worker auth.** Each run dir has its own YouTube login. [`setup_worker.py`](setup_worker.py) opens a real browser through `undetected-chromedriver`, the operator logs in to a Google account, and the cookies are saved in Netscape format to `auth/youtube_cookies.txt` in the run dir ([`worker_auth.py`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/worker_auth.py#L98-L140)). Because the cookies live in the run dir, each machine can use a different account. That was the goal, but the deployment did not get there, and it is untested how fresh accounts would hold up. For every download attempt the cookie file is copied to a temporary directory and yt-dlp gets the copy, so yt-dlp never writes to the original file ([`scraper_utils.py#L609-L615`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/scraper_utils.py#L609-L615)). PO tokens come from the `bgutil-ytdlp-pot-provider` plugin in script mode, and yt-dlp's JavaScript challenges are solved with Deno (or Node, Bun or QuickJS) through `yt-dlp-ejs` ([`_build_ydl_opts`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/scraper_utils.py#L623-L663)). The worker checks all of this on startup and refuses to run with an incomplete profile ([`validate_worker_environment`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/scraper_utils.py#L247-L277)).

**Database connections.** Each worker process keeps a few long-lived connections: one for the main loop, one per active batch for the lease heartbeat, and one shared by the download threads for the cooldown key ([`DbSession`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/worker_common.py#L29-L68)). A connection that breaks is dropped and reopened on the next call. Nothing is retried automatically: a failed claim or write raises as before, the worker exits, and the supervisor restarts it.

**Process supervision.** [`supervise_workers.ps1`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/b216571610bbc3812adb3cb619af4d3f14c276bf/supervise_workers.ps1#L384-L427) starts the SSH tunnel, the downloader and optionally the reaper and archiver, polls them every 2 seconds, and restarts any that exited after a delay (default 10 s), with each child's stdout and stderr in its own log file. The tunnel runs with `ExitOnForwardFailure` and keepalives, so a dead connection ends the `ssh` process and the supervisor starts a new one. A worker that loses the database crashes and is restarted in the same way; its rows are recovered through lease expiry and the reaper.

## Known limitations

- **Test coverage stops at the downloader.** The tests cover claiming, reaping, the retry policy, bot-check detection and the full worker loop with a fake downloader. Real yt-dlp downloads, the rate-limit cooldown, the supervisor and the dashboard are not tested automatically.
- **A reaper must be running.** Nothing starts it automatically outside the supervisor's `StartBackupAndReap` setting. Without it, rows from a crashed worker stay `in_progress` forever. The reaper also always runs `pg_dump`; there is no reap-only mode.
- **Duplicates across machines.** See at-least-once processing above. The skip check only knows about files on the same machine.
- **Bot-check detection matches text.** It looks for the start of yt-dlp's "Sign in to confirm you're not a bot" message. If yt-dlp rewords that sentence, bot checks are recorded as ordinary failures and the backoff does not start.
- **`skipped` means two things:** the file was already on disk, or the video is permanently unavailable. `last_error` tells them apart.
- **Built for Windows.** The supervisor, launcher, default paths and browser discovery are written for Windows, and that is where it has been run. The Python worker uses portable APIs, and the tests run on Linux.
- **Single database.** One Postgres instance is the only shared state. If it is down, no worker can claim or record work. Backups are periodic `pg_dump` files (default: hourly, keep 3); there is no replication.
- **SSH tunnel dependency.** Because eduroam blocks direct connections to self-hosted databases, every worker reaches the database through its own SSH tunnel, and the UCloud SSH port changes with every database job, so each machine's config has to be updated when the database is restarted.
- **Claims sort all pending rows.** No index matches the claim order (dataset priority, row index), so every claim scans and sorts the pending part of the table.
- **Pacing is per process.** The request rate limit applies to each worker process separately; only the rate-limit cooldown is shared.
- **Run status can go stale.** If the database is unreachable when a worker exits, its `runs` row stays `running`.
- **Dashboard is a work in progress.** `dashboard.py` is not the supported workflow. Its HTTP API can start processes and has no login; it only accepts requests from the local machine.

## Quick start

The shortest path to one worker against any Postgres. For the full deployment (UCloud, SSH tunnel, supervisor, backups) see [docs/operations.md](docs/operations.md).

1. Install Python 3.10+, ffmpeg and [Deno](https://deno.com), then the Python packages:

   ```bash
   python -m pip install -r requirements.txt
   python -m pip install -U --pre "yt-dlp[default]"
   ```

2. Set up the script-mode server of [bgutil-ytdlp-pot-provider](https://github.com/Brainicism/bgutil-ytdlp-pot-provider) as described in that project. The worker refuses to start without the server directory.

3. Point the scripts at a database, either in a `.env` file in the repo root or with `--db-url`:

   ```text
   DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
   ```

4. Load IDs. `create_database.py` expects three Parquet files, each with an `id` column, in priority order. An empty file with an `id` column is fine for the ones you don't need:

   ```bash
   python create_database.py --ids-ok ids.parquet --ids-no-upload empty.parquet --ids-errors empty.parquet
   ```

5. Create the worker profile. This opens a browser for a YouTube login and saves the cookies in the run dir:

   ```bash
   python setup_worker.py --run-dir ./run_a --preset normal --bgutil-server-home /path/to/bgutil-ytdlp-pot-provider/server
   ```

6. Start the worker, and the reaper somewhere. The reaper needs `pg_dump`, because each pass also writes a backup (to `./db_backups` by default):

   ```bash
   python start_download.py --run-dir ./run_a --workers 4
   python backup_and_reap.py --reap --interval-minutes 15
   ```

   Videos land in `./run_a/batches/<batch_id>/videos/`. Check progress with `python get_status.py`.

### Running the tests

Most tests need a disposable Postgres and are skipped unless `TEST_DATABASE_URL` is set; the bot-check detection tests run without one. The tests never read `.env` or `DATABASE_URL`. Each database test creates its tables in a new, uniquely named schema and drops that schema afterwards.

```bash
python -m pip install -r requirements-dev.txt
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/yt_test python -m pytest tests -v
```

## Project layout

- `distributed_core.py`: schema, claiming, leases, reaping, result writes, shared cooldown and run logging.
- `scraper_utils.py`: yt-dlp download, error classification, request pacing, rate-limit cooldown and worker profile handling.
- `start_download.py`: primary downloader worker (batch claiming, thread pool, bot-check pause).
- `rerun_failed_batch.py`: retry failed videos from one batch.
- `worker_common.py`: shared by the two workers: database sessions, lease heartbeat, probe loop and backoff state.
- `create_database.py`: creates schema and loads IDs from Parquet files.
- `backup_and_reap.py`: lease reaper plus timestamped `pg_dump` backups.
- `get_status.py`: quick progress overview.
- `reset_database.py`: destructive reset of downloader tables.
- `setup_worker.py`: one-time worker auth/profile setup.
- `refresh_cookies.py`: refresh cookies for an existing worker profile.
- `worker_auth.py`: browser discovery and cookie capture used by the two scripts above.
- `supervise_workers.ps1`: keeps the SSH tunnel and selected workers running.
- `start_supervisor.bat`, `start_supervisor.ps1`: launcher that runs the supervisor from `supervise_workers.config.psd1`.
- `supervise_workers.config.example.psd1`: template for that config file.
- `start_archiver.py`: optional zip/archive worker (not required for downloading).
- `dashboard.py`: local dashboard (work in progress).
- `terminal_logging.py`: copies a script's terminal output to a log file.
- `env_utils.py`: loads `.env` from the repo root.
- `utilities/get_video_info.py`: inspect DB state for specific IDs.
- `utilities/get_run_report.py`: inspect run history and logs.
- `tests/`: claiming and reaping under concurrency, retry policy, bot-check detection, and end-to-end worker runs with a fake downloader.
- `docs/operations.md`: runbook for the current deployment.
- `requirements.txt`, `requirements-dev.txt`: runtime and test dependencies.
