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

Each worker machine runs its own supervisor, tunnel and downloader, and keeps all local state in its own run dir. The only shared state is the Postgres database. The reaper/backup process only needs to run in one place.

## How it works

### Life of a video

```mermaid
stateDiagram-v2
    [*] --> pending: create_database.py loads IDs
    pending --> in_progress: claimed in a batch (attempts + 1, lease set)
    in_progress --> success: downloaded
    in_progress --> skipped: already on disk, or permanently unavailable
    in_progress --> failure: download error
    in_progress --> pending: bot check (attempt is kept)
    in_progress --> pending: batch paused before it started (attempt refunded)
    in_progress --> pending: lease expired, reset by the reaper
    failure --> in_progress: re-claimed while attempts < max_attempts
```

1. **Loading.** [`create_database.py`](create_database.py) creates the schema and streams IDs from three Parquet files into the `videos` table as `pending`, each row tagged with a dataset priority and row index ([`load_dataset`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L367-L404), [`videos` schema](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L180-L206)).
2. **Claiming.** A worker takes the next batch (default 1000) in a single statement: a CTE selects `pending` rows with `attempts < max_attempts` in priority order with `FOR UPDATE SKIP LOCKED`, and the `UPDATE` sets `status = 'in_progress'`, `worker_id`, `batch_id`, `lease_until = now() + lease_seconds`, and increments `attempts` ([`claim_videos`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L452-L494)). The batch gets a row in `batches` and its own directory in the run dir ([`claim_new_batch`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/start_download.py#L437-L537)).
3. **Holding the lease.** While the batch is active, a background thread extends the lease for all of the batch's `in_progress` rows every half lease period (minimum 30 s) ([`start_lease_heartbeat`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/start_download.py#L202-L243), [`extend_batch_leases`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L716-L729)).
4. **Downloading.** IDs whose file already exists in the batch's `videos/` folder are marked `skipped` without a download ([`start_download.py#L500-L520`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/start_download.py#L500-L520)). The rest go to a thread pool. With `--overlap-batches 2` (the default) the next batch is claimed as soon as the current one has nothing left to schedule, so the pool does not drain between batches ([`maybe_claim_overlap_batch`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/start_download.py#L539-L575)). Each download is paced, tried with two player clients, and classified ([`download_video`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/scraper_utils.py#L658-L748)).
5. **Writing the result.** `success`, `failure` or `skipped` is written with the error text, elapsed time, output path and log path, and the lease is cleared. The `UPDATE` is guarded by `worker_id`, so a worker whose row was reaped and handed to someone else cannot overwrite it ([`update_video_result`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L689-L713)).
6. **Bot checks.** A result is `blocked` when the last line of the yt-dlp log is the "Sign in to confirm you're not a bot" error ([`is_bot_blocked_log`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L59-L84)), or when rate-limit retries run out. Blocked videos go back to `pending` and keep the attempt they used ([`release_blocked_video`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L653-L686)). After 20 consecutive blocks the batch is paused, its unscheduled videos go back to `pending` with their attempt refunded, and the worker enters the backoff loop described below ([`start_download.py#L577-L627`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/start_download.py#L577-L627), [`#L657-L711`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/start_download.py#L657-L711)).
7. **Crashes.** If a worker dies, its rows stay `in_progress` until their lease runs out. [`backup_and_reap.py --reap`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/backup_and_reap.py#L107-L139) then resets them to `pending` ([`reap_expired_leases`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L436-L449)). The attempt is not refunded.
8. **Retries.** `failure` rows with attempts left are claimed again only when a worker runs with `--retry-failures`, or through [`rerun_failed_batch.py`](rerun_failed_batch.py), which re-claims the failures of one batch into a new retry batch ([`claim_failed_from_batch`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L497-L539)).

Every worker process also writes a row to `runs` (host, pid, settings, final status) and events to `run_logs` (batch summaries, bot-check streaks, heartbeat failures, backoff sleeps) ([`create_run` / `log_run_event`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L301-L358)). [`utilities/get_run_report.py`](utilities/get_run_report.py) and [`get_status.py`](get_status.py) read them, and the terminal output of each worker is also copied to a log file in its run dir ([`terminal_logging.py`](terminal_logging.py)).

## Design decisions

**Postgres as the coordination store.** The `videos` table is the queue and the record at the same time: it holds the work order (dataset priority, row index), the claim state (worker, batch, lease), the retry state (attempts, last error) and the outcome (output file, log path, elapsed time). Progress and debugging are plain SQL queries, and workers need nothing besides a Postgres connection. Any reachable Postgres works; the code uses no extensions. <!-- CONFIRM: Postgres was chosen over a message queue (Redis, RabbitMQ, SQS) because the state per video has to be stored and queried anyway, so one system is simpler to run than a queue plus a database, and the team already had Postgres available. -->

**Leases and lease extension.** A claim is a lease, not ownership: `lease_until` says how long the claim is valid (default 30 minutes). Leases are taken and extended per batch, with one `UPDATE` for all the batch's rows, while the worker is alive. A batch can therefore take much longer than one lease period without being reaped, and a dead worker's rows become reclaimable at the first reaper pass after their lease ends. <!-- CONFIRM: leases are per batch rather than per video to keep the number of round trips over the SSH tunnel low. --> `lease_seconds` and `max_attempts` are stored in the `meta` table when the database is created, and a worker started with different values refuses to start ([`ensure_db_ready`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L423-L433)), so all workers apply the same policy.

**Reaping is a separate process.** `claim_videos` only picks `pending` rows, so an expired `in_progress` row is invisible to other workers until the reaper resets it. This keeps the claim query simple, but it means a reaper has to run somewhere. <!-- CONFIRM: the reaper is a separate, optional process (bundled with backups) rather than part of the claim query because it was simpler to reason about one place that changes expired rows. -->

**At-least-once processing.** `FOR UPDATE SKIP LOCKED` means two workers never hold the same row at the same time: concurrent claimers skip rows that another transaction has locked instead of waiting for them. The result write is guarded by `worker_id`, so a stale worker cannot overwrite a row that was reaped and re-claimed. What the design does not prevent is a second download after a lease expires: if a worker is still downloading when its lease runs out (for example because the tunnel was down and the heartbeat failed), the reaper can hand the video to another worker, and both download it. The file-exists check only looks in the current batch's own folder, so it does not catch this case. Duplicates are rare but possible, and each video's `output_file` in the database points at the copy that was recorded. <!-- CONFIRM: occasional duplicate files are acceptable for the dataset, and deduplication (if needed) happens downstream. -->

**Retry policy.** `attempts` is incremented when a video is claimed, not when it fails, so a crash or an expired lease also uses an attempt. The default is `max_attempts = 3`. Errors are classified ([`scraper_utils.py#L48-L67`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/scraper_utils.py#L48-L67), [`#L712-L743`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/scraper_utils.py#L712-L743)):

- Permanent errors (private, removed, terminated account) are recorded as `skipped` and never retried.
- Format and other errors are retried once within the attempt with a second player client, then recorded as `failure`.
- Rate-limit errors trigger a cooldown (see below) and a retry, up to a per-preset limit, then count as `blocked`.
- A bot check (`blocked`) keeps its attempt (commit [e3f9cbd](https://github.com/MarcusOlesen/YT_Download_7mil/commit/e3f9cbdf3ff1fa4fe68098b8cd249508ad601350)). <!-- CONFIRM: blocked videos keep their attempt so that a video that always triggers the bot check cannot be retried forever. --> Videos that were released because their batch was paused before they started get their attempt back.
- `failure` rows are not retried automatically. A worker picks them up only with `--retry-failures`, or an operator retries one batch with `rerun_failed_batch.py`. <!-- CONFIRM: failures are not retried by default so that the first pass over the whole list finishes before time is spent on videos that already failed. -->

**Bot-block backoff with probing.** There are two separate mechanisms.

- *Shared rate-limit cooldown.* When yt-dlp reports a rate limit (HTTP 429, "try again later"), the worker sets `global_cooldown_until_utc` in the `meta` table, taking the later of the existing and the new deadline under `SELECT ... FOR UPDATE` ([`extend_global_cooldown`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/distributed_core.py#L270-L299)). Every worker checks this key before each request and waits until it has passed ([`scraper_utils.py#L487-L569`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/scraper_utils.py#L487-L569)). If the database cannot be reached, the worker falls back to its local cooldown. Requests are also paced per process to a target rate set by the preset, with random jitter.
- *Per-worker sleep and probe.* After 20 consecutive bot checks in a batch, the worker stops scheduling from that batch, claims no new batches, waits until all active work has finished, and then loops: sleep, claim one video, try it ([`probe_until_clear`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/start_download.py#L246-L343)). If the probe is blocked, the wait grows by 1.5x and it sleeps again. If the probe succeeds, the next base wait becomes 0.8x the wait that worked, and normal downloading resumes. A non-bot error on the probe is recorded and another video is tried at once. The waits are written to `block_wait_state.json` in the run dir ([`load_block_state` / `save_block_state`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/start_download.py#L168-L199)), so a restarted worker continues from the wait it had learned instead of starting again at 15 minutes. <!-- CONFIRM: the state is persisted so that supervisor restarts during a block do not reset the backoff; growth by 1.5x and shrinking by 0.8x were picked by hand, not tuned from data. -->

**Per-worker auth.** Each run dir has its own YouTube login. [`setup_worker.py`](setup_worker.py) opens a real browser through `undetected-chromedriver`, the operator logs in to a machine-specific Google account, and the cookies are saved in Netscape format to `auth/youtube_cookies.txt` in the run dir ([`worker_auth.py`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/worker_auth.py#L98-L140)). For every download attempt the cookie file is copied to a temporary directory and the copy is passed to yt-dlp ([`scraper_utils.py#L594-L600`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/scraper_utils.py#L594-L600)). <!-- CONFIRM: the per-attempt copy exists because yt-dlp writes cookies back to the file, and concurrent threads writing the same file corrupted it. --> PO tokens come from the `bgutil-ytdlp-pot-provider` plugin in script mode, and yt-dlp's JavaScript challenges are solved with Deno through `yt-dlp-ejs` ([`_build_ydl_opts`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/scraper_utils.py#L608-L650)). The worker checks all of this on startup and refuses to run with an incomplete profile ([`validate_worker_environment`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/scraper_utils.py#L236-L266)). <!-- CONFIRM: one account per machine spreads the request volume, so a block on one account does not stop the other machines. -->

**Process supervision.** [`supervise_workers.ps1`](https://github.com/MarcusOlesen/YT_Download_7mil/blob/3e7ec3931abf2d59d244ab55454ad003d82510c2/supervise_workers.ps1#L384-L427) starts the SSH tunnel, the downloader and optionally the reaper and archiver, polls them every 2 seconds, and restarts any that exited after a delay (default 10 s), with each child's stdout and stderr in its own log file. The tunnel runs with `ExitOnForwardFailure` and keepalives, so a dead connection ends the `ssh` process and the supervisor starts a new one. A worker that loses the database crashes and is restarted in the same way; its rows are recovered through lease expiry and the reaper.

## Known limitations

- **Little test coverage.** The only automated test is [`tests/test_claiming.py`](tests/test_claiming.py), which covers claiming and reaping against a real Postgres. The worker loop, the backoff and the download classification are not tested.
- **A reaper must be running.** It is a separate process that nothing starts automatically. Without a running `backup_and_reap.py --reap`, rows from a crashed worker stay `in_progress` forever. The reaper also always runs `pg_dump`; there is no reap-only mode.
- **Rows can get stuck at `max_attempts`.** The reaper and the bot-check release both return rows to `pending` without refunding the attempt. A row that reaches `max_attempts` this way stays `pending`, can no longer be claimed, and is never marked `failure`.
- **Narrow on-disk check.** The "already exists" check only looks in the new batch's own folder. Batch IDs come from a counter that only goes up, so in normal operation the folder is empty and the check rarely applies.
- **Bot-check detection is an exact string match** on the last line of the yt-dlp log. If yt-dlp changes the wording, bot checks are recorded as ordinary failures and the backoff does not start.
- **`skipped` means two things:** the file was already on disk, or the video is permanently unavailable. `last_error` tells them apart.
- **Windows-centric.** The supervisor, launcher, default paths and browser discovery are written for Windows. The Python worker uses portable APIs, but other platforms have not been tested. <!-- CONFIRM: the worker has only been run on Windows. -->
- **Single database.** One Postgres instance is the only shared state. If it is down, no worker can claim or record work. Backups are periodic `pg_dump` files (default: hourly, keep 3); there is no replication.
- **SSH tunnel dependency.** In the current deployment every worker reaches the database through its own SSH tunnel, and the UCloud SSH port changes with every database job, so each machine's config has to be updated when the database is restarted.
- **Connection per operation.** Every database call opens a new connection (at least one per video), with no pooling.
- **Pacing is per process.** The request rate limit applies to each worker process separately; only the rate-limit cooldown is shared.
- **Dashboard is a work in progress.** `dashboard.py` is not the supported workflow, and its HTTP API (bound to `127.0.0.1`) can start processes without any authentication.
- **Duplicated code.** `rerun_failed_batch.py` carries its own copy of the heartbeat, block-state and probe code from `start_download.py`.
- `get_status.py` hardcodes the dataset totals instead of reading the `datasets` table.

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

The test needs a disposable Postgres and is skipped unless `TEST_DATABASE_URL` is set. It never reads `.env` or `DATABASE_URL`. It creates its tables in a new, uniquely named schema and drops that schema afterwards.

```bash
python -m pip install -r requirements-dev.txt
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/yt_test python -m pytest tests -v
```

## Project layout

- `distributed_core.py`: schema, claiming, leases, reaping, result writes, shared cooldown and run logging.
- `scraper_utils.py`: yt-dlp download, error classification, request pacing, rate-limit cooldown and worker profile handling.
- `start_download.py`: primary downloader worker (batch claiming, thread pool, lease heartbeat, bot-check backoff).
- `rerun_failed_batch.py`: retry failed videos from one batch.
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
- `tests/test_claiming.py`: concurrency test for claiming and reaping.
- `docs/operations.md`: runbook for the current deployment.
- `requirements.txt`, `requirements-dev.txt`: runtime and test dependencies.
