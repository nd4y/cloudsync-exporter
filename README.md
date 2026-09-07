# cloudsync-exporter

🇬🇧 **English** | [🇷🇺 Русский](README.ru.md)

Prometheus exporter for **Synology Cloud Sync** (DSM 7, Cloud Sync 2.7.x).

Cloud Sync has no metrics endpoint and no SNMP branch; the only view is the DSM
UI. The daemon (`syno-cloud-syncd`) does, however, keep all of its state in
SQLite databases under its repository directory, and writes a verbose log. This
exporter reads both **read-only** and exposes:

- what is configured (connections, tasks, directions, encryption, limits);
- how big each task is (files, directories, local/remote bytes) and how much
  is still not on the cloud side;
- what is queued right now: pending remote events, unfinished transfers,
  resumable uploads, queued directory scans;
- what happened recently: uploads/downloads/removals per task, errors by code
  (rotating history table turned into counters);
- **whether the cloud is throttling you** and for how long, API errors by
  reason (`userRateLimitExceeded`, `rateLimitExceeded`, `authError`, ...),
  per-worker outcomes, event pipeline stages (detected → processing → done);
- optionally, the exact status the UI shows (`uptodate`/`syncing`/`pause`/...)
  and the *"Processing N files"* counter, via the DSM Web API.

Pure Python standard library, a single file, runs on `python:3.12-alpine`.

## How it works

```
/volumeN/@cloudsync/                     (repo_path in config/daemon.conf)
├── db/config.sqlite          connections + sessions (tasks)
├── db/history.sqlite         last 500 events per connection (rotating)
├── db/resume-info-db.sqlite  interrupted transfers with resume info
├── connection/<id>/server-db.sqlite   remote listing, pending remote events,
│                                      unfinished transfers
└── session/<id>/event-db.sqlite       local index of the synced folder

/var/packages/CloudSync/var/log/synocloudsync.log   (root only)
```

Databases are opened with SQLite `mode=ro`. They run in WAL mode; a read-only
bind mount is fine (SQLite falls back to a heap wal-index). If a read still
fails with an I/O error the exporter re-reads the main file with `immutable=1`
and reports it in `cloudsync_db_immutable_fallback_reads_total`. Temporary
b-trees are kept in memory (`PRAGMA temp_store=MEMORY`): with a read-only root
filesystem and no tmpfs a `GROUP BY` over a large index would otherwise fail
with the very same "disk I/O error". Database
metrics are cached for `REFRESH_INTERVAL` seconds regardless of how often
Prometheus scrapes; the log is tailed continuously and survives rotation.

The daemon log is readable only by root on DSM. The reference compose keeps the
container as root but drops every capability except `DAC_READ_SEARCH` (read
files you do not own) on a read-only root filesystem. Without the log mount the
exporter simply skips the log metrics.

## Metrics

Labels: `conn_id`/`task_name` identify a connection (cloud account), `sess_id`/
`conn_id`/`share` identify a task (one synced folder).

### Configuration (`db/config.sqlite`)

| Metric | Description |
|---|---|
| `cloudsync_connection_info{client_type,local_user,remote_user,remote_root}` | 1 per connection |
| `cloudsync_connection_status`, `_error`, `_last_sync_status` | raw integers from `connection_table` |
| `cloudsync_connection_pull_event_period_seconds` | how often remote changes are polled |
| `cloudsync_connection_max_{upload,download}_speed_kbytes` | configured per-file limits (0 = none) |
| `cloudsync_connection_schedule_enabled` | 1 if a schedule restricts syncing |
| `cloudsync_session_info{local_path,remote_path,sync_direction,encrypted,priority}` | 1 per task |
| `cloudsync_session_status`, `_error` | raw integers from `session_table` |
| `cloudsync_session_created_timestamp_seconds`, `_remote_file_count` | |
| `cloudsync_sessions_removed` | tasks still in the DB but marked removed (they are skipped) |

Observed on Cloud Sync 2.7.2: `client_type` 1 = Google Drive, 11 = WebDAV;
`sync_direction` 2 = "Upload local changes only"; `status` 1 = active,
2 = removed task.

### Local index (`session/<id>/event-db.sqlite`)

| Metric | Description |
|---|---|
| `cloudsync_session_files`, `_directories` | entries in the index |
| `cloudsync_session_deleted_entries` | entries whose local file is gone |
| `cloudsync_session_local_bytes`, `_remote_bytes` | remote is larger with client-side encryption |
| `cloudsync_session_files_without_remote_id`, `_bytes_without_remote_id` | files with no cloud file id — **for ID-based clouds (Google Drive, OneDrive, Dropbox, Box) this is the not-yet-uploaded backlog**; for WebDAV/S3 the field is always empty, ignore it there |
| `cloudsync_session_pending_scans` | directory scans queued |
| `cloudsync_session_recycle_bin_entries` | |
| `cloudsync_session_last_index_change_timestamp_seconds` | last time the daemon touched the index |
| `cloudsync_session_index_readable`, `cloudsync_session_event_db_bytes` | |

### Cloud side (`connection/<id>/server-db.sqlite`)

| Metric | Description |
|---|---|
| `cloudsync_connection_remote_files`, `_remote_directories`, `_remote_bytes` | what the daemon knows about the cloud folder |
| `cloudsync_remote_pending_events{sess_id,mode}`, `cloudsync_remote_pending_bytes` | remote change events fetched but not yet reconciled (`mode` 2 = file, 3 = directory) |
| `cloudsync_unfinished_events{sess_id}`, `cloudsync_unfinished_bytes` | transfers started but not finished |
| `cloudsync_remote_pending_raw_events` | raw change notifications not yet expanded |
| `cloudsync_resumable_transfers{sess_id}`, `cloudsync_resumable_transfer_retries` | from `db/resume-info-db.sqlite` |

### History (`db/history.sqlite`)

The daemon keeps the last 500 events per connection. The exporter remembers the
highest row id it has seen and counts new rows, so the counters are exact as
long as fewer than 500 events per connection arrive between two refreshes
(`cloudsync_history_dropped_rows_total` tells you if that happened).

| Metric | Description |
|---|---|
| `cloudsync_history_events_total{action}` | counter since exporter start; `action` = upload, download, remove_remote, remove_local, rename_*, merge, ... |
| `cloudsync_history_errors_total{action,error,code}` | events with a non-zero error code (`error` decoded: request_throttled, quota, auth_token_expired, ...) |
| `cloudsync_history_last_event_timestamp_seconds{action}` | last event of that kind still in the table (gauge, available immediately) |
| `cloudsync_history_recent_events{action}` | rows of that kind still in the table |
| `cloudsync_history_rows`, `cloudsync_history_dropped_rows_total` | |

### Daemon log (optional, `CLOUDSYNC_LOG_DIR`)

| Metric | Description |
|---|---|
| `cloudsync_connection_throttled{conn_id}` | 1 while the daemon logs `connection N is under throttling` (window `THROTTLE_WINDOW`) |
| `cloudsync_connection_throttle_remaining_seconds{conn_id}` | back-off still to wait (last logged value) |
| `cloudsync_throttle_waits_total{conn_id}`, `cloudsync_throttle_wait_seconds_total` | back-offs imposed and their total length |
| `cloudsync_api_errors_total{reason}` | `userRateLimitExceeded`, `rateLimitExceeded`, `authError`, ... |
| `cloudsync_worker_results_total{result}` | `Successful`, `Request throttled`, `Auth token expired`, ... |
| `cloudsync_uploads_started_total{sess_id}`, `cloudsync_upload_failures_total{sess_id,code,error}` | |
| `cloudsync_events_total{stage,type,sess_id}` | `stage` pushed (detected locally) → processing → done; `type` EV_ADD, EV_MODIFY, ... |
| `cloudsync_resume_session_responses_total{code}` | HTTP codes when resuming interrupted uploads |
| `cloudsync_log_available`, `_lines_total{level}`, `_last_timestamp_seconds`, `_file_bytes`, `_rotations_total`, `_continuation_lines_total` | tailer health; `last_timestamp` doubles as daemon liveness |

Per-worker lines carry no connection id, so the exporter maps workers to
connections/tasks from the throttling and `current event` lines (the last
512 KB of the log are pre-read for that on start). Until a worker has been
seen, its counters land in `conn_id="unknown"`.

### DSM Web API (optional, `DSM_URL`)

Requires a DSM account allowed to use Cloud Sync (the API accepts non-admin
users, but they only see their own connections). The exporter logs in with
`SYNO.API.Auth` and polls `SYNO.CloudSync` `list_conn` + `list_sess`.

| Metric | Description |
|---|---|
| `cloudsync_dsm_connection_state{state}` | state set: `uptodate`, `syncing`, `processing`, `scanning`, `connecting`, `pause`, `suspended`, `error`, `unlink` — exactly one is 1 |
| `cloudsync_dsm_connection_unfinished_files` | the UI's *"Processing N file(s)..."* |
| `cloudsync_dsm_connection_info{link_status,last_sync_status,error_type}` | |
| `cloudsync_dsm_connection_error_code`, `_session_errors`, `_next_sync_timestamp_seconds` | |
| `cloudsync_dsm_session_state{state}`, `cloudsync_dsm_session_error_code` | per task |
| `cloudsync_dsm_up`, `_requests_total`, `_poll_failures_total`, `_last_success_timestamp_seconds` | |

This source has been implemented against the DSM UI's own calls but not yet
verified against a live DSM with credentials — run with `DEBUG=1` to log the
raw responses if something looks off.

### Exporter

`cloudsync_repo_readable`, `cloudsync_collect_duration_seconds`,
`cloudsync_db_read_errors_total{db}`, `cloudsync_db_immutable_fallback_reads_total{db}`,
`cloudsync_exporter_scrapes_total`, `cloudsync_exporter_cache_age_seconds`,
`cloudsync_*_db_bytes`.

## Configuration (env)

| Variable | Default | |
|---|---|---|
| `CLOUDSYNC_REPO` | `/cloudsync` | mount of the repository (`repo_path` in `daemon.conf`, e.g. `/volume2/@cloudsync`) |
| `CLOUDSYNC_LOG_DIR` | *(off)* | mount of `/var/packages/CloudSync/var/log` |
| `CLOUDSYNC_LOG_FILE` | `synocloudsync.log` | |
| `LISTEN_PORT` | `9840` | |
| `REFRESH_INTERVAL` | `30` | seconds between database re-reads |
| `LOG_POLL_INTERVAL` | `5` | seconds between log polls |
| `THROTTLE_WINDOW` | `60` | seconds after the last throttling line the connection still counts as throttled |
| `DSM_URL` | *(off)* | e.g. `https://192.168.0.10:5001` |
| `DSM_USER`, `DSM_PASS` / `DSM_PASS_FILE` | | |
| `DSM_VERIFY_TLS` | `true` | set `false` for the self-signed DSM certificate |
| `DSM_INTERVAL`, `DSM_TIMEOUT` | `30`, `15` | |
| `DEBUG` | | log raw DSM responses and fallbacks |

## Running

```bash
docker run -d --name cloudsync-exporter \
  -v /volume2/@cloudsync:/cloudsync:ro \
  -v /volume1/@appdata/CloudSync/log:/cloudsync-log:ro \
  -e CLOUDSYNC_LOG_DIR=/cloudsync-log \
  --read-only --cap-drop ALL --cap-add DAC_READ_SEARCH \
  -p 9840:9840 ghcr.io/nd4y/cloudsync-exporter:latest
```

Or `docker compose up -d` with the included `docker-compose.yml`. Find your
repository volume with `cat /var/packages/CloudSync/target/etc/daemon.conf`
or `ls -d /volume*/@cloudsync`.

`/healthz` is a cheap liveness endpoint (no database access) — point container
healthchecks there.

## Useful queries

```promql
# files still to upload (Google Drive / OneDrive / Dropbox tasks)
cloudsync_session_files_without_remote_id

# what the UI shows as "Processing N files" (needs DSM_URL)
cloudsync_dsm_connection_unfinished_files

# uploads per hour
increase(cloudsync_history_events_total{action="upload"}[1h])

# share of the last hour spent in Google back-off
increase(cloudsync_throttle_wait_seconds_total[1h]) / 3600

# throttled right now
cloudsync_connection_throttled == 1
```

## Limitations

- The daemon's in-memory queue is not persisted anywhere readable; the exact
  *"N files to process"* figure exists only in the DSM Web API (`DSM_URL`).
  The database-derived queue gauges (`files_without_remote_id`,
  `remote_pending_events`, `unfinished_events`, `resumable_transfers`) are the
  closest persisted equivalents.
- With client-side encryption on, remote sizes include the encryption
  overhead, so local and remote byte totals never match exactly.
- Semantics of some raw integers (`status`, `last_sync_status`,
  `sync_direction`, `client_type`) are documented only as observed on
  Cloud Sync 2.7.2; they are exposed raw rather than guessed.
