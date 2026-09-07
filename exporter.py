#!/usr/bin/env python3
"""Prometheus exporter for Synology Cloud Sync.

Cloud Sync (DSM package `CloudSync`, daemon `syno-cloud-syncd`) keeps all of its
state in SQLite databases under its repository directory (default
`/volumeN/@cloudsync`):

    db/config.sqlite               connections and sessions (tasks)
    db/history.sqlite              last N transfer/log events (rotating table)
    db/resume-info-db.sqlite       resumable transfers in progress
    connection/<conn>/server-db.sqlite   remote listing + pending remote events
    session/<sess>/event-db.sqlite       local index of the synced folder

The exporter opens these read-only (SQLite `mode=ro`; the daemon's WAL files
stay untouched) and turns them into gauges/counters.  Two optional sources add
what the databases cannot tell:

* the daemon log (`/var/packages/CloudSync/var/log/synocloudsync.log`, root
  only) - throttling state, API errors, per-event outcomes;
* the DSM Web API (`SYNO.CloudSync`, needs a DSM account) - the same status the
  Cloud Sync UI shows: `uptodate`/`syncing`/`pause`/... and the number of
  files still to be processed (`unfinished_files`).

Stdlib only.  Metrics are cached for REFRESH_INTERVAL seconds so a busy
Prometheus cannot hammer 100+ MB databases.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import sqlite3
import ssl
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO = os.environ.get("CLOUDSYNC_REPO", "/cloudsync").rstrip("/")
LOG_DIR = os.environ.get("CLOUDSYNC_LOG_DIR", "").rstrip("/")
LOG_FILE = os.environ.get("CLOUDSYNC_LOG_FILE", "synocloudsync.log")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "9840"))
REFRESH_INTERVAL = float(os.environ.get("REFRESH_INTERVAL", "30"))
LOG_POLL_INTERVAL = float(os.environ.get("LOG_POLL_INTERVAL", "5"))
THROTTLE_WINDOW = float(os.environ.get("THROTTLE_WINDOW", "60"))
DSM_URL = os.environ.get("DSM_URL", "").rstrip("/")
DSM_USER = os.environ.get("DSM_USER", "")
DSM_PASS = os.environ.get("DSM_PASS", "")
DSM_PASS_FILE = os.environ.get("DSM_PASS_FILE", "")
DSM_VERIFY_TLS = os.environ.get("DSM_VERIFY_TLS", "true").lower() not in ("0", "false", "no")
DSM_INTERVAL = float(os.environ.get("DSM_INTERVAL", "30"))
DSM_TIMEOUT = float(os.environ.get("DSM_TIMEOUT", "15"))
DEBUG = os.environ.get("DEBUG", "").lower() in ("1", "true", "yes")

# history_table.action -> name (SYNO.SDS.DSCloudSync.FA_* in the DSM UI bundle)
HISTORY_ACTIONS = {
    0: "remove_remote",
    1: "download",
    2: "upload",
    3: "remove_local",
    4: "rename_remote",
    5: "rename_local",
    6: "err_upload_remote_file",
    7: "err_download_remote_file",
    8: "merge",
    9: "merge_deletion",
    10: "async_remove",
}

# history_table.error_code / daemon error codes -> name (SYNO.SDS.DSCloudSync.LOG_ERR_*)
ERROR_CODES = {
    0: "ok", -1: "int", -2: "io", -3: "sys", -4: "inval", -5: "proto", -6: "auth",
    -7: "auth_token_expired", -8: "server", -9: "conflict", -10: "timeout", -11: "quota",
    -12: "syncfolder_miss", -13: "permiss", -14: "local_diskfull", -15: "ssl_verify_fail",
    -16: "remote_file", -17: "remote_quota", -18: "resume_failed", -19: "server_proxy",
    -20: "parent_folder_missing", -21: "request_throttled", -22: "operation_not_supported",
    -23: "file_name_exists", -24: "remote_file_not_found", -25: "share_unmount",
    -26: "need_re_sync", -27: "user_delete", -28: "user_expired", -29: "user_disable",
    -30: "app_privilege", -31: "cycle_found", -32: "local_time_skewed", -33: "hierarchy_too_deep",
    -34: "file_limit_exceeded", -35: "remote_file_locked", -36: "remote_file_not_support",
    -37: "upload_file_too_large", -38: "bad_request", -39: "file_no_permission",
    -40: "dirsvs_offline", -41: "content_restricted", -42: "remote_changed",
    -43: "baidu_app_quota_full", -44: "dropbox_request_limit", -46: "cloud_drive_not_support",
    -47: "file_multi_parent", -48: "sfr_not_support", -49: "file_abusive",
    -50: "remote_blob_archived", -51: "remote_file_under_shortcut",
    -52: "dropbox_team_no_permission", -53: "baidu_app_id_empty",
    -54: "file_path_illegal_character", -55: "dropbox_teamspace_root_no_permission",
    -56: "ok_async_remove", -57: "dropbox_server_timeout",
}

# Connection states the Cloud Sync UI knows about (DSM Web API `list_conn` -> status).
DSM_STATES = ("uptodate", "syncing", "processing", "scanning", "connecting",
              "pause", "suspended", "error", "unlink")

MAX_LABEL_VALUES = 64  # cardinality guard for free-text labels harvested from the log


def log(msg):
    print(f"{datetime.now().isoformat(timespec='seconds')} {msg}", file=sys.stderr, flush=True)


def debug(msg):
    if DEBUG:
        log(msg)


def esc(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class Sample:
    """Accumulates exposition-format lines grouped per metric family."""

    def __init__(self):
        self.families = {}  # name -> (type, help, [lines])

    def add(self, name, mtype, help_text, labels, value):
        fam = self.families.setdefault(name, (mtype, help_text, []))
        if labels:
            label_str = "{" + ",".join(f'{k}="{esc(v)}"' for k, v in labels.items()) + "}"
        else:
            label_str = ""
        if isinstance(value, float) and value == int(value):
            value = int(value)
        fam[2].append(f"{name}{label_str} {value}")

    def merge(self, other):
        for name, (mtype, help_text, lines) in other.families.items():
            fam = self.families.setdefault(name, (mtype, help_text, []))
            fam[2].extend(lines)

    def render(self):
        out = []
        for name, (mtype, help_text, lines) in self.families.items():
            out.append(f"# HELP {name} {help_text}")
            out.append(f"# TYPE {name} {mtype}")
            out.extend(lines)
        return "\n".join(out) + "\n"


# ---------------------------------------------------------------- SQLite ---

def open_ro(path, immutable=False):
    """Open a database strictly read-only.

    The daemon runs the databases in WAL mode; with `mode=ro` SQLite still
    needs the -wal/-shm files to exist (they always do while the daemon is
    running) and reads them without writing anything.  `timeout` covers the
    occasional SQLITE_BUSY while the daemon commits.
    """
    uri = "file:" + urllib.parse.quote(path) + "?mode=ro"
    if immutable:
        uri += "&immutable=1"
    conn = sqlite3.connect(uri, uri=True, timeout=5, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def db_size(path):
    total = 0
    for suffix in ("", "-wal"):
        with contextlib.suppress(OSError):
            total += os.path.getsize(path + suffix)
    return total


def table_exists(conn, name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return row is not None


class HistoryState:
    """Turns the rotating history_table into monotonically growing counters.

    Rows are only ever appended (autoincrement id), old ones are trimmed, so
    counting rows with id > last_seen_id is exact as long as fewer rows than
    the table keeps (rotate_count, 500-1000) arrive between two refreshes.
    Counters start at zero when the exporter starts; rate()/increase() handle
    the reset like any other counter restart.
    """

    def __init__(self):
        self.last_id = None
        self.events = {}   # (conn, sess, action) -> count
        self.errors = {}   # (conn, sess, action, error_code) -> count
        self.dropped_rows = 0

    def update(self, conn):
        if self.last_id is None:
            row = conn.execute("SELECT MAX(id) AS m FROM history_table").fetchone()
            self.last_id = row["m"] or 0
            return
        rows = conn.execute(
            "SELECT id, conn_id, sess_id, action, log_level, error_code FROM history_table "
            "WHERE id > ? ORDER BY id", (self.last_id,)).fetchall()
        if not rows:
            return
        # If the oldest surviving row is already past last_id + 1 the table
        # rotated faster than we poll and some rows are gone for good.
        oldest = conn.execute("SELECT MIN(id) AS n FROM history_table").fetchone()["n"]
        if oldest is not None and oldest > self.last_id + 1:
            self.dropped_rows += oldest - self.last_id - 1
        for r in rows:
            key = (r["conn_id"], r["sess_id"], r["action"])
            self.events[key] = self.events.get(key, 0) + 1
            if r["log_level"] != 0 or r["error_code"] != 0:
                ekey = key + (r["error_code"],)
                self.errors[ekey] = self.errors.get(ekey, 0) + 1
            self.last_id = r["id"]


class DbCollector:
    def __init__(self, repo):
        self.repo = repo
        self.history = HistoryState()
        self.read_errors = {}  # db name -> count
        self.fallbacks = {}    # db name -> count of immutable-snapshot reads

    # -- helpers -----------------------------------------------------------

    def _fail(self, name, exc):
        self.read_errors[name] = self.read_errors.get(name, 0) + 1
        log(f"{name}: {exc}")

    def _query(self, path, name, fn):
        """Run fn(conn) against a database; returns None if unreadable."""
        if not os.path.exists(path):
            return None
        try:
            return self._run(path, fn, immutable=False)
        except sqlite3.Error as e:
            # A WAL database whose -shm we cannot write (read-only bind mount) is
            # normally fine, but a torn wal-index shows up as "disk I/O error".
            # Fall back to reading the main file alone: slightly stale (up to the
            # last checkpoint) but better than no data.
            if "I/O" not in str(e) and "unable to open" not in str(e):
                self._fail(name, e)
                return None
            try:
                res = self._run(path, fn, immutable=True)
            except sqlite3.Error as e2:
                self._fail(name, f"{e}; immutable fallback: {e2}")
                return None
            self.fallbacks[name] = self.fallbacks.get(name, 0) + 1
            debug(f"{name}: {e}; served from immutable snapshot")
            return res

    @staticmethod
    def _run(path, fn, immutable):
        conn = open_ro(path, immutable=immutable)
        try:
            return fn(conn)
        finally:
            conn.close()

    # -- collection --------------------------------------------------------

    def collect(self):
        s = Sample()
        start = time.monotonic()
        repo_ok = os.path.isdir(os.path.join(self.repo, "db"))
        s.add("cloudsync_repo_readable", "gauge",
              "1 if the Cloud Sync repository directory (db/, session/, connection/) is readable",
              {}, 1 if repo_ok else 0)
        if repo_ok:
            connections, sessions = self._collect_config(s)
            self._collect_sessions(s, sessions)
            self._collect_connections(s, connections, sessions)
            self._collect_resume(s, sessions)
            self._collect_history(s)
        for name, n in sorted(self.read_errors.items()):
            s.add("cloudsync_db_read_errors_total", "counter",
                  "SQLite open/query failures per database", {"db": name}, n)
        for name, n in sorted(self.fallbacks.items()):
            s.add("cloudsync_db_immutable_fallback_reads_total", "counter",
                  "Reads served from the main database file alone because the WAL index "
                  "was unreadable (data may lag behind by one checkpoint)", {"db": name}, n)
        s.add("cloudsync_collect_duration_seconds", "gauge",
              "Time spent reading the Cloud Sync databases", {},
              round(time.monotonic() - start, 3))
        return s

    def _collect_config(self, s):
        path = os.path.join(self.repo, "db", "config.sqlite")

        def fn(conn):
            conns = conn.execute(
                "SELECT id, task_name, client_type, local_user_name, user_name, status, error, "
                "last_sync_status, max_upload_speed, max_download_speed, pull_event_period, "
                "is_enabled_schedule, root_folder_path FROM connection_table ORDER BY id"
            ).fetchall()
            sess = conn.execute(
                "SELECT id, conn_id, share_name, sync_folder, server_folder_path, "
                "enable_server_encryption, status, error, create_time, removed_time, "
                "sync_direction, remote_file_count, priority FROM session_table ORDER BY id"
            ).fetchall()
            return [dict(r) for r in conns], [dict(r) for r in sess]

        res = self._query(path, "config", fn)
        s.add("cloudsync_config_db_bytes", "gauge", "Size of db/config.sqlite (+wal)", {},
              db_size(path))
        if res is None:
            return [], []
        connections, sessions = res
        for c in connections:
            lbl = {"conn_id": c["id"], "task_name": c["task_name"]}
            s.add("cloudsync_connection_info", "gauge",
                  "Connection (cloud account) as configured; value is always 1",
                  {**lbl, "client_type": c["client_type"], "local_user": c["local_user_name"],
                   "remote_user": c["user_name"], "remote_root": c["root_folder_path"]}, 1)
            s.add("cloudsync_connection_status", "gauge",
                  "connection_table.status raw value (1 observed for a live connection)",
                  lbl, c["status"])
            s.add("cloudsync_connection_error", "gauge",
                  "connection_table.error raw value (0 = no error)", lbl, c["error"])
            s.add("cloudsync_connection_last_sync_status", "gauge",
                  "connection_table.last_sync_status raw value", lbl, c["last_sync_status"])
            s.add("cloudsync_connection_pull_event_period_seconds", "gauge",
                  "How often the daemon polls the cloud for remote changes", lbl,
                  c["pull_event_period"])
            s.add("cloudsync_connection_max_upload_speed_kbytes", "gauge",
                  "Configured per-file upload limit in KB/s (0 = unlimited)", lbl,
                  c["max_upload_speed"])
            s.add("cloudsync_connection_max_download_speed_kbytes", "gauge",
                  "Configured per-file download limit in KB/s (0 = unlimited)", lbl,
                  c["max_download_speed"])
            s.add("cloudsync_connection_schedule_enabled", "gauge",
                  "1 if a sync schedule restricts when this connection syncs", lbl,
                  c["is_enabled_schedule"])
        removed = 0
        for t in sessions:
            if t["removed_time"]:
                removed += 1
                continue
            lbl = self.sess_labels(t)
            local = "/" + t["share_name"].strip("/")
            if t["sync_folder"] and t["sync_folder"] != "/":
                local += "/" + t["sync_folder"].strip("/")
            s.add("cloudsync_session_info", "gauge",
                  "Sync task (session) as configured; value is always 1",
                  {**lbl, "local_path": local, "remote_path": t["server_folder_path"],
                   "sync_direction": t["sync_direction"],
                   "encrypted": t["enable_server_encryption"], "priority": t["priority"]}, 1)
            s.add("cloudsync_session_status", "gauge",
                  "session_table.status raw value (1 observed for an active task)", lbl,
                  t["status"])
            s.add("cloudsync_session_error", "gauge",
                  "session_table.error raw value (0 = no error)", lbl, t["error"])
            s.add("cloudsync_session_created_timestamp_seconds", "gauge",
                  "When the task was created", lbl, t["create_time"] or 0)
            s.add("cloudsync_session_remote_file_count", "gauge",
                  "session_table.remote_file_count as recorded by the daemon", lbl,
                  t["remote_file_count"])
        s.add("cloudsync_sessions_removed", "gauge",
              "Tasks still present in config.sqlite but marked removed", {}, removed)
        return connections, [t for t in sessions if not t["removed_time"]]

    @staticmethod
    def sess_labels(t):
        return {"sess_id": t["id"], "conn_id": t["conn_id"], "share": t["share_name"]}

    def _collect_sessions(self, s, sessions):
        for t in sessions:
            lbl = self.sess_labels(t)
            path = os.path.join(self.repo, "session", str(t["id"]), "event-db.sqlite")
            s.add("cloudsync_session_event_db_bytes", "gauge",
                  "Size of the session's event-db.sqlite (+wal)", lbl, db_size(path))

            def fn(conn):
                out = {}
                out["groups"] = conn.execute(
                    "SELECT file_type, is_exist, COUNT(*) AS n, "
                    "COALESCE(SUM(local_file_size),0) AS lbytes, "
                    "COALESCE(SUM(file_size),0) AS rbytes, "
                    "SUM(CASE WHEN file_id='' THEN 1 ELSE 0 END) AS no_id, "
                    "COALESCE(SUM(CASE WHEN file_id='' THEN local_file_size ELSE 0 END),0) "
                    "AS no_id_bytes, MAX(timestamp) AS ts "
                    "FROM event_info GROUP BY file_type, is_exist").fetchall()
                out["scan"] = (conn.execute("SELECT COUNT(*) AS n FROM scan_event_info")
                               .fetchone()["n"] if table_exists(conn, "scan_event_info") else 0)
                out["recycle"] = (conn.execute("SELECT COUNT(*) AS n FROM recycle_bin")
                                  .fetchone()["n"] if table_exists(conn, "recycle_bin") else 0)
                return out

            res = self._query(path, f"session/{t['id']}/event-db", fn)
            s.add("cloudsync_session_index_readable", "gauge",
                  "1 if the session's event-db.sqlite could be read", lbl,
                  0 if res is None else 1)
            if res is None:
                continue
            files = dirs = deleted = 0
            lbytes = rbytes = no_id = no_id_bytes = 0
            ts = 0
            for g in res["groups"]:
                ts = max(ts, g["ts"] or 0)
                if g["is_exist"] != 1:
                    deleted += g["n"]
                    continue
                if g["file_type"] == 0:
                    files += g["n"]
                    lbytes += g["lbytes"]
                    rbytes += g["rbytes"]
                    no_id += g["no_id"]
                    no_id_bytes += g["no_id_bytes"]
                else:
                    dirs += g["n"]
            s.add("cloudsync_session_files", "gauge",
                  "Files in the local index of the task", lbl, files)
            s.add("cloudsync_session_directories", "gauge",
                  "Directories in the local index of the task", lbl, dirs)
            s.add("cloudsync_session_deleted_entries", "gauge",
                  "Index entries whose local file no longer exists (is_exist=0)", lbl, deleted)
            s.add("cloudsync_session_local_bytes", "gauge",
                  "Sum of local file sizes in the index", lbl, lbytes)
            s.add("cloudsync_session_remote_bytes", "gauge",
                  "Sum of remote file sizes in the index (larger than local when "
                  "client-side encryption is on)", lbl, rbytes)
            s.add("cloudsync_session_files_without_remote_id", "gauge",
                  "Files with an empty remote file_id. For ID-based clouds (Google Drive, "
                  "OneDrive, Dropbox, Box) these are files not uploaded yet; for WebDAV/S3 "
                  "the field is always empty and the value is meaningless", lbl, no_id)
            s.add("cloudsync_session_bytes_without_remote_id", "gauge",
                  "Local bytes of files with an empty remote file_id (see "
                  "cloudsync_session_files_without_remote_id)", lbl, no_id_bytes)
            s.add("cloudsync_session_pending_scans", "gauge",
                  "Directory scans queued for the task (scan_event_info rows)", lbl, res["scan"])
            s.add("cloudsync_session_recycle_bin_entries", "gauge",
                  "Entries in the task's recycle_bin table", lbl, res["recycle"])
            s.add("cloudsync_session_last_index_change_timestamp_seconds", "gauge",
                  "Most recent event_info.timestamp - last time the daemon touched the index",
                  lbl, ts)

    def _collect_connections(self, s, connections, sessions):
        sess_by_id = {t["id"]: t for t in sessions}
        for c in connections:
            lbl = {"conn_id": c["id"], "task_name": c["task_name"]}
            path = os.path.join(self.repo, "connection", str(c["id"]), "server-db.sqlite")
            s.add("cloudsync_connection_server_db_bytes", "gauge",
                  "Size of the connection's server-db.sqlite (+wal)", lbl, db_size(path))

            def fn(conn):
                out = {}
                out["remote"] = conn.execute(
                    "SELECT file_type, is_exist, COUNT(*) AS n, COALESCE(SUM(file_size),0) AS b "
                    "FROM server_info GROUP BY file_type, is_exist").fetchall()
                out["pending"] = conn.execute(
                    "SELECT sess_id, ev_mode, COUNT(*) AS n, COALESCE(SUM(file_size),0) AS b "
                    "FROM medium_db_pending_events GROUP BY sess_id, ev_mode").fetchall()
                out["unfinished"] = conn.execute(
                    "SELECT sess_id, COUNT(*) AS n, COALESCE(SUM(file_size),0) AS b "
                    "FROM unfinished_event_info GROUP BY sess_id").fetchall()
                out["raw"] = (conn.execute("SELECT COUNT(*) AS n FROM medium_db_pending_raw_events")
                              .fetchone()["n"]
                              if table_exists(conn, "medium_db_pending_raw_events") else 0)
                return out

            res = self._query(path, f"connection/{c['id']}/server-db", fn)
            s.add("cloudsync_connection_server_db_readable", "gauge",
                  "1 if the connection's server-db.sqlite could be read", lbl,
                  0 if res is None else 1)
            if res is None:
                continue
            rfiles = rdirs = rbytes = 0
            for g in res["remote"]:
                if g["is_exist"] != 1:
                    continue
                if g["file_type"] == 0:
                    rfiles += g["n"]
                    rbytes += g["b"]
                else:
                    rdirs += g["n"]
            s.add("cloudsync_connection_remote_files", "gauge",
                  "Files known on the cloud side (server_info)", lbl, rfiles)
            s.add("cloudsync_connection_remote_directories", "gauge",
                  "Directories known on the cloud side (server_info)", lbl, rdirs)
            s.add("cloudsync_connection_remote_bytes", "gauge",
                  "Bytes known on the cloud side (server_info)", lbl, rbytes)
            for g in res["pending"]:
                t = sess_by_id.get(g["sess_id"])
                slbl = {"conn_id": c["id"], "sess_id": g["sess_id"],
                        "share": t["share_name"] if t else "", "mode": g["ev_mode"]}
                s.add("cloudsync_remote_pending_events", "gauge",
                      "Remote change events fetched from the cloud but not yet reconciled "
                      "(medium_db_pending_events); mode is the raw ev_mode (2 = file, 3 = "
                      "directory observed)", slbl, g["n"])
                s.add("cloudsync_remote_pending_bytes", "gauge",
                      "Bytes referenced by pending remote events", slbl, g["b"])
            for g in res["unfinished"]:
                t = sess_by_id.get(g["sess_id"])
                slbl = {"conn_id": c["id"], "sess_id": g["sess_id"],
                        "share": t["share_name"] if t else ""}
                s.add("cloudsync_unfinished_events", "gauge",
                      "Transfers the daemon has started but not finished "
                      "(unfinished_event_info)", slbl, g["n"])
                s.add("cloudsync_unfinished_bytes", "gauge",
                      "Bytes of unfinished transfers", slbl, g["b"])
            s.add("cloudsync_remote_pending_raw_events", "gauge",
                  "Raw remote change notifications not yet expanded "
                  "(medium_db_pending_raw_events)", lbl, res["raw"])

    def _collect_resume(self, s, sessions):
        path = os.path.join(self.repo, "db", "resume-info-db.sqlite")
        sess_by_id = {t["id"]: t for t in sessions}

        def fn(conn):
            return conn.execute(
                "SELECT sess_id, COUNT(*) AS n, COALESCE(SUM(retry),0) AS r "
                "FROM resume_info_table GROUP BY sess_id").fetchall()

        res = self._query(path, "resume-info", fn)
        if res is None:
            return
        for g in res:
            t = sess_by_id.get(g["sess_id"])
            slbl = {"sess_id": g["sess_id"], "conn_id": t["conn_id"] if t else "",
                    "share": t["share_name"] if t else ""}
            s.add("cloudsync_resumable_transfers", "gauge",
                  "Interrupted uploads/downloads with saved resume info", slbl, g["n"])
            s.add("cloudsync_resumable_transfer_retries", "gauge",
                  "Sum of retry counters over the saved resume infos", slbl, g["r"])

    def _collect_history(self, s):
        path = os.path.join(self.repo, "db", "history.sqlite")

        def fn(conn):
            self.history.update(conn)
            last = conn.execute(
                "SELECT conn_id, sess_id, action, MAX(time) AS t, COUNT(*) AS n "
                "FROM history_table GROUP BY conn_id, sess_id, action").fetchall()
            rows = conn.execute("SELECT COUNT(*) AS n FROM history_table").fetchone()["n"]
            return last, rows

        s.add("cloudsync_history_db_bytes", "gauge", "Size of db/history.sqlite (+wal)", {},
              db_size(path))
        res = self._query(path, "history", fn)
        if res is None:
            return
        last, rows = res
        s.add("cloudsync_history_rows", "gauge",
              "Rows currently kept in history_table (the daemon trims it to rotate_count)",
              {}, rows)
        for g in last:
            lbl = {"conn_id": g["conn_id"], "sess_id": g["sess_id"],
                   "action": HISTORY_ACTIONS.get(g["action"], str(g["action"]))}
            s.add("cloudsync_history_last_event_timestamp_seconds", "gauge",
                  "Time of the most recent history event of this kind still in the table",
                  lbl, g["t"])
            s.add("cloudsync_history_recent_events", "gauge",
                  "Events of this kind among the rows still kept in history_table",
                  lbl, g["n"])
        for (conn_id, sess_id, action), n in sorted(self.history.events.items()):
            s.add("cloudsync_history_events_total", "counter",
                  "History events seen since the exporter started (upload, download, "
                  "remove_remote, ...)",
                  {"conn_id": conn_id, "sess_id": sess_id,
                   "action": HISTORY_ACTIONS.get(action, str(action))}, n)
        for (conn_id, sess_id, action, code), n in sorted(self.history.errors.items()):
            s.add("cloudsync_history_errors_total", "counter",
                  "History events with a non-zero error code since the exporter started",
                  {"conn_id": conn_id, "sess_id": sess_id,
                   "action": HISTORY_ACTIONS.get(action, str(action)),
                   "error": ERROR_CODES.get(code, str(code)), "code": code}, n)
        s.add("cloudsync_history_dropped_rows_total", "counter",
              "History rows that rotated out before the exporter could count them "
              "(poll more often if this grows)", {}, self.history.dropped_rows)


# ------------------------------------------------------------- daemon log --

class LogState:
    """Counters and last-seen state harvested from synocloudsync.log."""

    def __init__(self):
        self.lock = threading.Lock()
        self.available = 0
        self.lines = {}            # level -> n
        self.last_ts = 0.0
        self.rotations = 0
        self.parse_errors = 0
        self.throttle = {}         # conn -> (monotonic_seen, remaining_seconds)
        self.worker_conn = {}      # worker -> conn (from throttling lines)
        self.worker_sess = {}      # worker -> sess (from "current event" lines)
        self.throttle_waits = {}   # conn -> n
        self.throttle_wait_secs = {}  # conn -> seconds
        self.api_errors = {}       # reason -> n
        self.results = {}          # HandleError text -> n
        self.uploads = {}          # sess -> n
        self.upload_failures = {}  # (sess, code) -> n
        self.events = {}           # (stage, type, sess) -> n   stage: pushed/processing/done
        self.resume_http = {}      # code -> n
        self.file_size = 0


RE_HEAD = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:[+-]\d\d:\d\d|Z)?)\s+\S+\s+"
                     r"syno-cloud-syncd\[\d+\]:\s+\[(\w+)\]\s+(.*)$")
RE_WORKER = re.compile(r"(?:[\w.-]+\(\d+\):\s+)?Worker \((\d+)\): (.*)")
RE_THROTTLING = re.compile(r"connection (\d+) is under throttling '(\d+)'")
RE_SET_THROTTLED = re.compile(r"Set throttled to wait (\d+) seconds")
RE_HANDLE_ERROR = re.compile(r"HandleError: (.+?)\.?$")
RE_UPLOAD_LOCAL = re.compile(r"UploadLocal '")
RE_UPLOAD_FAILED = re.compile(r"Failed to RunUploadLocalProtocol \((-?\d+)\)")
RE_EVENT = re.compile(r"(PushEvent|current event|Done event): Event<(\w+)> \((\w+)\): \[(\d+)\]")
RE_API_REASON = re.compile(r"error reason: \[(\w*)\]")
RE_AUTH_REASON = re.compile(r"API auth error with reason: (\w+)")
RE_RESUME_HTTP = re.compile(r"resume session return \[(\d+)\]")
EVENT_STAGES = {"PushEvent": "pushed", "current event": "processing", "Done event": "done"}


def parse_ts(text):
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def bump(d, key, n=1):
    if key not in d and len(d) >= MAX_LABEL_VALUES:
        key = "other" if not isinstance(key, tuple) else tuple("other" for _ in key)
    d[key] = d.get(key, 0) + n


def parse_log_line(line, st, now=None):
    """Update LogState from one log line. Caller holds st.lock."""
    now = time.monotonic() if now is None else now
    m = RE_HEAD.match(line)
    if not m:
        st.parse_errors += 1
        return
    ts_text, level, msg = m.groups()
    bump(st.lines, level)
    ts = parse_ts(ts_text)
    if ts:
        st.last_ts = ts

    worker = None
    wm = RE_WORKER.match(msg)
    if wm:
        worker, msg = wm.group(1), wm.group(2)

    t = RE_THROTTLING.search(msg)
    if t:
        conn, remaining = t.group(1), int(t.group(2))
        st.throttle[conn] = (now, remaining)
        if worker:
            st.worker_conn[worker] = conn
        return
    t = RE_SET_THROTTLED.search(msg)
    if t:
        conn = st.worker_conn.get(worker, "unknown")
        bump(st.throttle_waits, conn)
        bump(st.throttle_wait_secs, conn, int(t.group(1)))
        return
    t = RE_EVENT.search(msg)
    if t:
        kind, ev_type, _ev_state, sess = t.groups()
        stage = EVENT_STAGES[kind]
        bump(st.events, (stage, ev_type, sess))
        if worker and stage == "processing":
            st.worker_sess[worker] = sess
        return
    t = RE_HANDLE_ERROR.search(msg)
    if t:
        bump(st.results, t.group(1))
        return
    if RE_UPLOAD_LOCAL.search(msg):
        bump(st.uploads, st.worker_sess.get(worker, "unknown"))
        return
    t = RE_UPLOAD_FAILED.search(msg)
    if t:
        bump(st.upload_failures, (st.worker_sess.get(worker, "unknown"), t.group(1)))
        return
    t = RE_API_REASON.search(msg)
    if t:
        bump(st.api_errors, t.group(1) or "empty")
        return
    t = RE_AUTH_REASON.search(msg)
    if t:
        bump(st.api_errors, t.group(1))
        return
    t = RE_RESUME_HTTP.search(msg)
    if t:
        bump(st.resume_http, t.group(1))


class LogTailer(threading.Thread):
    """Follows the daemon log across rotations (rename -> .1 -> .xz)."""

    def __init__(self, path, state, poll=LOG_POLL_INTERVAL):
        super().__init__(daemon=True, name="log-tailer")
        self.path = path
        self.st = state
        self.poll = poll
        self.fh = None
        self.ino = None

    PRIME_BYTES = 512 * 1024

    def _open(self, from_start):
        try:
            fh = open(self.path, "rb")  # noqa: SIM115 - kept open across polls
        except OSError as e:
            if self.st.available:
                log(f"log {self.path} unavailable: {e}")
            with self.st.lock:
                self.st.available = 0
            return False
        self.ino = os.fstat(fh.fileno()).st_ino
        if not from_start:
            self._prime(fh)
            fh.seek(0, os.SEEK_END)
        self.fh = fh
        with self.st.lock:
            self.st.available = 1
        return True

    def _prime(self, fh):
        """Learn which worker serves which connection/session from the existing
        tail of the log, so counters attributed per worker are not 'unknown'
        until every worker has logged a throttling line again. Nothing is
        counted here - counters start from the moment the tailer attaches."""
        size = fh.seek(0, os.SEEK_END)
        start = max(0, size - self.PRIME_BYTES)
        fh.seek(start)
        chunk = fh.read().split(b"\n")
        if start:
            chunk = chunk[1:]  # the first line is most likely cut in half
        with self.st.lock:
            for raw in chunk:
                m = RE_HEAD.match(raw.decode("utf-8", "replace"))
                if not m:
                    continue
                wm = RE_WORKER.match(m.group(3))
                if not wm:
                    continue
                worker, msg = wm.groups()
                t = RE_THROTTLING.search(msg)
                if t:
                    self.st.worker_conn[worker] = t.group(1)
                    continue
                t = RE_EVENT.search(msg)
                if t and EVENT_STAGES[t.group(1)] == "processing":
                    self.st.worker_sess[worker] = t.group(4)

    def _consume(self):
        data = self.fh.read()
        if not data:
            return
        lines = data.split(b"\n")
        # keep an unfinished trailing line for the next round
        if not data.endswith(b"\n"):
            self.fh.seek(-len(lines[-1]), os.SEEK_CUR)
        lines = lines[:-1]
        now = time.monotonic()
        with self.st.lock:
            for raw in lines:
                parse_log_line(raw.decode("utf-8", "replace"), self.st, now)

    def step(self):
        if self.fh is None and not self._open(from_start=False):
            return
        self._consume()
        try:
            stt = os.stat(self.path)
        except OSError:
            return
        with self.st.lock:
            self.st.file_size = stt.st_size
        if stt.st_ino != self.ino or stt.st_size < self.fh.tell():
            # rotated: drain what the old file still has, then switch
            self._consume()
            self.fh.close()
            self.fh = None
            with self.st.lock:
                self.st.rotations += 1
            if self._open(from_start=True):
                self._consume()

    def run(self):
        while True:
            try:
                self.step()
            except Exception as e:  # noqa: BLE001 - keep the tailer alive whatever happens
                log(f"log tailer: {e!r}")
                self.fh = None
            time.sleep(self.poll)


def log_metrics(st, now=None):
    now = time.monotonic() if now is None else now
    s = Sample()
    with st.lock:
        s.add("cloudsync_log_available", "gauge",
              "1 if synocloudsync.log is being followed", {}, st.available)
        if not st.available and not st.lines:
            return s
        for level, n in sorted(st.lines.items()):
            s.add("cloudsync_log_lines_total", "counter", "Log lines seen by level",
                  {"level": level}, n)
        s.add("cloudsync_log_last_timestamp_seconds", "gauge",
              "Timestamp of the last log line seen (daemon liveness)", {}, st.last_ts)
        s.add("cloudsync_log_file_bytes", "gauge", "Current size of the followed log file", {},
              st.file_size)
        s.add("cloudsync_log_rotations_total", "counter", "Log rotations noticed", {},
              st.rotations)
        s.add("cloudsync_log_parse_errors_total", "counter",
              "Lines that did not match the expected syno-cloud-syncd format", {},
              st.parse_errors)
        for conn, (seen, remaining) in sorted(st.throttle.items()):
            fresh = now - seen <= THROTTLE_WINDOW
            s.add("cloudsync_connection_throttled", "gauge",
                  f"1 if the daemon logged throttling for this connection in the last "
                  f"{int(THROTTLE_WINDOW)} s", {"conn_id": conn}, 1 if fresh else 0)
            s.add("cloudsync_connection_throttle_remaining_seconds", "gauge",
                  "Back-off seconds the daemon still has to wait for this connection "
                  "(last logged value, 0 once the window has passed)", {"conn_id": conn},
                  remaining if fresh else 0)
        for conn, n in sorted(st.throttle_waits.items()):
            s.add("cloudsync_throttle_waits_total", "counter",
                  "Times a worker was put on back-off ('Set throttled to wait N seconds')",
                  {"conn_id": conn}, n)
            s.add("cloudsync_throttle_wait_seconds_total", "counter",
                  "Sum of back-off seconds workers were told to wait", {"conn_id": conn},
                  st.throttle_wait_secs.get(conn, 0))
        for reason, n in sorted(st.api_errors.items()):
            s.add("cloudsync_api_errors_total", "counter",
                  "Cloud API errors by reason (userRateLimitExceeded, rateLimitExceeded, "
                  "authError, ...)", {"reason": reason}, n)
        for result, n in sorted(st.results.items()):
            s.add("cloudsync_worker_results_total", "counter",
                  "Worker outcomes ('HandleError: ...'): Successful, Request throttled, "
                  "Auth token expired, ...", {"result": result}, n)
        for sess, n in sorted(st.uploads.items()):
            s.add("cloudsync_uploads_started_total", "counter",
                  "Upload attempts started (UploadLocal)", {"sess_id": sess}, n)
        for (sess, code), n in sorted(st.upload_failures.items()):
            s.add("cloudsync_upload_failures_total", "counter",
                  "Failed upload attempts by daemon error code",
                  {"sess_id": sess, "code": code,
                   "error": ERROR_CODES.get(num(code, None), code)}, n)
        for (stage, ev_type, sess), n in sorted(st.events.items()):
            s.add("cloudsync_events_total", "counter",
                  "Sync events by stage (pushed = detected locally, processing = picked "
                  "up by a worker, done = finished) and type (EV_ADD, EV_MODIFY, ...)",
                  {"stage": stage, "type": ev_type, "sess_id": sess}, n)
        for code, n in sorted(st.resume_http.items()):
            s.add("cloudsync_resume_session_responses_total", "counter",
                  "HTTP status codes returned when resuming an interrupted upload",
                  {"code": code}, n)
    return s


# ------------------------------------------------------------- DSM WebAPI --

def num(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return float(v)
        except (TypeError, ValueError):
            return default


def extract_list(data, preferred):
    """DSM answers {"conn":[...],"total":N} / {"sess":[...]}; be tolerant about the key."""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        if isinstance(data.get(preferred), list):
            return [x for x in data[preferred] if isinstance(x, dict)]
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return []


class DsmClient(threading.Thread):
    """Polls SYNO.CloudSync list_conn/list_sess through the DSM Web API."""

    def __init__(self, url, user, password, interval=DSM_INTERVAL):
        super().__init__(daemon=True, name="dsm-poller")
        self.url = url
        self.user = user
        self.password = password
        self.interval = interval
        self.sid = None
        self.lock = threading.Lock()
        self.up = 0
        self.last_error = ""
        self.conns = []
        self.sess = {}   # conn_id -> list
        self.last_ok = 0.0
        self.requests = 0
        self.failures = 0
        ctx = ssl.create_default_context()
        if not DSM_VERIFY_TLS:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        self.opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))

    def _call(self, params):
        q = urllib.parse.urlencode(params)
        req = urllib.request.Request(f"{self.url}/webapi/entry.cgi?{q}",
                                     headers={"User-Agent": "cloudsync-exporter"})
        self.requests += 1
        with self.opener.open(req, timeout=DSM_TIMEOUT) as r:
            body = json.loads(r.read().decode())
        if not body.get("success"):
            code = body.get("error", {}).get("code")
            raise RuntimeError(f"{params.get('api')}.{params.get('method')} failed: error {code}")
        return body.get("data")

    def login(self):
        data = self._call({"api": "SYNO.API.Auth", "version": "6", "method": "login",
                           "account": self.user, "passwd": self.password,
                           "session": "CloudSyncExporter", "format": "sid"})
        self.sid = data["sid"]

    def _list_conn(self):
        return self._call({"api": "SYNO.CloudSync", "version": "1", "method": "list_conn",
                           "is_tray": "false", "_sid": self.sid})

    def poll(self):
        if not self.sid:
            self.login()
        try:
            data = self._list_conn()
        except RuntimeError as e:
            # 119 = invalid/expired sid, 105/106/107 = session problems: re-login once
            if any(f"error {c}" in str(e) for c in (105, 106, 107, 119)):
                self.login()
                data = self._list_conn()
            else:
                raise
        conns = extract_list(data, "conn")
        sess = {}
        for c in conns:
            cid = c.get("id")
            if cid is None:
                continue
            d = self._call({"api": "SYNO.CloudSync", "version": "1", "method": "list_sess",
                            "connection_id": str(cid), "_sid": self.sid})
            sess[cid] = extract_list(d, "sess")
        debug(f"dsm list_conn: {json.dumps(conns)[:2000]}")
        debug(f"dsm list_sess: {json.dumps(sess)[:2000]}")
        with self.lock:
            self.conns, self.sess, self.up, self.last_error = conns, sess, 1, ""
            self.last_ok = time.time()

    def run(self):
        while True:
            try:
                self.poll()
            except Exception as e:  # noqa: BLE001 - keep polling
                with self.lock:
                    self.up = 0
                    self.failures += 1
                    self.last_error = str(e)
                log(f"dsm: {e}")
                self.sid = None
            time.sleep(self.interval)


def dsm_metrics(client):
    s = Sample()
    with client.lock:
        s.add("cloudsync_dsm_up", "gauge",
              "1 if the last DSM Web API poll (SYNO.CloudSync list_conn) succeeded", {},
              client.up)
        s.add("cloudsync_dsm_requests_total", "counter", "DSM Web API requests made", {},
              client.requests)
        s.add("cloudsync_dsm_poll_failures_total", "counter", "DSM Web API poll failures", {},
              client.failures)
        s.add("cloudsync_dsm_last_success_timestamp_seconds", "gauge",
              "When the DSM Web API was last polled successfully", {}, client.last_ok)
        for c in client.conns:
            cid = c.get("id")
            lbl = {"conn_id": cid, "task_name": c.get("task_name", "")}
            status = str(c.get("status", "unknown"))
            states = DSM_STATES + ((status,) if status not in DSM_STATES else ())
            for state in states:
                s.add("cloudsync_dsm_connection_state", "gauge",
                      "Connection status as shown in the Cloud Sync UI; exactly one "
                      "state per connection is 1", {**lbl, "state": state},
                      1 if state == status else 0)
            s.add("cloudsync_dsm_connection_unfinished_files", "gauge",
                  "Files still to be processed for this connection ('Processing N "
                  "file(s)...' in the UI)", lbl, num(c.get("unfinished_files")))
            s.add("cloudsync_dsm_connection_info", "gauge",
                  "Connection status details from the DSM Web API; value is always 1",
                  {**lbl, "link_status": str(c.get("link_status", "")),
                   "last_sync_status": str(c.get("last_sync_status", "")),
                   "error_type": str(c.get("error_type", ""))}, 1)
            s.add("cloudsync_dsm_connection_error_code", "gauge",
                  "Connection error code reported by the DSM Web API (0 = none)", lbl,
                  num(c.get("error")))
            s.add("cloudsync_dsm_connection_session_errors", "gauge",
                  "Sessions of this connection currently in error (sess_err_cnt)", lbl,
                  num(c.get("sess_err_cnt")))
            s.add("cloudsync_dsm_connection_next_sync_timestamp_seconds", "gauge",
                  "Next scheduled sync (0 when not scheduled)", lbl,
                  num(c.get("next_sync_timestamp")))
            for t in client.sess.get(cid, []):
                slbl = {"conn_id": cid, "sess_id": t.get("sess_id", t.get("id", "")),
                        "share": t.get("share_name", "")}
                st_ = str(t.get("status", t.get("link_status", "unknown")))
                s.add("cloudsync_dsm_session_state", "gauge",
                      "Task status as reported by the DSM Web API (list_sess)",
                      {**slbl, "state": st_}, 1)
                s.add("cloudsync_dsm_session_error_code", "gauge",
                      "Task error code reported by the DSM Web API (0 = none)", slbl,
                      num(t.get("error")))
    return s


# ------------------------------------------------------------------- HTTP --

class Exporter:
    def __init__(self, repo, log_state=None, dsm=None, refresh=REFRESH_INTERVAL):
        self.db = DbCollector(repo)
        self.log_state = log_state
        self.dsm = dsm
        self.refresh = refresh
        self.cache = None
        self.cache_at = 0.0
        self.lock = threading.Lock()
        self.scrapes = 0

    def db_sample(self):
        with self.lock:
            if self.cache is None or time.monotonic() - self.cache_at >= self.refresh:
                self.cache = self.db.collect()
                self.cache_at = time.monotonic()
            return self.cache

    def render(self):
        self.scrapes += 1
        s = Sample()
        s.merge(self.db_sample())
        if self.log_state is not None:
            s.merge(log_metrics(self.log_state))
        if self.dsm is not None:
            s.merge(dsm_metrics(self.dsm))
        s.add("cloudsync_exporter_scrapes_total", "counter", "Scrapes served", {},
              self.scrapes)
        s.add("cloudsync_exporter_cache_age_seconds", "gauge",
              "Age of the database snapshot served", {},
              round(time.monotonic() - self.cache_at, 1))
        return s.render()


def make_handler(exporter):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                self._send(200, b"ok\n", "text/plain")
                return
            if self.path in ("/", ""):
                self._send(200, b"cloudsync-exporter: /metrics /healthz\n", "text/plain")
                return
            if self.path.split("?")[0] != "/metrics":
                self._send(404, b"not found\n", "text/plain")
                return
            try:
                body = exporter.render().encode()
            except Exception as e:  # noqa: BLE001
                log(f"render failed: {e!r}")
                self._send(500, f"error: {e}\n".encode(), "text/plain")
                return
            self._send(200, body, "text/plain; version=0.0.4; charset=utf-8")

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            pass

    return Handler


def main():
    log_state = None
    if LOG_DIR:
        log_state = LogState()
        LogTailer(os.path.join(LOG_DIR, LOG_FILE), log_state).start()
    dsm = None
    if DSM_URL:
        password = DSM_PASS
        if DSM_PASS_FILE:
            with open(DSM_PASS_FILE) as f:
                password = f.read().strip()
        if not DSM_USER or not password:
            log("DSM_URL set but DSM_USER/DSM_PASS missing - DSM polling disabled")
        else:
            dsm = DsmClient(DSM_URL, DSM_USER, password)
            dsm.start()
    exporter = Exporter(REPO, log_state, dsm)
    server = ThreadingHTTPServer(("", LISTEN_PORT), make_handler(exporter))
    log(f"cloudsync-exporter listening on :{LISTEN_PORT}, repo={REPO}, "
        f"log={'on' if log_state else 'off'}, dsm={'on' if dsm else 'off'}")
    server.serve_forever()


if __name__ == "__main__":
    main()
