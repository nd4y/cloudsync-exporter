"""Tests against synthetic copies of the Cloud Sync databases (schemas mirror
DSM 7.2 / Cloud Sync 2.7.2) and log lines captured from a real daemon."""

import sqlite3
import sys

import pytest

import exporter

# --- fixtures: a minimal Cloud Sync repository --------------------------------

CONFIG_SCHEMA = """
CREATE TABLE connection_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER NOT NULL, gid INTEGER NOT NULL,
    client_type INTEGER NOT NULL, task_name TEXT NOT NULL, local_user_name TEXT NOT NULL,
    user_name TEXT NOT NULL, access_token TEXT NOT NULL DEFAULT '',
    refresh_token TEXT NOT NULL DEFAULT '', root_folder_path TEXT NOT NULL DEFAULT '/',
    max_upload_speed INTEGER NOT NULL DEFAULT 0, max_download_speed INTEGER NOT NULL DEFAULT 0,
    pull_event_period INTEGER NOT NULL DEFAULT 60, status INTEGER NOT NULL DEFAULT 1,
    error INTEGER NOT NULL DEFAULT 0, last_sync_status INTEGER NOT NULL DEFAULT 0,
    is_enabled_schedule INTEGER NOT NULL DEFAULT 0);
CREATE TABLE session_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT, conn_id INTEGER NOT NULL, share_name TEXT NOT NULL,
    sync_folder TEXT NOT NULL, server_folder_id TEXT NOT NULL DEFAULT '',
    server_folder_path TEXT NOT NULL, enable_server_encryption INTEGER NOT NULL DEFAULT 0,
    status INTEGER NOT NULL DEFAULT 1, error INTEGER NOT NULL DEFAULT 0,
    create_time DATETIME DEFAULT (strftime('%s', 'now')), removed_time DATETIME,
    sync_direction INTEGER NOT NULL DEFAULT 0, remote_file_count INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0);
"""

HISTORY_SCHEMA = """
CREATE TABLE history_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT, conn_id INTEGER NOT NULL, sess_id INTEGER NOT NULL,
    uid INTEGER NOT NULL DEFAULT 1026, action INTEGER NOT NULL, name TEXT NOT NULL DEFAULT '',
    path TEXT NOT NULL DEFAULT '', to_name TEXT NOT NULL DEFAULT '',
    to_path TEXT NOT NULL DEFAULT '', file_type INTEGER NOT NULL DEFAULT 0,
    time INTEGER NOT NULL, log_level INTEGER NOT NULL DEFAULT 0,
    error_code INTEGER NOT NULL DEFAULT 0);
"""

RESUME_SCHEMA = """
CREATE TABLE resume_info_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT, sess_id INTEGER NOT NULL,
    resume_info TEXT NOT NULL DEFAULT '', description TEXT DEFAULT '', retry INTEGER DEFAULT 0);
"""

EVENT_SCHEMA = """
CREATE TABLE event_info (
    id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, inode INTEGER default NULL,
    file_type INTEGER NOT NULL, is_exist INTEGER NOT NULL, local_mtime INTEGER NOT NULL DEFAULT 0,
    mtime INTEGER NOT NULL DEFAULT 0, local_file_size INTEGER NOT NULL DEFAULT 0,
    file_size INTEGER NOT NULL DEFAULT 0, file_hash TEXT NOT NULL DEFAULT '',
    file_id TEXT NOT NULL DEFAULT '', timestamp INTEGER NOT NULL DEFAULT 0);
CREATE TABLE scan_event_info (path TEXT NOT NULL, type INTEGER NOT NULL, ref_cnt INTEGER DEFAULT 0,
    primary key (path, type) ON CONFLICT IGNORE);
CREATE TABLE recycle_bin (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL);
"""

SERVER_SCHEMA = """
CREATE TABLE server_info (
    id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, file_type INTEGER NOT NULL,
    is_exist INTEGER NOT NULL, file_size INTEGER NOT NULL DEFAULT 0);
CREATE TABLE medium_db_pending_events (
    control_flag INTEGER NOT NULL DEFAULT 1, ev_type INTEGER NOT NULL DEFAULT 1,
    ev_status INTEGER NOT NULL DEFAULT 0, sess_id INTEGER NOT NULL, ev_mode INTEGER NOT NULL,
    path TEXT NOT NULL DEFAULT '', file_size INTEGER NOT NULL DEFAULT 0);
CREATE TABLE unfinished_event_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT, sess_id INTEGER NOT NULL,
    file_size INTEGER NOT NULL DEFAULT 0, path TEXT NOT NULL DEFAULT '');
CREATE TABLE medium_db_pending_raw_events (file_id TEXT NOT NULL);
"""


def make_db(path, schema, statements=()):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(schema)
    for sql, params in statements:
        conn.execute(sql, params)
    conn.commit()
    conn.close()


@pytest.fixture
def repo(tmp_path):
    make_db(tmp_path / "db" / "config.sqlite", CONFIG_SCHEMA, [
        ("INSERT INTO connection_table (id, uid, gid, client_type, task_name, local_user_name, "
         "user_name, pull_event_period, last_sync_status) VALUES (7, 1026, 100, 1, "
         "'Google Drive', 'AV', 'Alexandr V', 300, 0)", ()),
        ("INSERT INTO session_table (id, conn_id, share_name, sync_folder, server_folder_path, "
         "enable_server_encryption, sync_direction, priority, create_time) VALUES "
         "(12, 7, 'homes', '/AV', '/CloudSync/st1/homes/AV', 1, 2, 1, 1788632306)", ()),
        ("INSERT INTO session_table (id, conn_id, share_name, sync_folder, server_folder_path, "
         "status, removed_time, create_time) VALUES (13, 7, 'Veeam', '/', "
         "'/CloudSync/st1/Veeam', 2, 1788739336, 1788632397)", ()),
    ])
    make_db(tmp_path / "db" / "history.sqlite", HISTORY_SCHEMA, [
        ("INSERT INTO history_table (id, conn_id, sess_id, action, time) VALUES (100, 7, 12, 2, 1000)",
         ()),
        ("INSERT INTO history_table (id, conn_id, sess_id, action, time) VALUES (101, 7, 12, 0, 1001)",
         ()),
    ])
    make_db(tmp_path / "db" / "resume-info-db.sqlite", RESUME_SCHEMA, [
        ("INSERT INTO resume_info_table (sess_id, retry) VALUES (12, 3)", ()),
        ("INSERT INTO resume_info_table (sess_id, retry) VALUES (12, 0)", ()),
    ])
    make_db(tmp_path / "session" / "12" / "event-db.sqlite", EVENT_SCHEMA, [
        ("INSERT INTO event_info (path, file_type, is_exist, local_file_size, file_size, file_id, "
         "timestamp) VALUES ('/a', 1, 1, 0, 0, 'dir1', 10)", ()),
        ("INSERT INTO event_info (path, file_type, is_exist, local_file_size, file_size, file_id, "
         "timestamp) VALUES ('/a/x.bin', 0, 1, 100, 1100, 'f1', 20)", ()),
        ("INSERT INTO event_info (path, file_type, is_exist, local_file_size, file_size, file_id, "
         "timestamp) VALUES ('/a/y.bin', 0, 1, 50, 0, '', 30)", ()),
        ("INSERT INTO event_info (path, file_type, is_exist, local_file_size, file_size, file_id, "
         "timestamp) VALUES ('/gone', 0, 0, 7, 7, 'f2', 5)", ()),
        ("INSERT INTO scan_event_info (path, type) VALUES ('/', 10)", ()),
    ])
    # the removed session still has a directory on disk - it must be ignored
    make_db(tmp_path / "session" / "13" / "event-db.sqlite", EVENT_SCHEMA, [
        ("INSERT INTO event_info (path, file_type, is_exist) VALUES ('/big.vbk', 0, 1)", ()),
    ])
    make_db(tmp_path / "connection" / "7" / "server-db.sqlite", SERVER_SCHEMA, [
        ("INSERT INTO server_info (path, file_type, is_exist, file_size) VALUES ('/r', 1, 1, 0)", ()),
        ("INSERT INTO server_info (path, file_type, is_exist, file_size) VALUES ('/r/x', 0, 1, 1100)",
         ()),
        ("INSERT INTO server_info (path, file_type, is_exist, file_size) VALUES ('/r/old', 0, 0, 9)",
         ()),
        ("INSERT INTO medium_db_pending_events (sess_id, ev_mode, file_size) VALUES (12, 2, 500)", ()),
        ("INSERT INTO medium_db_pending_events (sess_id, ev_mode, file_size) VALUES (12, 2, 600)", ()),
        ("INSERT INTO medium_db_pending_events (sess_id, ev_mode, file_size) VALUES (12, 3, 0)", ()),
        ("INSERT INTO unfinished_event_info (sess_id, file_size) VALUES (12, 42)", ()),
    ])
    return tmp_path


def parse(text):
    """exposition text -> {metric{labels}: value}"""
    out = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        key, _, value = line.rpartition(" ")
        out[key] = float(value)
    return out


# --- database collector -------------------------------------------------------

def test_collect_config_and_sessions(repo):
    m = parse(exporter.DbCollector(str(repo)).collect().render())
    assert m["cloudsync_repo_readable"] == 1
    assert m['cloudsync_connection_info{conn_id="7",task_name="Google Drive",client_type="1",'
             'local_user="AV",remote_user="Alexandr V",remote_root="/"}'] == 1
    assert m['cloudsync_connection_pull_event_period_seconds{conn_id="7",task_name="Google Drive"}'] \
        == 300
    assert m['cloudsync_session_info{sess_id="12",conn_id="7",share="homes",'
             'local_path="/homes/AV",remote_path="/CloudSync/st1/homes/AV",sync_direction="2",'
             'encrypted="1",priority="1"}'] == 1
    assert m["cloudsync_sessions_removed"] == 1
    # removed session 13 produces no series at all
    assert not any('sess_id="13"' in k for k in m)


def test_collect_event_db(repo):
    m = parse(exporter.DbCollector(str(repo)).collect().render())
    lbl = '{sess_id="12",conn_id="7",share="homes"}'
    assert m["cloudsync_session_index_readable" + lbl] == 1
    assert m["cloudsync_session_files" + lbl] == 2
    assert m["cloudsync_session_directories" + lbl] == 1
    assert m["cloudsync_session_deleted_entries" + lbl] == 1
    assert m["cloudsync_session_local_bytes" + lbl] == 150
    assert m["cloudsync_session_remote_bytes" + lbl] == 1100
    assert m["cloudsync_session_files_without_remote_id" + lbl] == 1
    assert m["cloudsync_session_bytes_without_remote_id" + lbl] == 50
    assert m["cloudsync_session_pending_scans" + lbl] == 1
    assert m["cloudsync_session_recycle_bin_entries" + lbl] == 0
    assert m["cloudsync_session_last_index_change_timestamp_seconds" + lbl] == 30
    assert m["cloudsync_session_event_db_bytes" + lbl] > 0


def test_collect_server_db_and_resume(repo):
    m = parse(exporter.DbCollector(str(repo)).collect().render())
    c = '{conn_id="7",task_name="Google Drive"}'
    assert m["cloudsync_connection_server_db_readable" + c] == 1
    assert m["cloudsync_connection_remote_files" + c] == 1
    assert m["cloudsync_connection_remote_directories" + c] == 1
    assert m["cloudsync_connection_remote_bytes" + c] == 1100
    assert m['cloudsync_remote_pending_events{conn_id="7",sess_id="12",share="homes",mode="2"}'] == 2
    assert m['cloudsync_remote_pending_bytes{conn_id="7",sess_id="12",share="homes",mode="2"}'] \
        == 1100
    assert m['cloudsync_remote_pending_events{conn_id="7",sess_id="12",share="homes",mode="3"}'] == 1
    assert m['cloudsync_unfinished_events{conn_id="7",sess_id="12",share="homes"}'] == 1
    assert m['cloudsync_unfinished_bytes{conn_id="7",sess_id="12",share="homes"}'] == 42
    assert m["cloudsync_remote_pending_raw_events" + c] == 0
    assert m['cloudsync_resumable_transfers{sess_id="12",conn_id="7",share="homes"}'] == 2
    assert m['cloudsync_resumable_transfer_retries{sess_id="12",conn_id="7",share="homes"}'] == 3


def test_history_counters_only_count_new_rows(repo):
    col = exporter.DbCollector(str(repo))
    m = parse(col.collect().render())
    assert m["cloudsync_history_rows"] == 2
    assert m['cloudsync_history_recent_events{conn_id="7",sess_id="12",action="upload"}'] == 1
    assert m['cloudsync_history_last_event_timestamp_seconds{conn_id="7",sess_id="12",'
             'action="remove_remote"}'] == 1001
    # nothing counted yet: counters start from the state at startup
    assert not any(k.startswith("cloudsync_history_events_total") for k in m)

    conn = sqlite3.connect(repo / "db" / "history.sqlite")
    conn.execute("INSERT INTO history_table (id, conn_id, sess_id, action, time) VALUES (102, 7, 12, 2, 1002)")
    conn.execute("INSERT INTO history_table (id, conn_id, sess_id, action, time, log_level, error_code) "
                 "VALUES (103, 7, 12, 6, 1003, 2, -21)")
    conn.commit()
    conn.close()
    m = parse(col.collect().render())
    assert m['cloudsync_history_events_total{conn_id="7",sess_id="12",action="upload"}'] == 1
    assert m['cloudsync_history_events_total{conn_id="7",sess_id="12",action="err_upload_remote_file"}'] \
        == 1
    assert m['cloudsync_history_errors_total{conn_id="7",sess_id="12",action="err_upload_remote_file",'
             'error="request_throttled",code="-21"}'] == 1
    assert m["cloudsync_history_dropped_rows_total"] == 0


def test_history_detects_rotation_gap(repo):
    col = exporter.DbCollector(str(repo))
    col.collect()
    conn = sqlite3.connect(repo / "db" / "history.sqlite")
    conn.execute("DELETE FROM history_table")
    conn.execute("INSERT INTO history_table (id, conn_id, sess_id, action, time) VALUES (150, 7, 12, 2, 1)")
    conn.commit()
    conn.close()
    m = parse(col.collect().render())
    assert m["cloudsync_history_dropped_rows_total"] == 150 - 101 - 1


def test_missing_repo_is_reported_not_fatal(tmp_path):
    m = parse(exporter.DbCollector(str(tmp_path / "nope")).collect().render())
    assert m["cloudsync_repo_readable"] == 0
    assert "cloudsync_collect_duration_seconds" in m


def test_unreadable_db_counts_error(repo):
    (repo / "connection" / "7" / "server-db.sqlite").write_bytes(b"not a database at all\n" * 50)
    m = parse(exporter.DbCollector(str(repo)).collect().render())
    assert m['cloudsync_connection_server_db_readable{conn_id="7",task_name="Google Drive"}'] == 0
    assert m['cloudsync_db_read_errors_total{db="connection/7/server-db"}'] == 1


def test_exporter_caches_db_snapshot(repo):
    ex = exporter.Exporter(str(repo), refresh=3600)
    first = ex.render()
    conn = sqlite3.connect(repo / "db" / "config.sqlite")
    conn.execute("UPDATE connection_table SET pull_event_period = 5")
    conn.commit()
    conn.close()
    second = ex.render()
    assert parse(first)['cloudsync_connection_pull_event_period_seconds{conn_id="7",'
                        'task_name="Google Drive"}'] == 300
    assert parse(second)['cloudsync_connection_pull_event_period_seconds{conn_id="7",'
                         'task_name="Google Drive"}'] == 300
    assert parse(second)["cloudsync_exporter_scrapes_total"] == 2


# --- log parser -----------------------------------------------------------------

LOG = """\
2026-09-07T19:37:06+03:00 st1 syno-cloud-syncd[24411]: [INFO] worker.cpp(1028): Worker (32): connection 7 is under throttling '289'
2026-09-07T19:41:55+03:00 st1 syno-cloud-syncd[24411]: [INFO] worker.cpp(887): Worker (32): HandleError: Auth token expired.
2026-09-07T14:41:26+03:00 st1 syno-cloud-syncd[24411]: [INFO] worker.cpp(887): Worker (32): HandleError: Request throttled.
2026-09-07T14:41:26+03:00 st1 syno-cloud-syncd[24411]: [NOTE] worker.cpp(891): Worker (32): Set throttled to wait 300 seconds.
2026-09-07T14:41:26+03:00 st1 syno-cloud-syncd[24411]: [NOTE] worker.cpp(891): Worker (99): Set throttled to wait 5 seconds.
2026-09-07T14:41:26+03:00 st1 syno-cloud-syncd[24411]: [ERROR] gd-transport.cpp(2360): Hit user-rate limitation.
2026-09-07T14:41:26+03:00 st1 syno-cloud-syncd[24411]: [ERROR] gd-transport.cpp(2297): error reason: [userRateLimitExceeded].
2026-09-07T14:41:26+03:00 st1 syno-cloud-syncd[24411]: [ERROR] gd-transport.cpp(2298): error message: [User rate limit exceeded].
2026-09-07T15:21:36+03:00 st1 syno-cloud-syncd[24411]: [ERROR] gd-transport.cpp(3320): [-110] API auth error with reason: authError. URL='https://www.googleapis.com/drive/v2/files?x'
2026-09-07T15:21:36+03:00 st1 syno-cloud-syncd[24411]: [INFO] worker.cpp(1051): Worker (20): current event: Event<EV_MODIFY> (PROCESSING): [12] /Personal/x.jpg (local,file) size = 0, hash = ,
2026-09-07T15:21:36+03:00 st1 syno-cloud-syncd[24411]: [INFO] worker.cpp(1942): Worker (20): UploadLocal '/volume1/homes/AV/Personal/x.jpg'.
2026-09-07T15:21:37+03:00 st1 syno-cloud-syncd[24411]: [ERROR] worker.cpp(2099): Worker (20): Failed to RunUploadLocalProtocol (-21) '/Personal/x.jpg'.
2026-09-07T15:21:37+03:00 st1 syno-cloud-syncd[24411]: [INFO] dscs-detector.cpp(1): PushEvent: Event<EV_ADD> (WAITTING): [12] /Workdir/new.txt (local,file)
2026-09-07T15:21:38+03:00 st1 syno-cloud-syncd[24411]: [INFO] event-peatree.cpp(1): Done event: Event<EV_MODIFY> (PROCESSING): [12] /Personal/x.jpg
2026-09-07T15:21:38+03:00 st1 syno-cloud-syncd[24411]: [INFO] worker.cpp(887): Worker (20): HandleError: Successful.
2026-09-07T19:41:55+03:00 st1 syno-cloud-syncd[24411]: [INFO] gd-transport.cpp(1267): resume session return [403] --> set start_byte = 0.
garbage line that is not from the daemon
"""


def feed(text, now=1000.0):
    st = exporter.LogState()
    st.available = 1
    with st.lock:
        for line in text.splitlines():
            exporter.parse_log_line(line, st, now)
    return st


def test_log_parser_counters():
    st = feed(LOG)
    m = parse(exporter.log_metrics(st, now=1000.0).render())
    assert m['cloudsync_log_lines_total{level="INFO"}'] == 9
    assert m['cloudsync_log_lines_total{level="ERROR"}'] == 5
    assert m['cloudsync_log_lines_total{level="NOTE"}'] == 2
    assert m["cloudsync_log_continuation_lines_total"] == 1
    assert m['cloudsync_connection_throttled{conn_id="7"}'] == 1
    assert m['cloudsync_connection_throttle_remaining_seconds{conn_id="7"}'] == 289
    # worker 32 was seen throttling for connection 7, worker 99 never was
    assert m['cloudsync_throttle_waits_total{conn_id="7"}'] == 1
    assert m['cloudsync_throttle_wait_seconds_total{conn_id="7"}'] == 300
    assert m['cloudsync_throttle_waits_total{conn_id="unknown"}'] == 1
    assert m['cloudsync_api_errors_total{reason="userRateLimitExceeded"}'] == 1
    assert m['cloudsync_api_errors_total{reason="authError"}'] == 1
    assert m['cloudsync_worker_results_total{result="Auth token expired"}'] == 1
    assert m['cloudsync_worker_results_total{result="Request throttled"}'] == 1
    assert m['cloudsync_worker_results_total{result="Successful"}'] == 1
    assert m['cloudsync_uploads_started_total{sess_id="12"}'] == 1
    assert m['cloudsync_upload_failures_total{sess_id="12",code="-21",error="request_throttled"}'] == 1
    assert m['cloudsync_events_total{stage="processing",type="EV_MODIFY",sess_id="12"}'] == 1
    assert m['cloudsync_events_total{stage="pushed",type="EV_ADD",sess_id="12"}'] == 1
    assert m['cloudsync_events_total{stage="done",type="EV_MODIFY",sess_id="12"}'] == 1
    assert m['cloudsync_resume_session_responses_total{code="403"}'] == 1
    # 2026-09-07T19:41:55+03:00
    assert m["cloudsync_log_last_timestamp_seconds"] == 1788799315


def test_throttle_state_expires():
    st = feed(LOG, now=1000.0)
    m = parse(exporter.log_metrics(st, now=1000.0 + exporter.THROTTLE_WINDOW + 1).render())
    assert m['cloudsync_connection_throttled{conn_id="7"}'] == 0
    assert m['cloudsync_connection_throttle_remaining_seconds{conn_id="7"}'] == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Windows cannot rename an open file")
def test_log_tailer_follows_and_survives_rotation(tmp_path):
    path = tmp_path / "synocloudsync.log"
    lines = LOG.splitlines()
    path.write_text(lines[0] + "\n")
    st = exporter.LogState()
    tailer = exporter.LogTailer(str(path), st, poll=0)
    tailer.step()  # opens at EOF: the pre-existing line is not counted
    assert st.available == 1 and st.lines == {}
    with path.open("a") as f:
        f.write(lines[1] + "\n" + lines[2][:20])  # second line complete, third partial
    tailer.step()
    assert st.lines == {"INFO": 1}
    with path.open("a") as f:
        f.write(lines[2][20:] + "\n")
    tailer.step()
    assert st.lines == {"INFO": 2}
    # rotation: rename current file away, start a fresh one
    path.rename(tmp_path / "synocloudsync.log.1")
    path.write_text(lines[3] + "\n")
    tailer.step()
    assert st.rotations == 1
    assert st.lines == {"INFO": 2, "NOTE": 1}


def test_log_tailer_primes_worker_mapping(tmp_path):
    path = tmp_path / "synocloudsync.log"
    path.write_text("\n".join(LOG.splitlines()[:1] + LOG.splitlines()[9:10]) + "\n")
    st = exporter.LogState()
    tailer = exporter.LogTailer(str(path), st, poll=0)
    tailer.step()
    assert st.lines == {}                      # nothing counted from the old tail
    assert st.worker_conn == {"32": "7"}       # but the mapping is known
    assert st.worker_sess == {"20": "12"}
    with path.open("a") as f:
        f.write(LOG.splitlines()[3] + "\n")   # worker 32: Set throttled to wait 300
    tailer.step()
    assert st.throttle_waits == {"7": 1}


def test_immutable_fallback_on_io_error(repo, monkeypatch):
    col = exporter.DbCollector(str(repo))
    real_open = exporter.open_ro
    calls = []

    def fake_open(path, immutable=False):
        calls.append(immutable)
        if path.endswith("history.sqlite") and not immutable:
            raise sqlite3.OperationalError("disk I/O error")
        return real_open(path, immutable)

    monkeypatch.setattr(exporter, "open_ro", fake_open)
    m = parse(col.collect().render())
    assert m["cloudsync_history_rows"] == 2
    assert m['cloudsync_db_immutable_fallback_reads_total{db="history"}'] == 1
    assert not any(k.startswith("cloudsync_db_read_errors_total") for k in m)


def test_label_cardinality_guard():
    d = {}
    for i in range(exporter.MAX_LABEL_VALUES + 10):
        exporter.bump(d, f"reason{i}")
    assert len(d) == exporter.MAX_LABEL_VALUES + 1
    assert d["other"] == 10


# --- DSM Web API mapping ---------------------------------------------------------

class FakeDsm:
    lock = exporter.threading.Lock()
    up = 1
    requests = 3
    failures = 0
    last_ok = 123.0
    conns = [{"id": 7, "task_name": "Google Drive", "status": "syncing", "link_status": "online",
              "unfinished_files": 1234, "last_sync_status": "unknown", "error": 0,
              "error_type": "", "sess_err_cnt": 0, "next_sync_timestamp": 0}]
    sess = {7: [{"sess_id": 12, "share_name": "homes", "status": "syncing", "error": 0}]}


def test_dsm_metrics_state_set():
    m = parse(exporter.dsm_metrics(FakeDsm()).render())
    assert m["cloudsync_dsm_up"] == 1
    c = 'conn_id="7",task_name="Google Drive"'
    assert m[f'cloudsync_dsm_connection_state{{{c},state="syncing"}}'] == 1
    assert m[f'cloudsync_dsm_connection_state{{{c},state="uptodate"}}'] == 0
    assert m[f'cloudsync_dsm_connection_unfinished_files{{{c}}}'] == 1234
    assert m[f'cloudsync_dsm_connection_info{{{c},link_status="online",last_sync_status="unknown",'
             f'error_type=""}}'] == 1
    assert m['cloudsync_dsm_session_state{conn_id="7",sess_id="12",share="homes",state="syncing"}'] == 1


def test_extract_list_shapes():
    assert exporter.extract_list({"conn": [{"id": 1}], "total": 1}, "conn") == [{"id": 1}]
    assert exporter.extract_list({"items": [{"id": 2}]}, "conn") == [{"id": 2}]
    assert exporter.extract_list([{"id": 3}, "x"], "conn") == [{"id": 3}]
    assert exporter.extract_list(None, "conn") == []
