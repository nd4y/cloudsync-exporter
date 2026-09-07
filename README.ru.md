# cloudsync-exporter

[🇬🇧 English](README.md) | 🇷🇺 **Русский**

Prometheus-экспортер для **Synology Cloud Sync** (DSM 7, Cloud Sync 2.7.x).

У Cloud Sync нет ни эндпойнта метрик, ни ветки в SNMP — единственное окно
в него это UI DSM. Зато демон (`syno-cloud-syncd`) хранит всё состояние в
sqlite-базах в своём репозитории и пишет подробный лог. Экспортер читает и то и
другое **только на чтение** и отдаёт:

- конфигурацию (соединения, задачи, направление, шифрование, лимиты);
- размер каждой задачи (файлы, каталоги, локальные/удалённые байты) и сколько
  ещё не лежит в облаке;
- очередь: ожидающие удалённые события, незавершённые передачи, возобновляемые
  загрузки, отложенные сканы каталогов;
- что происходило недавно: загрузки/скачивания/удаления по задачам, ошибки по
  кодам (ротируемая таблица истории превращена в счётчики);
- **троттлит ли облако** и на сколько, ошибки API по причинам
  (`userRateLimitExceeded`, `rateLimitExceeded`, `authError`, …), исходы
  воркеров, стадии событий (обнаружено → в обработке → готово);
- опционально — тот же статус, что показывает UI (`uptodate`/`syncing`/`pause`/…)
  и счётчик *«Processing N files»*, через DSM Web API.

Только стандартная библиотека Python, один файл, работает на `python:3.12-alpine`.

## Как устроено

```
/volumeN/@cloudsync/                     (repo_path в config/daemon.conf)
├── db/config.sqlite          соединения + сессии (задачи)
├── db/history.sqlite         последние 500 событий на соединение (ротация)
├── db/resume-info-db.sqlite  прерванные передачи с resume info
├── connection/<id>/server-db.sqlite   листинг облака, ожидающие удалённые
│                                      события, незавершённые передачи
└── session/<id>/event-db.sqlite       локальный индекс синхронизируемого каталога

/var/packages/CloudSync/var/log/synocloudsync.log   (доступен только root)
```

Базы открываются через SQLite `mode=ro`. Они в режиме WAL; read-only bind mount
подходит (SQLite переходит на wal-index в памяти). Если чтение всё же падает с
I/O error, экспортер перечитывает основной файл с `immutable=1` и отмечает это в
`cloudsync_db_immutable_fallback_reads_total`. Временные b-tree держатся в памяти
(`PRAGMA temp_store=MEMORY`): при read-only корне без tmpfs `GROUP BY` по большому
индексу иначе падает с тем же самым «disk I/O error». Метрики из баз кэшируются на
`REFRESH_INTERVAL` секунд независимо от частоты скрейпа; лог читается непрерывно
и переживает ротацию.

Лог демона на DSM читает только root. В эталонном compose контейнер остаётся
root'ом, но из capability оставлена одна `DAC_READ_SEARCH` (читать чужие файлы),
корневая ФС read-only. Без монтирования лога экспортер просто не отдаёт
лог-метрики.

## Метрики

Метки: `conn_id`/`task_name` — соединение (облачный аккаунт), `sess_id`/
`conn_id`/`share` — задача (один синхронизируемый каталог).

### Конфигурация (`db/config.sqlite`)

| Метрика | Описание |
|---|---|
| `cloudsync_connection_info{client_type,local_user,remote_user,remote_root}` | 1 на соединение |
| `cloudsync_connection_status`, `_error`, `_last_sync_status` | сырые целые из `connection_table` |
| `cloudsync_connection_pull_event_period_seconds` | период опроса облака на изменения |
| `cloudsync_connection_max_{upload,download}_speed_kbytes` | лимиты на файл (0 = нет) |
| `cloudsync_connection_schedule_enabled` | 1, если синхронизация ограничена расписанием |
| `cloudsync_session_info{local_path,remote_path,sync_direction,encrypted,priority}` | 1 на задачу |
| `cloudsync_session_status`, `_error` | сырые целые из `session_table` |
| `cloudsync_session_created_timestamp_seconds`, `_remote_file_count` | |
| `cloudsync_sessions_removed` | задачи, ещё лежащие в БД с отметкой removed (пропускаются) |

Наблюдения на Cloud Sync 2.7.2: `client_type` 1 = Google Drive, 11 = WebDAV;
`sync_direction` 2 = «Upload local changes only»; `status` 1 = активна,
2 = удалённая задача.

### Локальный индекс (`session/<id>/event-db.sqlite`)

| Метрика | Описание |
|---|---|
| `cloudsync_session_files`, `_directories` | записей в индексе |
| `cloudsync_session_deleted_entries` | записи, у которых локального файла уже нет |
| `cloudsync_session_local_bytes`, `_remote_bytes` | remote больше при шифровании на клиенте |
| `cloudsync_session_files_without_remote_id`, `_bytes_without_remote_id` | файлы без облачного file id — **для облаков с идентификаторами (Google Drive, OneDrive, Dropbox, Box) это ещё не залитый хвост**; для WebDAV/S3 поле всегда пустое, там метрика бессмысленна |
| `cloudsync_session_pending_scans` | отложенные сканы каталогов |
| `cloudsync_session_recycle_bin_entries` | |
| `cloudsync_session_last_index_change_timestamp_seconds` | когда демон последний раз трогал индекс |
| `cloudsync_session_index_readable`, `cloudsync_session_event_db_bytes` | |

### Сторона облака (`connection/<id>/server-db.sqlite`)

| Метрика | Описание |
|---|---|
| `cloudsync_connection_remote_files`, `_remote_directories`, `_remote_bytes` | что демон знает об облачном каталоге |
| `cloudsync_remote_pending_events{sess_id,mode}`, `cloudsync_remote_pending_bytes` | удалённые события изменений, полученные, но ещё не сведённые (`mode` 2 = файл, 3 = каталог) |
| `cloudsync_unfinished_events{sess_id}`, `cloudsync_unfinished_bytes` | передачи начаты, но не закончены |
| `cloudsync_remote_pending_raw_events` | сырые уведомления об изменениях, ещё не развёрнутые |
| `cloudsync_resumable_transfers{sess_id}`, `cloudsync_resumable_transfer_retries` | из `db/resume-info-db.sqlite` |

### История (`db/history.sqlite`)

Демон хранит последние 500 событий на соединение. Экспортер запоминает
максимальный виденный id и считает новые строки, поэтому счётчики точны, пока
между двумя обновлениями приходит меньше 500 событий на соединение
(`cloudsync_history_dropped_rows_total` покажет, если это случилось).

| Метрика | Описание |
|---|---|
| `cloudsync_history_events_total{action}` | счётчик с момента старта экспортера; `action` = upload, download, remove_remote, remove_local, rename_*, merge, … |
| `cloudsync_history_errors_total{action,error,code}` | события с ненулевым кодом ошибки (`error` расшифрован: request_throttled, quota, auth_token_expired, …) |
| `cloudsync_history_last_event_timestamp_seconds{action}` | последнее событие такого рода в таблице (gauge, доступна сразу) |
| `cloudsync_history_recent_events{action}` | строк такого рода сейчас в таблице |
| `cloudsync_history_rows`, `cloudsync_history_dropped_rows_total` | |

### Лог демона (опционально, `CLOUDSYNC_LOG_DIR`)

| Метрика | Описание |
|---|---|
| `cloudsync_connection_throttled{conn_id}` | 1, пока демон пишет `connection N is under throttling` (окно `THROTTLE_WINDOW`) |
| `cloudsync_connection_throttle_remaining_seconds{conn_id}` | сколько ещё ждать (последнее записанное значение) |
| `cloudsync_throttle_waits_total{conn_id}`, `cloudsync_throttle_wait_seconds_total` | сколько раз и на сколько суммарно воркеров отправляли в back-off |
| `cloudsync_api_errors_total{reason}` | `userRateLimitExceeded`, `rateLimitExceeded`, `authError`, … |
| `cloudsync_worker_results_total{result}` | `Successful`, `Request throttled`, `Auth token expired`, … |
| `cloudsync_uploads_started_total{sess_id}`, `cloudsync_upload_failures_total{sess_id,code,error}` | |
| `cloudsync_events_total{stage,type,sess_id}` | `stage` pushed (обнаружено локально) → processing → done; `type` EV_ADD, EV_MODIFY, … |
| `cloudsync_resume_session_responses_total{code}` | HTTP-коды при возобновлении прерванных загрузок |
| `cloudsync_log_available`, `_lines_total{level}`, `_last_timestamp_seconds`, `_file_bytes`, `_rotations_total`, `_continuation_lines_total` | здоровье tailer'а; `last_timestamp` заодно показывает, жив ли демон |

В строках воркеров нет id соединения, поэтому экспортер сопоставляет воркеры с
соединениями/задачами по строкам throttling и `current event` (при старте для
этого перечитываются последние 512 КБ лога). Пока воркер не встретился, его
счётчики попадают в `conn_id="unknown"`.

### DSM Web API (опционально, `DSM_URL`)

Нужна учётная запись DSM с доступом к Cloud Sync (API пускает и не-админов, но
они видят только свои соединения). Экспортер логинится через `SYNO.API.Auth` и
опрашивает `SYNO.CloudSync` `list_conn` + `list_sess`.

| Метрика | Описание |
|---|---|
| `cloudsync_dsm_connection_state{state}` | набор состояний: `uptodate`, `syncing`, `processing`, `scanning`, `connecting`, `pause`, `suspended`, `error`, `unlink` — ровно одно равно 1 |
| `cloudsync_dsm_connection_unfinished_files` | то самое *«Processing N file(s)…»* из UI |
| `cloudsync_dsm_connection_info{link_status,last_sync_status,error_type}` | |
| `cloudsync_dsm_connection_error_code`, `_session_errors`, `_next_sync_timestamp_seconds` | |
| `cloudsync_dsm_session_state{state}`, `cloudsync_dsm_session_error_code` | по задачам |
| `cloudsync_dsm_up`, `_requests_total`, `_poll_failures_total`, `_last_success_timestamp_seconds` | |

Этот источник написан по вызовам самого UI DSM, но на живом DSM с учётными
данными пока не проверялся — при странностях запускайте с `DEBUG=1`, сырые
ответы уйдут в лог.

### Сам экспортер

`cloudsync_repo_readable`, `cloudsync_collect_duration_seconds`,
`cloudsync_db_read_errors_total{db}`, `cloudsync_db_immutable_fallback_reads_total{db}`,
`cloudsync_exporter_scrapes_total`, `cloudsync_exporter_cache_age_seconds`,
`cloudsync_*_db_bytes`.

## Настройка (env)

| Переменная | По умолчанию | |
|---|---|---|
| `CLOUDSYNC_REPO` | `/cloudsync` | куда смонтирован репозиторий (`repo_path` из `daemon.conf`, например `/volume2/@cloudsync`) |
| `CLOUDSYNC_LOG_DIR` | *(выкл.)* | куда смонтирован `/var/packages/CloudSync/var/log` |
| `CLOUDSYNC_LOG_FILE` | `synocloudsync.log` | |
| `LISTEN_PORT` | `9840` | |
| `REFRESH_INTERVAL` | `30` | секунд между перечитываниями баз |
| `LOG_POLL_INTERVAL` | `5` | секунд между опросами лога |
| `THROTTLE_WINDOW` | `60` | сколько секунд после последней строки throttling соединение считается троттлимым |
| `DSM_URL` | *(выкл.)* | например `https://192.168.0.10:5001` |
| `DSM_USER`, `DSM_PASS` / `DSM_PASS_FILE` | | |
| `DSM_VERIFY_TLS` | `true` | `false` для самоподписанного сертификата DSM |
| `DSM_INTERVAL`, `DSM_TIMEOUT` | `30`, `15` | |
| `DEBUG` | | писать в лог сырые ответы DSM и фолбэки |

## Запуск

```bash
docker run -d --name cloudsync-exporter \
  -v /volume2/@cloudsync:/cloudsync:ro \
  -v /volume1/@appdata/CloudSync/log:/cloudsync-log:ro \
  -e CLOUDSYNC_LOG_DIR=/cloudsync-log \
  --read-only --cap-drop ALL --cap-add DAC_READ_SEARCH \
  -p 9840:9840 ghcr.io/nd4y/cloudsync-exporter:latest
```

Или `docker compose up -d` с приложенным `docker-compose.yml`. Том репозитория
ищется так: `cat /var/packages/CloudSync/target/etc/daemon.conf` или
`ls -d /volume*/@cloudsync`.

`/healthz` — дешёвая проверка живости без обращения к базам, healthcheck
контейнера направлять туда.

## Полезные запросы

```promql
# файлы, которых ещё нет в облаке (задачи Google Drive / OneDrive / Dropbox)
cloudsync_session_files_without_remote_id

# то, что UI показывает как «Processing N files» (нужен DSM_URL)
cloudsync_dsm_connection_unfinished_files

# загрузок в час
increase(cloudsync_history_events_total{action="upload"}[1h])

# доля последнего часа, проведённая в back-off от Google
increase(cloudsync_throttle_wait_seconds_total[1h]) / 3600

# троттлинг прямо сейчас
cloudsync_connection_throttled == 1
```

## Ограничения

- Очередь демона в памяти никуда не персистится; точное *«N files to process»*
  есть только в DSM Web API (`DSM_URL`). Гейджи очереди из баз
  (`files_without_remote_id`, `remote_pending_events`, `unfinished_events`,
  `resumable_transfers`) — ближайшие сохраняемые эквиваленты.
- При шифровании на клиенте удалённые размеры включают накладные расходы
  шифрования, поэтому локальные и удалённые байты никогда не сходятся точно.
- Семантика части сырых целых (`status`, `last_sync_status`, `sync_direction`,
  `client_type`) задокументирована только по наблюдениям на Cloud Sync 2.7.2 и
  отдаётся как есть, без догадок.
