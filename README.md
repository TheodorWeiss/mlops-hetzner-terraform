# BikeML MLOps Stack — Citi Bike Availability Risk Forecasting

Учебный MLOps-проект по прогнозу риска дефицита велосипедов и свободных доков в системе Citi Bike.

Цель проекта — построить production-like ML-систему уровня зрелости 2: от сбора raw-данных и хранения в object storage до Airflow pipeline, feature engineering, обучения модели, model registry, serving API и мониторинга.

Текущий статус: базовый MLOps-стек поднят на отдельном Hetzner Cloud сервере, а raw GBFS ingestion bridge успешно работает через Airflow DAG.

---

## 1. Архитектура серверов

В проекте используются два сервера.

### 1.1. Persistent Raw Ingestion Server

Существующий сервер, не управляемый Terraform.

Назначение:

- регулярно собирает live GBFS данные Citi Bike;
- сохраняет первичные raw snapshots;
- остаётся постоянным хранилищем raw-данных;
- не удаляется при `terraform destroy`.

Путь к raw-данным на старом сервере:

```text
/srv/bikeml/raw/gbfs/
├── latest/
│   ├── collector_status.json
│   ├── station_status_latest.json
│   └── station_information_latest.json
├── station_status/YYYY/MM/DD/*.json.gz
└── station_information/YYYY/MM/DD/*.json.gz
```

Collector на старом сервере продолжает собирать данные примерно каждые 5 минут.

На старом сервере создан отдельный read-only SSH-пользователь:

```text
bikeml_ro
```

Он используется новым MLOps-сервером только для чтения raw GBFS данных.

### 1.2. MLOps Server

Новый сервер в Hetzner Cloud, созданный через Terraform.

Фактически использованная конфигурация:

```text
Hetzner CX33
4 vCPU
8 GB RAM
Ubuntu 24.04 LTS
mounted volume: /mnt/mlops-data
Docker + Docker Compose
user: deploy
```

На этом сервере поднят основной MLOps-стек.

---

## 2. Базовый MLOps-стек

Стек запускается через Docker Compose в проекте:

```bash
docker compose -p bikeml up -d
```

Основные сервисы:

| Сервис | Назначение |
|---|---|
| PostgreSQL | metadata Airflow, backend MLflow, проектная БД `bikeml` |
| MinIO | S3-compatible object storage |
| Redis | будущий online store для Feast |
| MLflow | tracking server и будущий model registry |
| Airflow | orchestration DAGs |
| Airflow custom image | Airflow + `rsync`, `openssh-client`, `mc` |

Созданные базы PostgreSQL:

```text
airflow
mlflow
bikeml
```

Созданные MinIO bucket-ы:

```text
bikeml-raw
bikeml-processed
bikeml-models
bikeml-reports
```

---

## 3. Доступ к UI

UI-порты не открываются наружу. Доступ выполняется через SSH-туннель.

Пример с Windows PowerShell:

```powershell
ssh -i "$env:USERPROFILE\.ssh\id_ed25519_mlops_exam" `
  -L 8080:127.0.0.1:8080 `
  -L 5000:127.0.0.1:5000 `
  -L 9001:127.0.0.1:9001 `
  deploy@<MLOPS_SERVER_IP>
```

После открытия туннеля:

```text
Airflow: http://127.0.0.1:8080
MLflow:  http://127.0.0.1:5000
MinIO:   http://127.0.0.1:9001
```

---

## 4. Raw GBFS ingestion bridge

Реализован Airflow DAG:

```text
gbfs_ingestion_bridge
```

Расписание:

```text
*/15 * * * *
```

То есть DAG запускается каждые 15 минут.

Текущая цепочка задач:

```text
sync_gbfs_raw_to_minio
    ↓
check_upstream_freshness
```

### 4.1. Task `sync_gbfs_raw_to_minio`

Задача запускает скрипт:

```text
/opt/airflow/scripts/sync_gbfs_raw.sh
```

Скрипт выполняет:

1. rsync `latest/` со старого ingestion-сервера;
2. rsync `station_status/` archive;
3. rsync `station_information/` archive;
4. mirror `station_status` в MinIO bucket `bikeml-raw`;
5. mirror `station_information` в MinIO bucket `bikeml-raw`;
6. mirror `latest` в MinIO bucket `bikeml-raw`;
7. выводит контрольные counts.

Стратегия синхронизации:

```text
latest/              rsync без --ignore-existing, потому что файлы перезаписываются
dated archive         rsync с --ignore-existing, потому что json.gz файлы иммутабельны
MinIO dated archive   mc mirror без --overwrite
MinIO latest          mc mirror --overwrite
```

Пример итоговых логов успешного запуска:

```text
station_status local files: 254
station_information local files: 28

station_status MinIO files: 254
station_information MinIO files: 28

Latest station_status key in MinIO:
local/bikeml-raw/gbfs/station_status/2026/06/03/station_status_20260603_173105.json.gz
```

### 4.2. Task `check_upstream_freshness`

Задача запускает скрипт:

```text
/opt/airflow/scripts/check_upstream_freshness.py
```

Скрипт проверяет, что новый сервер не работает молча на устаревших данных.

Проверки:

- `collector_status.json` существует;
- `collector_status.status == "ok"`;
- `station_status_rows > 0`;
- `last_success_utc` не старше порога;
- самый свежий `station_status_*.json.gz` файл не старше порога.

Порог задаётся переменной:

```env
UPSTREAM_FRESHNESS_MAX_AGE_MINUTES=30
```

Пример успешной проверки:

```text
collector_status.status=ok
collector_status.station_status_rows=2410
last_success_age_seconds=15
latest_station_status_file_age_seconds=16
FRESHNESS_CHECK_OK
```

Если freshness-check падает, Airflow task завершается ошибкой. Это предотвращает ситуацию, когда DAG зелёный, но данные фактически устарели.

---

## 5. Переменные окружения

Реальные секреты хранятся в `.env`, который не должен попадать в Git.

Шаблон должен храниться в:

```text
.env.example
```

Ключевые переменные ingestion bridge:

```env
INGESTION_SSH_HOST=<OLD_INGESTION_SERVER_IP>
INGESTION_SSH_USER=bikeml_ro
INGESTION_RAW_PATH=/srv/bikeml/raw/gbfs
INGESTION_SSH_KEY_PATH=/opt/airflow/.ssh/bikeml_ingestion_read
UPSTREAM_FRESHNESS_MAX_AGE_MINUTES=30
```

Важно: приватный SSH-ключ для Airflow хранится на MLOps-сервере в отдельной папке:

```text
/mnt/mlops-data/airflow/ssh/bikeml_ingestion_read
```

Он монтируется в Airflow container read-only:

```text
/opt/airflow/.ssh/bikeml_ingestion_read
```

---

## 6. Проверки

### 6.1. Проверить контейнеры

```bash
docker compose -p bikeml ps
```

Ожидаем, что основные сервисы работают:

```text
bikeml-postgres
bikeml-minio
bikeml-redis
bikeml-mlflow
bikeml-airflow-webserver
bikeml-airflow-scheduler
```

### 6.2. Проверить Airflow DAG

```bash
docker compose -p bikeml exec airflow-scheduler bash -lc 'airflow dags list | grep gbfs'
```

Ожидаемый DAG:

```text
gbfs_ingestion_bridge
```

Проверить задачи DAG:

```bash
docker compose -p bikeml exec airflow-scheduler bash -lc 'airflow tasks list gbfs_ingestion_bridge'
```

Ожидаемые задачи:

```text
check_upstream_freshness
sync_gbfs_raw_to_minio
```

### 6.3. Запустить DAG вручную

```bash
docker compose -p bikeml exec airflow-scheduler bash -lc 'airflow dags trigger gbfs_ingestion_bridge'
```

### 6.4. Проверить freshness-check log

```bash
cat "$(find /mnt/mlops-data/airflow/logs/dag_id=gbfs_ingestion_bridge -path '*task_id=check_upstream_freshness*' -type f | sort | tail -n 1)"
```

В успешном случае должны быть строки:

```text
FRESHNESS_CHECK_OK
Command exited with return code 0
Marking task as SUCCESS
```

### 6.5. Проверить количество raw-файлов в MinIO

```bash
docker run --rm \
  --network bikeml-net \
  --env-file .env \
  --entrypoint /bin/sh \
  minio/mc:latest \
  -c 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc find local/bikeml-raw/gbfs/station_status --name "*.json.gz" | wc -l'
```

Последний `station_status` key:

```bash
docker run --rm \
  --network bikeml-net \
  --env-file .env \
  --entrypoint /bin/sh \
  minio/mc:latest \
  -c 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc find local/bikeml-raw/gbfs/station_status --name "*.json.gz" | sort | tail -n 1'
```

---

## 7. Текущий статус

На текущей контрольной точке закрыто:

```text
Persistent Raw Ingestion Server
    ↓ read-only SSH user bikeml_ro
Airflow DAG on MLOps Server
    ↓ sync_gbfs_raw_to_minio
Local staging + MinIO bikeml-raw
    ↓ check_upstream_freshness
Freshness validation of upstream collector and latest station_status data
```

Это означает, что raw ingestion bridge работает автоматически и проверяет свежесть upstream-источника.

---

## 8. Known limitations

Текущие осознанные ограничения MVP:

1. `StrictHostKeyChecking=no` используется в SSH-команде внутри ingestion script. Для учебного MVP допустимо, но в production лучше закрепить host key старого сервера в `known_hosts`.
2. Пока нет Telegram/email alert через `on_failure_callback`. Ошибки видны в Airflow UI.
3. Пока нет FastAPI serving layer, поэтому `stale_state` ещё не возвращается в API-ответах.
4. Raw GBFS уже попадает в MinIO, но JSON ещё не парсится в PostgreSQL structured tables.
5. `station_id_mapping` ещё не построен на текущих боевых данных.

---

## 9. Следующие шаги

Рекомендуемый порядок дальнейшей реализации:

1. Зафиксировать текущий checkpoint в Git.
2. Создать SQL-таблицы для structured GBFS данных.
3. Спарсить `station_information` и построить `station_id_mapping`.
4. Подтвердить mapping coverage на текущих данных.
5. Спарсить `station_status` в `bikeml.gbfs_status_snapshots`.
6. Добавить DQ counters и таблицу `gbfs_ingestion_log`.
7. Расширить Airflow DAG task-ом parsing.
8. После подтверждённого mapping переходить к historical trip CSV ingestion.
9. Далее — feature engineering, baseline model, MLflow tracking, serving API и monitoring.

---

## 10. Terraform infrastructure note

Инфраструктура MLOps-сервера создана через Terraform в Hetzner Cloud.

Terraform отвечает за:

- создание нового сервера;
- firewall;
- SSH key;
- volume;
- базовую подготовку сервера через cloud-init.

Docker Compose отвечает за runtime-стек сервисов внутри уже созданного сервера.

Старый Persistent Raw Ingestion Server не импортируется в Terraform и не управляется через него. Он остаётся отдельным постоянным raw ingestion сервером.

Подробное описание Terraform-конфигурации находится в:

```text
terraform/README.md
```
