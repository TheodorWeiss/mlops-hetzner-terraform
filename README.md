# BikeML MLOps Stack — Citi Bike Availability Forecasting

Учебный MLOps-проект по прогнозу изменения доступности велосипедов на станциях Citi Bike на горизонте 1 час.

MVP строит production-like ML-систему уровня зрелости 2: от сбора данных и feature engineering до Airflow pipeline, MLflow Model Registry, champion/candidate/challenger workflow, честного quality gate, serving API и мониторинга.

Основная модель прогнозирует:

```text
delta_bikes_1h = returns_1h - departures_1h
```

На inference прогноз соединяется с текущим live-состоянием станции:

```text
predicted_bikes_1h = current_bikes_now + predicted_delta_bikes_1h
predicted_bikes_1h = clip(predicted_bikes_1h, 0, capacity)
```

Поверх прогноза вычисляется производный сигнал:

```text
low_availability_risk = predicted_bikes_1h <= threshold
```

---

## 1. Цель проекта

Цель проекта — показать полный жизненный цикл ML-системы:

```text
raw data
→ ingestion
→ structured tables
→ feature engineering
→ model training
→ model evaluation
→ MLflow registry
→ champion/candidate/challenger
→ quality gate
→ serving
→ monitoring
→ monthly retraining
→ promotion / rollback
```

Бизнес-смысл: заранее выявлять станции, где через час может остаться слишком мало велосипедов, чтобы оператор мог принять решение о ребалансировке.

---

## 2. Архитектура серверов

В проекте используются два сервера.

### Persistent Raw Ingestion Server

Существующий сервер, не управляемый Terraform.

Он регулярно собирает live GBFS данные Citi Bike и сохраняет raw snapshots:

```text
/srv/bikeml/raw/gbfs/
├── latest/
│   ├── collector_status.json
│   ├── station_status_latest.json
│   └── station_information_latest.json
├── station_status/YYYY/MM/DD/*.json.gz
└── station_information/YYYY/MM/DD/*.json.gz
```

Этот сервер остаётся постоянным raw-хранилищем и не удаляется при `terraform destroy`.

### MLOps Server

Новый сервер в Hetzner Cloud, созданный через Terraform.

Фактическая конфигурация:

```text
Hetzner CX33
4 vCPU
8 GB RAM
Ubuntu 24.04 LTS
mounted volume: /mnt/mlops-data
Docker + Docker Compose
user: deploy
```

На этом сервере работает основной MLOps-стек.

---

## 3. Основные компоненты

Стек запускается через Docker Compose:

```bash
docker compose -p bikeml up -d
```

Основные сервисы:

| Компонент | Роль |
|---|---|
| PostgreSQL | Airflow metadata, MLflow backend, проектная БД `bikeml` |
| MinIO | S3-compatible object storage для raw/model/report artifacts |
| MLflow | Tracking server + Model Registry |
| Airflow | Оркестрация ingestion, feature engineering, training, quality gate |
| Redis | Зарезервирован под будущий online store |
| FastAPI | Serving API: `/health`, `/predict/station`, `/predict/batch` |

MinIO buckets:

```text
bikeml-raw
bikeml-processed
bikeml-models
bikeml-reports
```

---

## 4. Данные

### Offline training data

Исторические поездки Citi Bike:

```text
https://s3.amazonaws.com/tripdata/
```

Используются месячные CSV/ZIP-файлы. На текущем этапе загружены данные за февраль–май 2026.

Из tripdata строится чистый supervised target:

```text
departures_1h
returns_1h
delta_bikes_1h = returns_1h - departures_1h
```

Агрегация выполняется по паре:

```text
station_id × hour
```

### Online data

Live GBFS `station_status` используется для текущего состояния станции:

```text
current_bikes
current_docks
capacity
state_age_seconds
stale_state
```

GBFS `station_information` используется для метаданных станции и mapping.

### Station mapping

Historical trip CSV использует legacy station id / short name, а live GBFS использует UUID-like `station_id`.

Связка выполняется через:

```text
station_information.short_name
```

Маппинг проверяется отдельно, служебные и внешние станции фильтруются.

### Weather

Погодные данные берутся из Open-Meteo:

- archive API для train/test;
- forecast API для inference.

В MVP это осознанный train/serving skew: обучение использует фактическую историческую погоду, inference использует прогноз погоды на целевой час.

---

## 5. Airflow DAGs

### `gbfs_ingestion_bridge`

DAG синхронизирует raw GBFS данные со старого raw ingestion server на MLOps server и в MinIO.

Цепочка задач:

```text
sync_gbfs_raw_to_minio
→ check_upstream_freshness
→ parse_station_information
→ parse_station_status
→ build_gbfs_online_features
```

Задача `check_upstream_freshness` проверяет, что новый сервер не работает на устаревших данных.

### `monthly_tripdata_ingestion`

DAG отвечает за monthly retraining lifecycle.

Цепочка задач:

```text
check_and_download_tripdata
→ continue_if_new_file
→ parse_new_tripdata
→ fetch_weather_for_new_tripdata
→ build_station_hourly_features
→ build_delta_training_features
→ evaluate_current_champion_on_new_test
→ train_delta_lightgbm_weather
→ promote_delta_candidate_if_passed
→ train_delta_xgboost_weather_challenger
```

Логика:

1. Проверить появление нового monthly tripdata.
2. Скачать новый файл, если он появился.
3. Распарсить поездки в hourly demand tables.
4. Скачать weather archive за тот же месяц.
5. Пересобрать feature tables.
6. Переоценить текущий champion на новом test split.
7. Обучить LightGBM candidate.
8. Принять решение promotion/reject через честный quality gate.
9. Обучить XGBoost challenger.

---

## 6. Модели и MLflow Registry

Основная production-модель:

```text
bikeml_delta_bikes_forecaster
```

MLflow aliases:

| Alias | Назначение |
|---|---|
| `@champion` | production-модель для serving |
| `@candidate` | новая LightGBM-модель после monthly retraining |
| `@challenger` | XGBoost-модель для сравнения устойчивости |

Текущая production-постановка:

```text
target = delta_bikes_1h
features = hour, day_of_week, is_weekend, month, lat, lon, capacity, weather
```

Trip-lag модель сохранена только как offline benchmark / upper bound и не используется в serving, потому что её lag-фичи недоступны в live inference.

---

## 7. Quality gate

Candidate-модель не становится champion автоматически.

При появлении нового месяца:

1. Текущий `@champion` переоценивается на новом test split.
2. Новая LightGBM candidate обучается и оценивается на том же test split.
3. Candidate получает promotion только если:

```text
candidate_MAE <= champion_MAE_on_same_test_df * 0.99
AND candidate_MAE < baseline_MAE
```

Если условие не выполнено:

```text
@champion остаётся прежним
@candidate сохраняется как отклонённая версия
```

Rollback выполняется переключением MLflow alias `@champion` на предыдущую стабильную версию.

---

## 8. Текущий статус

Реализовано:

```text
Terraform provisioning
Docker Compose MLOps stack
PostgreSQL / MinIO / MLflow / Airflow
GBFS ingestion bridge
structured GBFS layer
station_id_mapping
monthly tripdata ingestion
automatic pickup of new monthly CSV
weather archive ingestion
delta feature engineering
LightGBM weather model
XGBoost weather challenger
MLflow champion/candidate/challenger aliases
fair quality gate with champion re-evaluation
```

Подтверждён живой monthly retraining scenario:

```text
202605-citibike-tripdata.zip появился на S3
→ DAG скачал файл
→ распарсил май
→ скачал weather archive за май
→ пересобрал delta features
→ обучил candidate/challenger
→ выполнил fair quality gate
```

---

## 9. Проверки

### Проверить контейнеры

```bash
docker compose -p bikeml ps
```

### Проверить Airflow DAGs

```bash
docker compose -p bikeml exec airflow-scheduler bash -lc 'airflow dags list'
```

```bash
docker compose -p bikeml exec airflow-scheduler bash -lc 'airflow tasks list monthly_tripdata_ingestion'
```

### Проверить MLflow aliases

```bash
docker compose -p bikeml exec airflow-scheduler bash -lc '
python - <<PY
import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_tracking_uri("http://mlflow:5000")
client = MlflowClient("http://mlflow:5000")

name = "bikeml_delta_bikes_forecaster"

for alias in ["champion", "candidate", "challenger"]:
    mv = client.get_model_version_by_alias(name, alias)
    print(alias, "version=", mv.version, "run_id=", mv.run_id, "tags=", dict(mv.tags))
PY
'
```

### Проверить последние model evaluation runs

```bash
docker exec -it bikeml-postgres psql -U bikeml_admin -d bikeml -c "
SELECT
    id,
    model_type,
    registered_model_version,
    rows_test,
    ROUND(mae_delta::numeric, 6) AS mae_delta,
    ROUND(rmse_delta::numeric, 6) AS rmse_delta,
    ROUND(bias_delta::numeric, 6) AS bias_delta,
    promotion_decision,
    created_at
FROM delta_model_evaluation_runs
ORDER BY created_at DESC
LIMIT 10;
"
```

---

## 10. Доступ к UI

UI-порты не открываются наружу. Доступ выполняется через SSH-туннель.

Пример Windows PowerShell:

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

## 11. Переменные окружения

Реальные секреты хранятся в `.env`, который не должен попадать в Git.

Шаблон хранится в:

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

---

## 12. Known limitations

Ограничения MVP:

1. FastAPI serving layer находится в работе.
2. Full Feast implementation вынесен в stretch; MVP использует feature tables в PostgreSQL.
3. Great Expectations, Evidently, Prometheus/Grafana вынесены в stretch.
4. Ребалансировка не моделируется, так как нет данных о действиях оператора.
5. Погода в train берётся из archive API, а в inference будет использовать forecast API.
6. Production SLA, multi-region и отказоустойчивость коммерческого уровня не входят в MVP.

---

## 13. Следующие шаги

Ближайший порядок работ:

1. Зафиксировать текущий checkpoint в Git.
2. Реализовать FastAPI:
   - `GET /health`;
   - `POST /predict/station`;
   - `POST /predict/batch`.
3. Добавить запись прогнозов в `prediction_log` с `target_time = predicted_at + 1 hour`.
4. Реализовать online evaluation: сравнение прогнозов с фактическими GBFS snapshots через час.
5. Подготовить `reports/sli_slo.md` с SLI/SLO на техническом, модельном и бизнес-уровнях.
6. Подготовить ADR по latency с H0/H1, статистическим тестом и p-value.
7. Добавить минимальный GitHub Actions workflow.
8. Финально проверить `/health`, Airflow DAGs, MLflow aliases, teardown-инструкцию.

---

## 14. Terraform infrastructure note

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
