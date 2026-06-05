# BikeML MLOps Stack — Citi Bike Availability Forecasting

Учебный MLOps-проект по прогнозу изменения доступности велосипедов на станциях Citi Bike на горизонте 1 час.

Проект строит production-like ML-систему уровня зрелости 2: от сбора данных и feature engineering до Airflow pipeline, MLflow Model Registry, champion/candidate/challenger workflow, честного quality gate, FastAPI serving, CI/CD и мониторинга в Prometheus/Grafana.

Основная модель прогнозирует:

```text
delta_bikes_1h = returns_1h - departures_1h
```

На inference прогноз соединяется с текущим live-состоянием станции:

```text
predicted_bikes_1h = current_bikes_now + predicted_delta_bikes_1h
predicted_bikes_1h = clip(predicted_bikes_1h, 0, capacity)
```

Поверх прогноза вычисляется производный бизнес-сигнал:

```text
low_availability_risk = predicted_bikes_1h <= threshold
```

Главный манифест проекта:

```text
MANIFEST.md
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
→ логирование прогнозов
→ online-оценку прогнозов
→ monitoring
→ ежемесячное переобучение
→ promotion / rollback
```

Бизнес-смысл: заранее выявлять станции, где через час может остаться слишком мало велосипедов, чтобы оператор мог принять решение о ребалансировке.

---

## 2. Архитектура серверов

В проекте используются два сервера.

### 2.1. Persistent Raw Ingestion Server

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

Для доступа к нему используется отдельный read-only пользователь:

```text
bikeml_ro
```

Это осознанное security-решение: MLOps-сервер может читать raw GBFS snapshots, но не должен иметь root-доступ к persistent raw server.

### 2.2. MLOps Server

Новый сервер в Hetzner Cloud, созданный через Terraform.

Фактическая конфигурация:

```text
Hetzner Cloud
Ubuntu 24.04 LTS
mounted volume: /mnt/mlops-data
Docker + Docker Compose
user: deploy
```

На этом сервере работает основной MLOps-стек:

```text
PostgreSQL
MinIO
MLflow
Airflow
Redis
FastAPI
Prometheus
Grafana
exporters
self-hosted GitHub Actions runner
```

---

## 3. Terraform / IaC

Инфраструктура MLOps-сервера создана через Terraform в Hetzner Cloud.

Terraform отвечает за:

- создание нового сервера;
- firewall;
- SSH key;
- volume;
- базовую подготовку сервера через cloud-init;
- установку Docker/Docker Compose;
- подготовку пользователя `deploy`.

Docker Compose отвечает за runtime-стек сервисов внутри уже созданного сервера.

Старый Persistent Raw Ingestion Server не импортируется в Terraform и не управляется через него. Он остаётся отдельным постоянным raw ingestion сервером.

Подробное описание Terraform-конфигурации находится в:

```text
terraform/README.md
```

---

## 4. Основные компоненты

Стек запускается через Docker Compose:

```bash
docker compose -p bikeml up -d
```

Основные сервисы:

| Компонент | Роль |
|---|---|
| PostgreSQL | Airflow metadata, MLflow backend, проектная БД `bikeml`, feature tables, prediction logs |
| MinIO | S3-compatible object storage для raw/model/report artifacts |
| MLflow | Tracking server + Model Registry |
| Airflow | Оркестрация ingestion, feature engineering, training, quality gate |
| Redis | Зарезервирован под будущий online store |
| FastAPI | Serving API: `/health`, `/predict/station`, `/predict/batch`, `/metrics` |
| Prometheus | Сбор технических, API и exporter-метрик |
| Grafana | Dashboard с техническими, модельными и бизнесовыми метриками |
| Blackbox exporter | HTTP health probes |
| Node exporter | Метрики сервера |
| cAdvisor | Метрики Docker containers |
| Postgres exporter | Метрики PostgreSQL |
| GitHub Actions self-hosted runner | CD на MLOps-сервере |

MinIO buckets:

```text
bikeml-raw
bikeml-processed
bikeml-models
bikeml-reports
```

---

## 5. Данные

### 5.1. Offline training data

Исторические поездки Citi Bike:

```text
https://s3.amazonaws.com/tripdata/
```

Используются месячные CSV/ZIP-файлы. На текущем этапе загружены данные за февраль–май 2026.

Из tripdata строится supervised target:

```text
departures_1h
returns_1h
delta_bikes_1h = returns_1h - departures_1h
```

Агрегация выполняется по паре:

```text
station_id × hour
```

### 5.2. Online data

Live GBFS `station_status` используется для текущего состояния станции:

```text
current_bikes
current_docks
capacity
state_age_seconds
stale_state
```

GBFS `station_information` используется для метаданных станции и mapping.

### 5.3. Station mapping

Historical trip CSV использует legacy station id / short name, а live GBFS использует UUID-like `station_id`.

Связка выполняется через:

```text
station_information.short_name
```

Это ключевой mapping-слой проекта: без него невозможно корректно соединить historical trip data и live GBFS station status.

Проверки mapping:

- отсутствие дублей по `short_name`;
- проверка coverage start/end station ids;
- фильтрация служебных и внешних станций;
- сохранение mapping в structured tables.

### 5.4. Weather

Погодные данные берутся из Open-Meteo:

- archive API для train/test;
- forecast API / fallback logic для inference.

В MVP это осознанный train/serving skew: обучение использует фактическую историческую погоду, inference использует прогноз или fallback weather values.

---

## 6. Airflow DAGs

### 6.1. `gbfs_ingestion_bridge`

DAG синхронизирует raw GBFS данные со старого raw ingestion server на MLOps server и строит online features.

Цепочка задач:

```text
sync_gbfs_raw
→ check_upstream_freshness
→ parse_station_information
→ parse_station_status
→ build_gbfs_online_features
→ evaluate_prediction_log_online
```

Логика:

1. синхронизировать latest/raw GBFS snapshots;
2. проверить свежесть upstream collector;
3. распарсить `station_information`;
4. распарсить `station_status`;
5. собрать `gbfs_online_features`;
6. оценить созревшие прогнозы из `prediction_log`.

### 6.2. `monthly_tripdata_ingestion`

DAG отвечает за ежемесячное переобучение lifecycle.

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

1. проверить появление нового monthly tripdata;
2. скачать новый файл, если он появился;
3. распарсить поездки в hourly demand tables;
4. скачать weather archive за тот же месяц;
5. пересобрать feature tables;
6. переоценить текущий champion на новом test split;
7. обучить LightGBM candidate;
8. принять решение promotion/reject через честный quality gate;
9. обучить XGBoost challenger.

---

## 7. Feature engineering

Основные feature groups:

```text
time features:
  hour_ny
  day_of_week_ny
  is_weekend_ny
  month_ny

station metadata:
  lat
  lon
  capacity

weather features:
  temperature
  precipitation
  wind speed / weather-related fields

online state:
  current_bikes
  current_docks
  state_age_seconds
```

Временные признаки считаются в timezone станции:

```text
America/New_York
```

Это важно, потому что рабочая/server timezone может отличаться от timezone Citi Bike station data.

---

## 8. Модели и MLflow Registry

Основная production-модель:

```text
bikeml_delta_bikes_forecaster
```

MLflow aliases:

| Alias | Назначение |
|---|---|
| `@champion` | production-модель для serving |
| `@candidate` | новая LightGBM-модель после ежемесячное переобучение |
| `@challenger` | XGBoost-модель для сравнения устойчивости |

Текущая production-постановка:

```text
target = delta_bikes_1h
features = hour, day_of_week, is_weekend, month, lat, lon, capacity, weather
```

Trip-lag модель сохранена только как offline benchmark / upper bound и не используется в serving, потому что её lag-фичи недоступны в live inference.

---

## 9. Quality gate

Candidate-модель не становится champion автоматически.

При появлении нового месяца:

1. текущий `@champion` переоценивается на новом test split;
2. новая LightGBM candidate обучается и оценивается на том же test split;
3. candidate получает promotion только если:

```text
candidate_MAE <= champion_MAE_on_same_test_df * 0.99
AND candidate_MAE < baseline_MAE
```

Если условие не выполнено:

```text
@champion остаётся прежним
@candidate сохраняется как отклонённая версия
```

Rollback выполняется переключением MLflow alias:

```text
@champion -> previous stable version
```

Это позволяет быстро вернуть production API на предыдущую стабильную модель без переобучения и без пересборки сервиса.

---

## 10. FastAPI serving

FastAPI реализует online serving layer.

Endpoints:

```text
GET  /health
GET  /docs
POST /predict/station
POST /predict/batch
GET  /metrics
```

### 10.1. `/health`

Проверяет:

- доступность PostgreSQL;
- статус загруженной MLflow champion-модели;
- текущий model alias/version.

Пример:

```bash
curl -s http://127.0.0.1:8000/health | python3 -m json.tool
```

### 10.2. `/predict/station`

Принимает station id и horizon, возвращает прогноз доступности.

Пример:

```bash
curl -s -X POST http://127.0.0.1:8000/predict/station \
  -H "Content-Type: application/json" \
  -d '{"legacy_station_id":"6879.04","horizon_minutes":60}' \
  | python3 -m json.tool
```

Endpoint возвращает:

```text
current_bikes
current_docks
capacity
predicted_delta_bikes_1h
predicted_bikes_raw
predicted_bikes_clipped
low_availability_risk
stale_state
model_name
model_version
mlflow_run_id
weather_source
predicted_at
target_time
prediction_log_id
```

### 10.3. `/predict/batch`

Позволяет получить прогноз сразу для нескольких станций.

### 10.4. `/metrics`

Endpoint Prometheus metrics, добавленный через `prometheus-fastapi-instrumentator`.

---

## 11. Prediction logging и online-оценку прогнозов

Каждый прогноз записывается в PostgreSQL:

```text
prediction_log
```

Таблица хранит:

- prediction timestamp;
- target timestamp;
- station id and station name;
- current bikes and docks;
- predicted delta;
- predicted bikes raw and clipped;
- model version and MLflow run ID;
- stale state flag;
- actual later bikes and docks;
- availability error;
- evaluation timestamp.

Online evaluation script:

```text
scripts/evaluate_prediction_log_online.py
```

Он сопоставляет созревшие прогнозы с фактическими future GBFS snapshots и обновляет:

```text
actual_bikes
actual_docks
availability_error
evaluated_at
```

Эти данные используются в Grafana для модельного и бизнес-мониторинга.

---

## 12. CI/CD

### 12.1. CI

GitHub Actions workflow:

```text
.github/workflows/ci.yml
```

CI запускается на push и проверяет:

- Python syntax для API, scripts и Airflow DAGs;
- Docker Compose config;
- наличие ключевых файлов.

### 12.2. CD

GitHub Actions workflow:

```text
.github/workflows/deploy.yml
```

Deploy запускается вручную:

```text
Actions → bikeml deploy → Run workflow
```

Используется self-hosted GitHub Actions runner на MLOps-сервере.

Это позволяет выполнять deploy через GitHub Actions без открытия SSH для GitHub-hosted runners.

Deploy workflow:

- выполняет checkout repository;
- синхронизирует код в `/home/deploy/mlops-stack`;
- копирует monitoring config;
- создаёт и готовит data directories для Prometheus/Grafana;
- исправляет права на persistent monitoring volumes;
- валидирует Python files и Docker Compose config;
- пересобирает code images;
- перезапускает API, Airflow и monitoring services;
- проверяет `/health`, `/metrics`, Prometheus health и Grafana health.

---

## 13. Monitoring

Проект включает полноценный Prometheus/Grafana monitoring stack.

### 13.1. Monitoring components

| Компонент | Роль |
|---|---|
| Prometheus | Сбор метрик |
| Grafana | Dashboard |
| prometheus-fastapi-instrumentator | FastAPI `/metrics` |
| Blackbox exporter | HTTP health probes |
| Node exporter | Метрики сервера |
| cAdvisor | Метрики Docker containers |
| Postgres exporter | Метрики PostgreSQL |

### 13.2. Prometheus targets

Prometheus scrapes:

```text
bikeml-api
prometheus
blackbox-http
node-exporter
cadvisor
postgres-exporter
```

На странице Prometheus Targets все основные targets находятся в состоянии `UP`.

### 13.3. Grafana dashboard

Dashboard:

```text
BikeML MLOps Monitoring
```

Dashboard покрывает три уровня мониторинга.

#### Технические / инфраструктурные метрики

- all service probes healthy;
- all Prometheus targets up;
- PostgreSQL up;
- server memory available;
- API request rate;
- API p95 latency;
- top container memory usage;
- top container CPU usage;
- PostgreSQL active connections;
- max scrape duration.

#### Модельные метрики

- latest offline MAE;
- latest offline bias;
- online availability MAE;
- offline MAE history;
- recent model evaluations.

#### Бизнесовые / операционные метрики

- logged predictions;
- low availability risk count;
- low availability risk share;
- latest prediction age;
- latest evaluation age;
- evaluation coverage.

### 13.4. Доступ к Grafana

Grafana доступна для просмотра:

```text
http://128.140.1.182:3000
```

Anonymous access включён только с ролью:

```text
Viewer
```

Prometheus наружу не открыт и остаётся доступен через localhost/tunnel.

Для локального доступа через SSH tunnel:

```bash
ssh -i ~/.ssh/id_ed25519_mlops_exam \
  -L 3000:127.0.0.1:3000 \
  -L 9090:127.0.0.1:9090 \
  deploy@128.140.1.182
```

После открытия туннеля:

```text
Grafana:    http://127.0.0.1:3000
Prometheus: http://127.0.0.1:9090
```

---

## 14. SLI/SLO и управление рисками

Документ:

```text
reports/sli_slo.md
```

SLI/SLO определены на трёх уровнях:

### 14.1. Технический уровень

Примеры:

- `/health` success rate >= 99%;
- `/predict/station` p95 latency <= 2 seconds;
- GBFS data age <= 900 seconds;
- Airflow DAG success rate >= 95%;
- Prometheus targets up = 100%;
- server memory available > 1 GB.

### 14.2. Модельный уровень

Примеры:

- candidate MAE лучше baseline и проходит fair gate;
- RMSE не растёт резко относительно champion;
- `abs(bias_delta) <= 0.25`;
- online MAE availability <= 5 bikes;
- evaluation coverage >= 90% для mature predictions.

### 14.3. Бизнес-уровень

Примеры:

- low availability share мониторится, critical threshold > 20%;
- recall low availability >= 0.70;
- stale prediction share <= 5%;
- latest prediction age контролируется.

---

## 15. ADR / MDD latency decision

Документ:

```text
adr/ADR-001-latency-serving-path.md
```

ADR содержит:

- decision question;
- H0/H1;
- latency samples для `/predict/station` и `/health`;
- descriptive statistics;
- p95 latency;
- Welch t-test;
- Mann–Whitney U test;
- p-value;
- итоговое архитектурное решение.

Ключевой вывод:

```text
/predict/station статистически медленнее /health,
но observed p95 ≈ 0.055 sec намного ниже SLO p95 <= 2 sec.
```

`/health` используется как минимальная baseline-точка latency инфраструктуры. Тест показывает стоимость полного prediction path относительно лёгкой проверки. Поскольку даже полный путь намного быстрее SLO, кэширование погоды и отдельный слой отдачи фичей не блокируют MVP и оставлены как будущие улучшения.

Поэтому текущий FastAPI serving path принят для MVP.

---

## 16. Переменные окружения

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

Ключевые monitoring переменные:

```env
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<secret>
PROMETHEUS_DATA_DIR=/mnt/mlops-data/prometheus
GRAFANA_DATA_DIR=/mnt/mlops-data/grafana
```

---

## 17. Проверки

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

### Проверить FastAPI

```bash
curl -s http://127.0.0.1:8000/health | python3 -m json.tool
```

```bash
curl -s http://127.0.0.1:8000/metrics | head
```

### Проверить Prometheus / Grafana

```bash
curl -s http://127.0.0.1:9090/-/healthy
curl -s http://127.0.0.1:3000/api/health
```

---

## 18. Teardown / деинсталляция

Перед удалением инфраструктуры желательно остановить self-hosted GitHub Actions runner, чтобы в GitHub не остался висящий offline-runner.

На MLOps-сервере:

```bash
cd ~/actions-runner
sudo ./svc.sh stop
sudo ./svc.sh uninstall
```

Если нужно полностью разрегистрировать runner в GitHub:

```bash
./config.sh remove --token <GITHUB_RUNNER_REMOVE_TOKEN>
```

После этого можно удалить Terraform-инфраструктуру с локальной машины из папки Terraform:

```bash
terraform destroy
```

Важно:

- `terraform destroy` удаляет MLOps-сервер и volume, созданные Terraform;
- Persistent Raw Ingestion Server не управляется Terraform и не удаляется;
- raw GBFS snapshots на старом ingestion-сервере сохраняются;
- если runner не разрегистрировать заранее, его можно удалить вручную в GitHub: `Settings → Actions → Runners`.

---

## 19. Known limitations

Ограничения MVP:

1. Feast feature store вынесен в stretch; MVP использует feature tables в PostgreSQL.
2. Evidently drift reports и automatic alerting не реализованы.
3. Ребалансировка не моделируется, так как нет данных о действиях оператора.
4. Погода в train берётся из archive API, а в inference используется forecast/fallback logic.
5. Production SLA, multi-region и отказоустойчивость коммерческого уровня не входят в MVP.
6. Anonymous Grafana access включён для проверки и должен быть отключён после проверки.

---

## 20. Текущий статус

Реализовано:

```text
Terraform provisioning
Docker Compose MLOps stack
PostgreSQL / MinIO / MLflow / Airflow / Redis
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
FastAPI serving
prediction_log
online prediction evaluation
Prometheus/Grafana monitoring
technical/model/business Grafana dashboard
SLI/SLO documentation
ADR/MDD latency decision with statistical test
GitHub Actions CI
GitHub Actions CD through self-hosted runner
```

Подтверждён живой ежемесячное переобучение scenario:

```text
202605-citibike-tripdata.zip появился на S3
→ DAG скачал файл
→ распарсил май
→ скачал weather archive за май
→ пересобрал delta features
→ обучил candidate/challenger
→ выполнил fair quality gate
```

Проект демонстрирует MLOps maturity level 2: автоматизированный pipeline, registry, quality gate, serving, logging, monitoring, CI/CD и документированные SLI/SLO/ADR.

---

## 21. Возможные дальнейшие улучшения

Возможные расширения после MVP:

1. Feast feature store integration;
2. автоматическое provisioning Grafana dashboard из JSON;
3. Nginx reverse proxy with TLS;
4. Prometheus/Grafana alerting rules;
5. Evidently drift reports;
6. более подробные бизнес-KPI для операционных решений по ребалансировке;
7. оптимизированный batch inference;
8. кэширование прогноза погоды.
