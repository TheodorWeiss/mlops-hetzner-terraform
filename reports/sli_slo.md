# SLI/SLO и управление рисками для BikeML MLOps Stack

## 1. Назначение документа

Этот документ описывает систему SLI/SLO для учебного MLOps-проекта BikeML: прогнозирования доступности велосипедов на станциях Citi Bike на горизонте 1 час.

Система прогнозирует изменение количества велосипедов:

```text
delta_bikes_1h = returns_1h - departures_1h
predicted_bikes_1h = current_bikes_now + predicted_delta_bikes_1h
```

Основной продуктовый результат:

```text
predicted_bikes_1h
low_availability_risk = predicted_bikes_1h <= threshold
```

SLI/SLO разделены на три уровня:

1. технический уровень;
2. модельный уровень;
3. бизнес-уровень.

---

## 2. Технический уровень

Технический уровень отвечает на вопрос: работает ли инфраструктура достаточно стабильно, чтобы система могла принимать данные, обучать модели и отдавать прогнозы.

### 2.1. SLI: доступность FastAPI

**SLI:** доля успешных ответов `/health`.

```text
api_health_success_rate = successful_health_checks / total_health_checks
```

**SLO:**

```text
api_health_success_rate >= 99%
```

**Критический порог:**

```text
api_health_success_rate < 95%
```

**Действие при нарушении:**

- проверить контейнер `bikeml-api`;
- проверить доступность PostgreSQL;
- проверить загрузку MLflow champion-модели;
- при необходимости перезапустить API-сервис.

---

### 2.2. SLI: latency API

**SLI:** p95 latency для `/predict/station`.

```text
predict_station_latency_p95_seconds
```

**SLO:**

```text
p95 latency <= 2 seconds
```

**Критический порог:**

```text
p95 latency > 5 seconds
```

**Риск:**

Высокая latency может возникать из-за задержек PostgreSQL, внешнего weather forecast API или проблем с контейнером FastAPI.

**Действие при нарушении:**

- проверить логи `bikeml-api`;
- проверить latency обращения к Open-Meteo;
- рассмотреть кэширование weather forecast;
- рассмотреть предварительную загрузку фичей и batch-предсказания.

---

### 2.3. SLI: свежесть live GBFS данных

**SLI:** возраст последнего live-снепшота.

```text
data_age_seconds
```

**SLO:**

```text
data_age_seconds <= 900 seconds
```

То есть данные должны быть не старше 15 минут.

**Критический порог:**

```text
data_age_seconds > 1800 seconds
```

То есть данные старше 30 минут считаются критически устаревшими.

**Действие при нарушении:**

- проверить DAG `gbfs_ingestion_bridge`;
- проверить task `check_upstream_freshness`;
- проверить доступность Persistent Raw Ingestion Server;
- помечать ответы API флагом `stale_state = true`.

---

### 2.4. SLI: успешность Airflow DAG

**SLI:** доля успешных запусков ключевых DAG:

```text
gbfs_ingestion_bridge_success_rate
monthly_tripdata_ingestion_success_rate
```

**SLO:**

```text
success_rate >= 95%
```

**Критический порог:**

```text
success_rate < 90%
```

**Действие при нарушении:**

- проверить Airflow task logs;
- проверить доступность PostgreSQL, MinIO, MLflow;
- проверить наличие нового tripdata-файла;
- проверить права доступа к `/mnt/mlops-data`.

---

## 3. Модельный уровень

Модельный уровень отвечает на вопрос: остаётся ли ML-модель достаточно качественной и не ухудшается ли качество прогнозов.

### 3.1. SLI: offline MAE по delta_bikes

**SLI:** MAE на test split для целевой переменной `delta_bikes_1h`.

```text
mae_delta = mean(abs(predicted_delta_bikes - actual_delta_bikes))
```

**SLO:**

```text
candidate_mae_delta < baseline_mae_delta
```

Также candidate может стать champion только если выполняется честный quality gate:

```text
candidate_mae <= champion_mae_on_same_test_df * 0.99
AND candidate_mae < baseline_mae
```

**Критический порог:**

```text
candidate_mae >= baseline_mae
```

**Действие при нарушении:**

- не переключать `@champion`;
- сохранить новую модель как `@candidate`;
- оставить текущий stable champion;
- проверить качество новых данных и weather coverage.

---

### 3.2. SLI: RMSE по delta_bikes

**SLI:** RMSE на test split.

```text
rmse_delta = sqrt(mean((predicted_delta_bikes - actual_delta_bikes)^2))
```

**SLO:**

```text
rmse_delta не должен резко расти относительно предыдущего champion на том же test_df
```

**Критический порог:**

```text
rmse_delta > champion_rmse_on_same_test_df * 1.10
```

**Действие при нарушении:**

- не промоутить candidate;
- проверить крупные ошибки на отдельных станциях;
- проверить наличие выбросов в новом месяце.

---

### 3.3. SLI: bias модели

**SLI:** средняя ошибка со знаком.

```text
bias_delta = mean(predicted_delta_bikes - actual_delta_bikes)
```

**SLO:**

```text
abs(bias_delta) <= 0.25
```

**Критический порог:**

```text
abs(bias_delta) > 0.5
```

**Риск:**

Систематический положительный bias может быть опаснее случайной ошибки: система будет предсказывать больше велосипедов, чем реально есть на станции.

**Действие при нарушении:**

- не промоутить модель без ручной проверки;
- проверить bias по районам, часам и типам станций;
- проверить weather features и сезонность.

---

### 3.4. SLI: online availability error

**SLI:** ошибка итогового прогноза наличия велосипедов.

```text
availability_error = predicted_bikes_clipped - actual_bikes
```

Основные online-метрики:

```text
mae_availability = mean(abs(availability_error))
rmse_availability = sqrt(mean(availability_error^2))
bias_availability = mean(availability_error)
```

**SLO:**

```text
mae_availability <= 5 bikes на скользящем окне 24 часа
```

**Критический порог:**

```text
mae_availability > 8 bikes
```

**Действие при нарушении:**

- проверить свежесть GBFS;
- проверить prediction_log;
- проверить, не изменилось ли поведение станций;
- дождаться следующего monthly retraining или запустить retraining вручную.

---

## 4. Бизнес-уровень

Бизнес-уровень отвечает на вопрос: помогает ли система заранее выявлять станции с риском низкой доступности.

### 4.1. SLI: доля станций с low availability risk

**SLI:** доля станций, для которых прогнозируется низкое количество велосипедов.

```text
low_availability_share = stations_with_predicted_bikes_below_threshold / total_predicted_stations
```

**SLO:**

```text
low_availability_share мониторится и не должен резко расти относительно обычного уровня
```

**Критический порог:**

```text
low_availability_share > 20%
```

**Действие при нарушении:**

- проверить, не устарели ли live GBFS данные;
- проверить weather forecast;
- проверить массовые события или сбои в данных;
- пометить ситуацию как business alert.

---

### 4.2. SLI: качество предсказания low availability

На основе online-факта из будущих GBFS-снепшотов можно построить бинарную метрику:

```text
predicted_low = predicted_bikes_clipped <= threshold
actual_low = actual_bikes <= threshold
```

**SLI:**

```text
precision_low_availability
recall_low_availability
f1_low_availability
```

**SLO:**

```text
recall_low_availability >= 0.70
```

**Критический порог:**

```text
recall_low_availability < 0.50
```

**Действие при нарушении:**

- пересмотреть threshold;
- проверить модельные ошибки на станциях с реальным дефицитом;
- проверить достаточность online-фичей и weather features.

---

### 4.3. SLI: доля stale-прогнозов

**SLI:** доля прогнозов, построенных на устаревшем live-состоянии станции.

```text
stale_prediction_share = predictions_with_stale_state_true / total_predictions
```

**SLO:**

```text
stale_prediction_share <= 5%
```

**Критический порог:**

```text
stale_prediction_share > 15%
```

**Действие при нарушении:**

- проверить upstream collector;
- проверить Airflow GBFS ingestion;
- проверить `data_age_seconds`;
- не использовать такие прогнозы для операционных решений без ручной проверки.

---

## 5. Quality gate и rollback

Promotion новой модели выполняется только через quality gate.

Новая LightGBM candidate-модель становится champion только если она:

```text
candidate_mae <= champion_mae_on_same_test_df * 0.99
AND candidate_mae < baseline_mae
```

Если условие не выполнено:

```text
@champion остаётся прежним
@candidate сохраняется как отклонённая версия
```

Rollback выполняется через MLflow alias:

```text
@champion -> previous stable version
```

Это позволяет быстро вернуть production API на предыдущую стабильную модель без переобучения и без пересборки сервиса.

---

## 6. Таблица summary

| Уровень | SLI | SLO | Критический порог |
|---|---|---|---|
| Технический | `/health` success rate | >= 99% | < 95% |
| Технический | `/predict/station` p95 latency | <= 2 sec | > 5 sec |
| Технический | GBFS data age | <= 900 sec | > 1800 sec |
| Технический | Airflow DAG success rate | >= 95% | < 90% |
| Модельный | MAE delta | better than baseline and fair gate | worse than baseline |
| Модельный | RMSE delta | stable vs champion | > 110% of champion RMSE |
| Модельный | abs(bias_delta) | <= 0.25 | > 0.5 |
| Модельный | online MAE availability | <= 5 bikes | > 8 bikes |
| Бизнес | low availability share | monitored | > 20% |
| Бизнес | recall low availability | >= 0.70 | < 0.50 |
| Бизнес | stale prediction share | <= 5% | > 15% |

---

## 7. Текущий статус реализации SLI/SLO

Реализовано:

```text
/health endpoint
FastAPI healthcheck в docker-compose
prediction_log
online evaluation script
GBFS freshness check
model_evaluation_runs
MLflow aliases champion/candidate/challenger
fair quality gate
```

В работе / stretch:

```text
агрегация online metrics по скользящим окнам
Prometheus/Grafana dashboard
Evidently drift reports
automatic alerting
```

Для MVP достаточно, что SLI/SLO определены, ключевые метрики логируются в PostgreSQL, а критические пороги и действия при нарушении задокументированы.
