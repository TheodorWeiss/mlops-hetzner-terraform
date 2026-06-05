# ADR-001: Решение по latency для serving path BikeML

## Статус

Принято для MVP.

---

## Контекст

BikeML — учебный MLOps-проект для прогнозирования доступности велосипедов на станциях Citi Bike на горизонте 1 час.

Serving API предоставляет endpoints:

```text
GET  /health
POST /predict/station
POST /predict/batch
GET  /metrics
```

Основной prediction endpoint строит итоговый прогноз так:

```text
predicted_bikes_1h = current_bikes_now + predicted_delta_bikes_1h
predicted_bikes_1h = clip(predicted_bikes_1h, 0, capacity)
```

Endpoint `/predict/station` выполняет полный production-like prediction path:

1. читает последние online station features из PostgreSQL;
2. получает признаки прогноза погоды;
3. использует MLflow `@champion` model;
4. вычисляет `predicted_delta_bikes_1h`;
5. вычисляет clipped bike availability;
6. записывает запрос и прогноз в `prediction_log`;
7. возвращает прогноз и operational flags.

Endpoint `/health` является лёгким техническим endpoint. Он проверяет доступность базы данных и статус загрузки модели, но не выполняет полный prediction path.

Для MVP нужно решить, приемлема ли latency полного prediction path или необходимо сначала переработать API: убрать внешние вызовы, сильнее кэшировать weather features или вводить отдельный feature-serving слой.

---

## Вопрос архитектурного решения

Принимаем ли текущий полный prediction path для MVP serving или нужно сначала redesign API для уменьшения latency?

---

## Гипотезы

### Нулевая гипотеза H0

Полный prediction path не имеет большей latency, чем лёгкий health endpoint.

```text
H0: latency_predict <= latency_health
```

### Альтернативная гипотеза H1

Полный prediction path имеет большую latency, чем лёгкий health endpoint.

```text
H1: latency_predict > latency_health
```

Ожидается, что `/predict/station` будет медленнее, потому что он выполняет чтение features, model inference и запись в `prediction_log`, тогда как `/health` выполняет только лёгкие проверки.

### Почему baseline сравнивается с `/health`

В этом ADR `/health` используется не как полноценная альтернатива бизнес-serving, а как минимальная baseline-точка latency инфраструктуры: FastAPI, сеть внутри сервера, базовая проверка БД и статуса модели.

Разница между `/predict/station` и `/health` показывает дополнительную стоимость полного prediction path: чтения online features, подготовки признаков, model inference и записи в `prediction_log`.

Для MVP этого достаточно для архитектурного решения: если даже полный prediction path значительно быстрее SLO, то дополнительные оптимизации вроде кэширования погоды, предварительного расчёта фичей или отдельного feature-serving слоя не блокируют релиз MVP и переносятся в future improvements.


---

## Сбор данных

Latency измерялась локально на MLOps-сервере с помощью `curl`.

### Prediction endpoint

Команда:

```bash
for i in $(seq 1 30); do
  curl -s -o /dev/null \
    -w "%{time_total}\n" \
    -X POST http://127.0.0.1:8000/predict/station \
    -H "Content-Type: application/json" \
    -d '{"legacy_station_id":"6879.04","horizon_minutes":60}' \
    >> /mnt/mlops-data/reports/latency/predict_station_latency.txt
done
```

Samples, seconds:

```text
0.059963
0.052650
0.053208
0.053228
0.055251
0.054947
0.053510
0.050591
0.041111
0.051130
0.044158
0.037933
0.038243
0.038360
0.036938
0.036879
0.036800
0.036942
0.036271
0.039170
0.034009
0.040374
0.046059
0.053699
0.045001
0.054612
0.041804
0.038530
0.039573
0.044799
```

### Health endpoint

Команда:

```bash
for i in $(seq 1 30); do
  curl -s -o /dev/null \
    -w "%{time_total}\n" \
    http://127.0.0.1:8000/health \
    >> /mnt/mlops-data/reports/latency/health_latency.txt
done
```

Samples, seconds:

```text
0.012350
0.014228
0.011923
0.008329
0.007636
0.006547
0.005928
0.005814
0.006755
0.006845
0.006652
0.006479
0.006816
0.007335
0.006507
0.006820
0.006409
0.006795
0.005754
0.006583
0.008124
0.010015
0.007550
0.007781
0.009361
0.009239
0.009024
0.006574
0.007359
0.008521
```

---

## Описательная статистика

| Endpoint | n | Mean, sec | Median, sec | p95, sec | Max, sec |
|---|---:|---:|---:|---:|---:|
| `/predict/station` | 30 | 0.0449 | 0.0430 | 0.0551 | 0.0600 |
| `/health` | 30 | 0.0079 | 0.0071 | 0.0122 | 0.0142 |

Полный prediction path медленнее `/health`, но абсолютная latency остаётся низкой.

MVP SLO для prediction latency:

```text
p95 latency <= 2 seconds
```

Наблюдаемый результат:

```text
/predict/station p95 latency ≈ 0.055 seconds
```

Это намного ниже SLO-порога.

---

## Статистический тест

Использованы два статистических теста:

1. Welch's t-test для сравнения средних;
2. Mann–Whitney U test как непараметрическая проверка устойчивости результата.

Результаты:

```text
Welch t-test p-value ≈ 9.85e-24
Mann-Whitney U test p-value ≈ 1.51e-11
```

Уровень значимости:

```text
alpha = 0.05
```

Оба теста отвергают H0.

---

## Интерпретация

Полный prediction path статистически значимо медленнее лёгкого `/health` endpoint.

Это ожидаемо и само по себе не является проблемой. Главный архитектурный вопрос: нарушает ли эта большая latency serving SLO.

Ответ: нет.

```text
observed p95 ≈ 0.055 sec
SLO p95 <= 2 sec
```

Наблюдаемая latency prediction endpoint намного ниже принятого порога для MVP.

---

## Решение

Для MVP принимается текущая FastAPI serving architecture:

```text
client request
→ FastAPI
→ PostgreSQL online features
→ признаки прогноза погоды
→ MLflow @champion model
→ prediction_log
→ response
```

Решение:

```text
Сохранить текущий полный prediction path для MVP.
Не блокировать проект из-за weather caching или отдельного слой отдачи фичей.
Отслеживать latency как SLI и рассматривать caching как future optimization.
```

---

## Последствия

### Положительные последствия

- Serving layer остаётся простым и понятным.
- API использует тот же feature contract, что и модель.
- Endpoint уже записывает прогнозы в `prediction_log`.
- Наблюдаемая latency намного ниже MVP SLO.
- Архитектура достаточна для демонстрации MLOps maturity level 2.

### Отрицательные последствия

- `/predict/station` зависит от weather forecast logic.
- Если внешний weather API будет медленным или недоступным, latency может вырасти.
- Batch endpoint сейчас вызывает station prediction последовательно и не оптимизирован.
- Dependency mismatch warnings от MLflow environment желательно устранить позже.

### Снижение рисков

Текущий API включает резервную логику погоды:

```text
weather_source = fallback_neutral_weather
```

Если внешний forecast call не срабатывает, endpoint может вернуть прогноз с нейтральными weather values.

Будущие улучшения:

1. кэшировать Open-Meteo forecast values;
2. предварительно рассчитывать weather features на несколько часов вперёд;
3. оптимизировать batch inference через единый DataFrame;
4. выровнять API Python dependencies с MLflow model dependencies;
5. добавить alerting rules для latency в Prometheus/Grafana.

---

## Мониторинг после принятия решения

После реализации monitoring stack latency decision поддерживается live-метриками:

```text
FastAPI /metrics
Prometheus scrape job bikeml-api
Grafana panel api p95 latency
Grafana panel api request rate
```

Таким образом, latency больше не является только разовым экспериментом: она отслеживается как технический SLI в Grafana dashboard.

---

## Финальный вывод

Latency experiment показывает, что полный prediction endpoint статистически медленнее лёгкого `/health`, но его абсолютная latency остаётся намного ниже MVP SLO.

Поэтому текущий FastAPI serving path принимается для MVP, а weather caching и оптимизированный batch inference задокументированы как будущие улучшения.
