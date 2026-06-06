# ADR-001: Решение по latency для serving path BikeML

## Статус

Принято для MVP.

---

## Контекст

BikeML - учебный MLOps-проект для прогнозирования доступности велосипедов на станциях Citi Bike на горизонте 1 час.

Serving API предоставляет endpoints:

```text
GET  /health
POST /predict/station
POST /predict/batch
GET  /metrics
```

В публичной схеме доступа запросы идут через nginx:

```text
Internet
-> nginx, port 80
-> FastAPI
```

Публичные endpoints для MVP:

```text
GET  http://128.140.1.182/health
POST http://128.140.1.182/api/predict/station
```

Endpoint `/api/predict/station` на nginx проксируется во внутренний FastAPI endpoint `/predict/station`.

FastAPI напрямую наружу не открыт. Прямой доступ к приложению используется только локально на сервере через `127.0.0.1:8000`.

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

Endpoint `/health` является лёгким техническим endpoint. Он нужен для проверки доступности сервиса, базы данных и статуса загрузки модели. Он не выполняет полный prediction path и не является бизнес-альтернативой endpoint `/predict/station`.

Для MVP нужно решить, приемлема ли latency полного prediction path или перед релизом нужно менять serving architecture: сильнее кэшировать weather features, убирать внешние вызовы или вводить отдельный feature-serving слой.

---

## Вопрос архитектурного решения

Можно ли оставить текущий полный prediction path для MVP serving при заданном latency SLO?

MVP SLO для prediction latency:

```text
p95 latency <= 2 seconds
```

---

## Гипотезы и критерий принятия решения

В этой версии ADR архитектурное решение принимается не по сравнению с `/health`, а по соответствию prediction endpoint принятому SLO.

### Нулевая гипотеза H0

Полный prediction path не укладывается в latency SLO.

```text
H0: p95(predict) >= 2.0 sec
```

### Альтернативная гипотеза H1

Полный prediction path укладывается в latency SLO.

```text
H1: p95(predict) < 2.0 sec
```

Для учебного MVP используется практическое one-sided decision rule:

```text
если наблюдаемый p95 для /predict/station меньше 2 секунд с большим запасом,
то текущий serving path принимается для MVP.
```

Endpoint `/health` остаётся в ADR только как diagnostic baseline. Он показывает нижнюю границу технической latency приложения, но не является объектом архитектурного решения.

---

## Сбор данных

Latency измерялась локально на MLOps-сервере с помощью `curl`.

Замеры выполнялись на FastAPI port `127.0.0.1:8000`, то есть напрямую на приложении, без публичного nginx path. Для данного решения это допустимо: nginx добавляет небольшой локальный proxy-overhead, а наблюдаемая latency prediction endpoint имеет очень большой запас относительно SLO `2 sec`.

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

| Endpoint           |  n | Mean, sec | Median, sec | p95, sec | Max, sec |
| ------------------ | -: | --------: | ----------: | -------: | -------: |
| `/predict/station` | 30 |    0.0449 |      0.0430 |   0.0551 |   0.0600 |
| `/health`          | 30 |    0.0079 |      0.0071 |   0.0122 |   0.0142 |

Наблюдаемый результат для основного prediction endpoint:

```text
/predict/station p95 latency ≈ 0.055 sec
```

Принятый SLO:

```text
p95 latency <= 2 sec
```

Запас относительно SLO:

```text
2.0 / 0.055 ≈ 36x
```

Даже максимальное наблюдение в выборке остаётся намного ниже SLO:

```text
max observed latency ≈ 0.060 sec
```

---

## Диагностическое сравнение с `/health`

Дополнительно были выполнены два теста для сравнения `/predict/station` и `/health`:

1. Welch's t-test для сравнения средних;
2. Mann-Whitney U test как непараметрическая проверка.

Результаты:

```text
Welch t-test p-value ≈ 9.85e-24
Mann-Whitney U test p-value ≈ 1.51e-11
alpha = 0.05
```

Эти тесты показывают, что полный prediction path статистически значимо медленнее лёгкого `/health`.

Это ожидаемый результат и сам по себе не является проблемой. `/health` не читает полный набор features, не выполняет полноценный inference path и не записывает прогноз в `prediction_log`.

Поэтому сравнение с `/health` используется только как диагностика дополнительной стоимости prediction path. Архитектурное решение принимается по SLO для `/predict/station`.

---

## Интерпретация

Главный вопрос ADR: нарушает ли полный prediction path latency SLO для MVP?

Ответ: нет.

```text
observed p95 ≈ 0.055 sec
SLO p95 <= 2 sec
```

Наблюдаемая p95 latency примерно в 36 раз ниже принятого SLO. Поэтому на текущем этапе нет необходимости блокировать MVP из-за отдельного feature-serving слоя, предварительного расчёта всех weather features или дополнительного кэширования.

При этом результат не означает, что оптимизация больше не нужна никогда. Он означает только то, что для MVP текущий serving path достаточно быстрый и понятный.

---

## Решение

Для MVP принимается текущая serving architecture:

```text
client request
-> nginx
-> FastAPI
-> PostgreSQL online features
-> weather features
-> MLflow @champion model
-> prediction_log
-> response
```

Решение:

```text
Сохранить текущий полный prediction path для MVP.
Считать latency /predict/station соответствующей MVP SLO.
Не блокировать MVP из-за weather caching или отдельного feature-serving слоя.
Отслеживать latency как SLI в Prometheus/Grafana.
```

Переключение модели в production path выполняется через MLflow model alias `@champion`. Это позволяет заменить активную модель без изменения публичного API endpoint.

---

## Trigger rule после MVP

После запуска monitoring stack latency decision поддерживается live-метриками:

```text
FastAPI /metrics
Prometheus scrape job bikeml-api
Grafana panel api p95 latency
Grafana panel api request rate
```

Если p95 latency для prediction endpoint на скользящем окне превышает MVP SLO, нужно активировать optimisation plan.

Базовое правило:

```text
если p95 latency /predict/station > 2 sec на скользящем окне мониторинга,
то текущий serving path больше не считается достаточным и требуется оптимизация.
```

Возможные меры:

1. кэшировать weather forecast values;
2. предварительно рассчитывать weather features на несколько часов вперёд;
3. вынести online features в отдельный feature-serving слой;
4. оптимизировать batch inference через единый DataFrame;
5. проверить MLflow model loading и Python dependency mismatch warnings.

---

## Последствия

### Положительные последствия

* Serving layer остаётся простым и понятным.
* API использует тот же feature contract, что и модель.
* Endpoint записывает прогнозы в `prediction_log`.
* Наблюдаемая latency намного ниже MVP SLO.
* Архитектура достаточна для демонстрации MLOps maturity level 2.
* nginx становится единственной публичной точкой входа, а FastAPI не открыт напрямую наружу.

### Отрицательные последствия

* `/predict/station` зависит от weather forecast logic.
* Если внешний weather API будет медленным или недоступным, latency может вырасти.
* Batch endpoint сейчас вызывает station prediction последовательно и не оптимизирован.
* Dependency mismatch warnings от MLflow environment желательно устранить позже.

### Снижение рисков

Текущий API включает резервную логику погоды:

```text
weather_source = fallback_neutral_weather
```

Если внешний forecast call не срабатывает, endpoint может вернуть прогноз с нейтральными weather values.

Кроме того, после MVP latency контролируется через Prometheus/Grafana. Если p95 latency начнёт приближаться к SLO, оптимизация serving path станет обязательной задачей, а не просто future improvement.

---

## Финальный вывод

Latency experiment показывает, что полный prediction endpoint ожидаемо медленнее лёгкого `/health`, но это не является основанием для redesign.

Архитектурное решение принимается по SLO для `/predict/station`:

```text
observed p95 ≈ 0.055 sec
SLO p95 <= 2 sec
```

С учётом большого запаса относительно SLO текущий FastAPI serving path через nginx принимается для MVP. Weather caching, отдельный feature-serving слой и оптимизированный batch inference остаются задокументированными улучшениями на случай роста latency или нагрузки.
