# ADR-001: Latency decision for BikeML serving path

## Status

Accepted for MVP.

## Context

BikeML is an educational MLOps project for forecasting Citi Bike station availability on a 1-hour horizon.

The serving API exposes:

```text
GET  /health
POST /predict/station
POST /predict/batch
```

The main prediction endpoint builds the final forecast as:

```text
predicted_bikes_1h = current_bikes_now + predicted_delta_bikes_1h
predicted_bikes_1h = clip(predicted_bikes_1h, 0, capacity)
```

The endpoint `/predict/station` performs a full production-like prediction path:

1. reads the latest online station features from PostgreSQL;
2. calls Open-Meteo forecast API for weather features;
3. loads and uses the MLflow `@champion` model;
4. computes `predicted_delta_bikes_1h`;
5. computes clipped bike availability;
6. writes the request and prediction into `prediction_log`.

The endpoint `/health` is a lightweight technical endpoint. It checks database connectivity and model load status, but does not execute the full prediction path.

For the MVP, the question is whether the full prediction path is still fast enough to be acceptable for serving, despite being slower than a lightweight endpoint.

---

## Decision question

Should the current full prediction path be accepted for MVP serving, or should we first redesign the API to remove external calls and precompute/cache more features?

---

## Hypotheses

### Null hypothesis H0

The full prediction path does not have higher latency than the lightweight health endpoint.

```text
H0: latency_predict <= latency_health
```

### Alternative hypothesis H1

The full prediction path has higher latency than the lightweight health endpoint.

```text
H1: latency_predict > latency_health
```

This is expected because `/predict/station` performs database reads, weather feature retrieval, model inference and logging, while `/health` performs only lightweight checks.

---

## Data collection

Latency was measured locally on the MLOps server with `curl`.

### Prediction endpoint

Command:

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

Command:

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

## Descriptive statistics

| Endpoint | n | Mean, sec | Median, sec | p95, sec | Max, sec |
|---|---:|---:|---:|---:|---:|
| `/predict/station` | 30 | 0.0449 | 0.0430 | 0.0551 | 0.0600 |
| `/health` | 30 | 0.0079 | 0.0071 | 0.0122 | 0.0142 |

The full prediction path is slower than `/health`, but the absolute latency is still low.

The MVP SLO for prediction latency is:

```text
p95 latency <= 2 seconds
```

Observed result:

```text
/predict/station p95 latency ≈ 0.055 seconds
```

This is far below the SLO threshold.

---

## Statistical test

Two statistical tests were used:

1. Welch's t-test for difference in means;
2. Mann-Whitney U test as a non-parametric robustness check.

Results:

```text
Welch t-test p-value ≈ 9.85e-24
Mann-Whitney U test p-value ≈ 1.51e-11
```

At significance level:

```text
alpha = 0.05
```

both tests reject H0.

---

## Interpretation

The full prediction path has statistically higher latency than the lightweight `/health` endpoint.

This is expected and not itself a problem. The relevant architectural question is whether this higher latency violates the serving SLO.

It does not:

```text
observed p95 ≈ 0.055 sec
SLO p95 <= 2 sec
```

The observed prediction latency is much lower than the accepted threshold for the MVP.

---

## Decision

For the MVP, we accept the current FastAPI serving architecture:

```text
client request
→ FastAPI
→ PostgreSQL online features
→ Open-Meteo forecast features
→ MLflow @champion model
→ prediction_log
→ response
```

The decision is:

```text
Keep the current full prediction path for MVP.
Do not block the project on weather caching or a dedicated feature-serving layer.
Track latency as an SLI and treat caching as a future optimization.
```

---

## Consequences

### Positive consequences

- The serving layer is simple and understandable.
- The API uses the same feature contract as the model.
- The endpoint already logs predictions into `prediction_log`.
- The observed latency is far below the MVP SLO.
- The architecture is sufficient for demonstrating MLOps maturity level 2.

### Negative consequences

- `/predict/station` depends on an external weather forecast API.
- If Open-Meteo is slow or unavailable, latency can increase.
- The current batch endpoint calls station prediction sequentially and is not optimized.
- Dependency mismatch warnings from MLflow environment should be resolved later.

### Risk mitigation

The current API includes fallback weather logic:

```text
weather_source = fallback_neutral_weather
```

If the external forecast call fails, the endpoint can still return a prediction using neutral weather values.

Future improvements:

1. cache Open-Meteo forecast values;
2. precompute weather features for the next several hours;
3. batch model inference with a single DataFrame;
4. align API Python dependencies with MLflow model dependencies;
5. add Prometheus/Grafana latency dashboards.

---

## Final conclusion

The latency experiment shows that the full prediction endpoint is statistically slower than the lightweight health endpoint, but its absolute latency remains far below the MVP SLO.

Therefore, the current FastAPI serving path is accepted for MVP, while weather caching and optimized batch inference are documented as future improvements.
