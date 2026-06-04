from __future__ import annotations

import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import mlflow
import pandas as pd
import psycopg2
import requests
from fastapi import FastAPI, HTTPException
from mlflow.tracking import MlflowClient
from pydantic import BaseModel, Field


APP_NAME = "bikeml-api"
MODEL_NAME = "bikeml_delta_bikes_forecaster"
MODEL_ALIAS = "champion"
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_S3_ENDPOINT_URL = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")

NY_LAT = 40.7128
NY_LON = -74.0060
LOW_AVAILABILITY_THRESHOLD = int(os.getenv("LOW_AVAILABILITY_THRESHOLD", "1"))
STALE_STATE_SECONDS = int(os.getenv("STALE_STATE_SECONDS", "900"))

FEATURE_COLUMNS = [
    "hour_ny",
    "day_of_week_ny",
    "is_weekend_ny",
    "month_ny",
    "lat",
    "lon",
    "capacity",
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
    "is_rain",
]


os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", ""))
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", MLFLOW_S3_ENDPOINT_URL)
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")


class PredictStationRequest(BaseModel):
    gbfs_station_id: Optional[str] = None
    legacy_station_id: Optional[str] = None
    horizon_minutes: int = Field(default=60, ge=1, le=180)


class PredictStationResponse(BaseModel):
    gbfs_station_id: str
    legacy_station_id: Optional[str]
    station_name: Optional[str]
    horizon_minutes: int
    current_bikes: Optional[int]
    current_docks: Optional[int]
    capacity: Optional[int]
    predicted_delta_bikes_1h: float
    predicted_bikes_raw: float
    predicted_bikes_clipped: float
    low_availability_risk: bool
    threshold: int
    stale_state: bool
    state_age_seconds: Optional[int]
    model_name: str
    model_alias: str
    model_version: str
    mlflow_run_id: Optional[str]
    weather_source: str
    predicted_at: str
    target_time: str
    prediction_log_id: Optional[int]


app = FastAPI(title="BikeML Citi Bike Availability API", version="0.1.0")

_model = None
_model_version = None
_model_run_id = None


def getenv_first(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def get_connection():
    return psycopg2.connect(
        host=getenv_first("POSTGRES_HOST", "PGHOST", default="postgres"),
        port=getenv_first("POSTGRES_PORT", "PGPORT", default="5432"),
        dbname=getenv_first("BIKEML_DB", "POSTGRES_DB", "PGDATABASE", default="bikeml"),
        user=getenv_first("POSTGRES_USER", "PGUSER", default="bikeml_admin"),
        password=getenv_first("POSTGRES_PASSWORD", "PGPASSWORD"),
    )


def load_champion_model() -> None:
    global _model, _model_version, _model_run_id

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient(MLFLOW_TRACKING_URI)
    mv = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)

    _model_version = str(mv.version)
    _model_run_id = mv.run_id
    _model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@{MODEL_ALIAS}")


@app.on_event("startup")
def startup_event() -> None:
    load_champion_model()


def fetch_station(conn, req: PredictStationRequest) -> dict[str, Any]:
    if not req.gbfs_station_id and not req.legacy_station_id:
        raise HTTPException(status_code=400, detail="Нужно передать gbfs_station_id или legacy_station_id")

    if req.gbfs_station_id:
        where_sql = "gbfs_station_id = %s"
        value = req.gbfs_station_id
    else:
        where_sql = "legacy_station_id = %s"
        value = req.legacy_station_id

    sql = f"""
        SELECT
            snapshot_time,
            snapshot_time_ny,
            gbfs_station_id,
            legacy_station_id,
            station_name,
            lat,
            lon,
            capacity,
            current_bikes,
            current_docks,
            hour_ny,
            day_of_week_ny,
            is_weekend_ny,
            month_ny,
            data_age_seconds
        FROM gbfs_online_features
        WHERE {where_sql}
        ORDER BY snapshot_time DESC
        LIMIT 1;
    """

    with conn.cursor() as cur:
        cur.execute(sql, (value,))
        row = cur.fetchone()
        cols = [desc[0] for desc in cur.description] if cur.description else []

    if row is None:
        raise HTTPException(status_code=404, detail="Станция не найдена в gbfs_online_features")

    return dict(zip(cols, row))


def fetch_weather_forecast(target_time_utc: datetime) -> tuple[dict[str, Any], str]:
    try:
        target_date = target_time_utc.date().isoformat()
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={NY_LAT}&longitude={NY_LON}"
            "&hourly=temperature_2m,precipitation,wind_speed_10m,weather_code"
            "&timezone=America/New_York"
            f"&start_date={target_date}&end_date={target_date}"
        )
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        hourly = data["hourly"]

        # target_time_utc is UTC; station snapshot_time_ny is already NY local.
        # For MVP we select nearest available forecast hour by hour index.
        target_hour = target_time_utc.astimezone(timezone.utc).hour
        times = hourly["time"]

        # Open-Meteo returns local NY timestamps as strings; use the nearest hour by suffix.
        index = min(range(len(times)), key=lambda i: abs(int(times[i][11:13]) - target_hour))

        weather_code = int(hourly["weather_code"][index])
        precipitation = float(hourly["precipitation"][index])

        return {
            "temperature_2m": float(hourly["temperature_2m"][index]),
            "precipitation": precipitation,
            "wind_speed_10m": float(hourly["wind_speed_10m"][index]),
            "weather_code": weather_code,
            "is_rain": 1 if precipitation > 0 or weather_code in {51, 53, 55, 61, 63, 65, 80, 81, 82} else 0,
        }, "open_meteo_forecast"
    except Exception:
        return {
            "temperature_2m": 15.0,
            "precipitation": 0.0,
            "wind_speed_10m": 3.0,
            "weather_code": 0,
            "is_rain": 0,
        }, "fallback_neutral_weather"


def insert_prediction_log(conn, payload: dict[str, Any]) -> Optional[int]:
    sql = """
        INSERT INTO prediction_log (
            predicted_at,
            target_time,
            gbfs_station_id,
            legacy_station_id,
            station_name,
            model_name,
            model_version,
            mlflow_run_id,
            current_bikes,
            current_docks,
            capacity,
            predicted_delta_bikes,
            predicted_bikes_raw,
            predicted_bikes_clipped,
            predicted_docks_raw,
            predicted_docks_clipped,
            stale_state,
            state_age_seconds
        )
        VALUES (
            %(predicted_at)s,
            %(target_time)s,
            %(gbfs_station_id)s,
            %(legacy_station_id)s,
            %(station_name)s,
            %(model_name)s,
            %(model_version)s,
            %(mlflow_run_id)s,
            %(current_bikes)s,
            %(current_docks)s,
            %(capacity)s,
            %(predicted_delta_bikes)s,
            %(predicted_bikes_raw)s,
            %(predicted_bikes_clipped)s,
            NULL,
            NULL,
            %(stale_state)s,
            %(state_age_seconds)s
        )
        RETURNING id;
    """

    with conn.cursor() as cur:
        cur.execute(sql, payload)
        prediction_id = cur.fetchone()[0]
    conn.commit()
    return prediction_id


@app.get("/health")
def health() -> dict[str, Any]:
    db_ok = False
    model_ok = _model is not None

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                db_ok = cur.fetchone()[0] == 1
    except Exception:
        db_ok = False

    status = "ok" if db_ok and model_ok else "degraded"

    return {
        "service": APP_NAME,
        "status": status,
        "db_ok": db_ok,
        "model_ok": model_ok,
        "model_name": MODEL_NAME,
        "model_alias": MODEL_ALIAS,
        "model_version": _model_version,
    }


@app.post("/predict/station", response_model=PredictStationResponse)
def predict_station(req: PredictStationRequest) -> PredictStationResponse:
    if _model is None:
        raise HTTPException(status_code=503, detail="Champion model is not loaded")

    predicted_at = datetime.now(timezone.utc)
    target_time = predicted_at + timedelta(minutes=req.horizon_minutes)

    with get_connection() as conn:
        station = fetch_station(conn, req)
        weather, weather_source = fetch_weather_forecast(target_time)

        features = {
            "hour_ny": int(station["hour_ny"]),
            "day_of_week_ny": int(station["day_of_week_ny"]),
            "is_weekend_ny": int(station["is_weekend_ny"]),
            "month_ny": int(station["month_ny"]),
            "lat": float(station["lat"]),
            "lon": float(station["lon"]),
            "capacity": int(station["capacity"]),
            "temperature_2m": float(weather["temperature_2m"]),
            "precipitation": float(weather["precipitation"]),
            "wind_speed_10m": float(weather["wind_speed_10m"]),
            "weather_code": int(weather["weather_code"]),
            "is_rain": int(weather["is_rain"]),
        }

        X = pd.DataFrame([features], columns=FEATURE_COLUMNS)

        int_columns = [
            "hour_ny",
            "day_of_week_ny",
            "is_weekend_ny",
            "month_ny",
            "capacity",
            "weather_code",
            "is_rain",
        ]
        for col in int_columns:
            X[col] = X[col].astype("int64")

        float_columns = [
            "lat",
            "lon",
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
        ]
        for col in float_columns:
            X[col] = X[col].astype("float64")

        pred_delta = float(_model.predict(X)[0])

        current_bikes = station["current_bikes"]
        capacity = station["capacity"]

        if current_bikes is None or capacity is None:
            raise HTTPException(status_code=422, detail="У станции нет current_bikes или capacity")

        predicted_bikes_raw = float(current_bikes) + pred_delta
        predicted_bikes_clipped = min(max(predicted_bikes_raw, 0.0), float(capacity))

        state_age_seconds = station.get("data_age_seconds")
        stale_state = bool(state_age_seconds is not None and int(state_age_seconds) > STALE_STATE_SECONDS)
        low_risk = predicted_bikes_clipped <= LOW_AVAILABILITY_THRESHOLD

        log_payload = {
            "predicted_at": predicted_at,
            "target_time": target_time,
            "gbfs_station_id": station["gbfs_station_id"],
            "legacy_station_id": station["legacy_station_id"],
            "station_name": station["station_name"],
            "model_name": MODEL_NAME,
            "model_version": _model_version,
            "mlflow_run_id": _model_run_id,
            "current_bikes": current_bikes,
            "current_docks": station["current_docks"],
            "capacity": capacity,
            "predicted_delta_bikes": pred_delta,
            "predicted_bikes_raw": predicted_bikes_raw,
            "predicted_bikes_clipped": predicted_bikes_clipped,
            "stale_state": stale_state,
            "state_age_seconds": state_age_seconds,
        }

        prediction_log_id = insert_prediction_log(conn, log_payload)

    return PredictStationResponse(
        gbfs_station_id=station["gbfs_station_id"],
        legacy_station_id=station["legacy_station_id"],
        station_name=station["station_name"],
        horizon_minutes=req.horizon_minutes,
        current_bikes=current_bikes,
        current_docks=station["current_docks"],
        capacity=capacity,
        predicted_delta_bikes_1h=pred_delta,
        predicted_bikes_raw=predicted_bikes_raw,
        predicted_bikes_clipped=predicted_bikes_clipped,
        low_availability_risk=low_risk,
        threshold=LOW_AVAILABILITY_THRESHOLD,
        stale_state=stale_state,
        state_age_seconds=state_age_seconds,
        model_name=MODEL_NAME,
        model_alias=MODEL_ALIAS,
        model_version=_model_version or "unknown",
        mlflow_run_id=_model_run_id,
        weather_source=weather_source,
        predicted_at=predicted_at.isoformat(),
        target_time=target_time.isoformat(),
        prediction_log_id=prediction_log_id,
    )


@app.post("/predict/batch")
def predict_batch(requests_list: list[PredictStationRequest]) -> dict[str, Any]:
    results = []
    errors = []

    for item in requests_list:
        try:
            results.append(predict_station(item).model_dump())
        except HTTPException as exc:
            errors.append({"request": item.model_dump(), "status_code": exc.status_code, "detail": exc.detail})

    return {
        "results": results,
        "errors": errors,
        "count_results": len(results),
        "count_errors": len(errors),
    }
