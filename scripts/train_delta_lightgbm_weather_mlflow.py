#!/usr/bin/env python3

import math
import os
from typing import Optional

import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import psycopg2
from lightgbm import LGBMRegressor
from mlflow.tracking import MlflowClient
from sklearn.metrics import mean_absolute_error, mean_squared_error


os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", ""))
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")


TRACKING_URI = "http://mlflow:5000"
EXPERIMENT_NAME = "bikeml-delta-forecasting"
REGISTERED_MODEL_NAME = "bikeml_delta_bikes_forecaster"

BASELINE_MAE_THRESHOLD = 2.227188

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


def load_dataset(conn) -> pd.DataFrame:
    sql = """
        SELECT
            bucket_start_ny,
            gbfs_station_id,
            hour_ny,
            day_of_week_ny,
            is_weekend_ny,
            month_ny,
            lat,
            lon,
            capacity,
            temperature_2m,
            precipitation,
            wind_speed_10m,
            weather_code,
            is_rain,
            delta_bikes,
            split
        FROM station_delta_training_features
        WHERE split IN ('train', 'test')
          AND lat IS NOT NULL
          AND lon IS NOT NULL
          AND capacity IS NOT NULL
          AND temperature_2m IS NOT NULL
          AND precipitation IS NOT NULL
          AND wind_speed_10m IS NOT NULL
          AND weather_code IS NOT NULL
          AND is_rain IS NOT NULL;
    """
    return pd.read_sql_query(sql, conn)


def get_split_window(df: pd.DataFrame) -> dict:
    train = df[df["split"] == "train"]
    test = df[df["split"] == "test"]

    if train.empty or test.empty:
        raise RuntimeError("Train or test split is empty.")

    test_start = test["bucket_start_ny"].min()
    test_end_exclusive = test["bucket_start_ny"].max() + pd.Timedelta(hours=1)
    train_start = train["bucket_start_ny"].min()
    train_end_exclusive = train["bucket_start_ny"].max() + pd.Timedelta(hours=1)

    test_month = pd.Timestamp(test_start).strftime("%Y%m")

    return {
        "train_start_ny": train_start,
        "train_end_ny": train_end_exclusive,
        "test_start_ny": test_start,
        "test_end_ny": test_end_exclusive,
        "test_month": test_month,
    }


def write_eval_to_postgres(conn, result):
    sql = """
        INSERT INTO delta_model_evaluation_runs (
            run_name,
            model_type,
            target_name,
            train_start_ny,
            train_end_ny,
            test_start_ny,
            test_end_ny,
            rows_train,
            rows_test,
            mae_delta,
            rmse_delta,
            bias_delta,
            mlflow_run_id,
            registered_model_name,
            registered_model_version,
            promotion_decision,
            notes
        )
        VALUES (
            %s,
            %s,
            'delta_bikes',
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        RETURNING id;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                result["run_name"],
                result["model_type"],
                result["train_start_ny"],
                result["train_end_ny"],
                result["test_start_ny"],
                result["test_end_ny"],
                result["rows_train"],
                result["rows_test"],
                result["mae_delta"],
                result["rmse_delta"],
                result["bias_delta"],
                result["mlflow_run_id"],
                result["registered_model_name"],
                result["registered_model_version"],
                result["promotion_decision"],
                result["notes"],
            ),
        )
        eval_id = cur.fetchone()[0]
    conn.commit()
    return eval_id


def main():
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient(TRACKING_URI)

    with get_connection() as conn:
        print("loading_dataset...")
        df = load_dataset(conn)
        window = get_split_window(df)

        train = df[df["split"] == "train"].copy()
        test = df[df["split"] == "test"].copy()

        X_train = train[FEATURE_COLUMNS]
        y_train = train["delta_bikes"]

        X_test = test[FEATURE_COLUMNS]
        y_test = test["delta_bikes"]

        run_name = f"lightgbm_delta_time_geo_weather_test_{window['test_month']}"
        model_type = "lightgbm_delta_time_geo_weather"

        params = {
            "objective": "regression",
            "n_estimators": 300,
            "learning_rate": 0.05,
            "num_leaves": 31,
            "min_child_samples": 50,
            "subsample": 0.9,
            "colsample_bytree": 0.9,
            "random_state": 42,
            "n_jobs": -1,
            "verbosity": -1,
        }

        model = LGBMRegressor(**params)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        mae = float(mean_absolute_error(y_test, y_pred))
        rmse = float(math.sqrt(mean_squared_error(y_test, y_pred)))
        bias = float(np.mean(y_pred - y_test))

        if mae < BASELINE_MAE_THRESHOLD:
            baseline_check = "baseline_passed"
            baseline_note = f"mae_delta={mae:.6f} лучше zero-change baseline={BASELINE_MAE_THRESHOLD:.6f}"
        else:
            baseline_check = "baseline_failed"
            baseline_note = f"mae_delta={mae:.6f} не лучше zero-change baseline={BASELINE_MAE_THRESHOLD:.6f}"

        promotion_decision = "candidate_pending_fair_gate"
        decision_note = (
            "LightGBM weather candidate обучена и ожидает честный quality gate: "
            "сравнение с текущим champion на том же test_df. "
            + baseline_note
        )

        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id

            mlflow.log_params(params)
            mlflow.log_param("target_name", "delta_bikes")
            mlflow.log_param("feature_columns", ",".join(FEATURE_COLUMNS))
            mlflow.log_param("train_start_ny", str(window["train_start_ny"]))
            mlflow.log_param("train_end_ny", str(window["train_end_ny"]))
            mlflow.log_param("test_start_ny", str(window["test_start_ny"]))
            mlflow.log_param("test_end_ny", str(window["test_end_ny"]))
            mlflow.log_param("test_month", window["test_month"])
            mlflow.log_param("rows_train", len(train))
            mlflow.log_param("rows_test", len(test))
            mlflow.log_param("baseline_mae_threshold", BASELINE_MAE_THRESHOLD)
            mlflow.log_param("baseline_check", baseline_check)
            mlflow.log_param("promotion_decision", promotion_decision)
            mlflow.log_param("weather_features", "true")

            mlflow.log_metric("mae_delta", mae)
            mlflow.log_metric("rmse_delta", rmse)
            mlflow.log_metric("bias_delta", bias)

            mlflow.lightgbm.log_model(
                model,
                artifact_path="model",
                input_example=X_test.head(5),
            )

        version = mlflow.register_model(
            model_uri=f"runs:/{run_id}/model",
            name=REGISTERED_MODEL_NAME,
        )

        client.set_registered_model_alias(
            name=REGISTERED_MODEL_NAME,
            alias="candidate",
            version=version.version,
        )

        client.set_registered_model_tag(
            name=REGISTERED_MODEL_NAME,
            key="serving",
            value="true",
        )

        client.set_model_version_tag(
            name=REGISTERED_MODEL_NAME,
            version=version.version,
            key="role",
            value="serving_delta_weather_candidate",
        )
        client.set_model_version_tag(
            name=REGISTERED_MODEL_NAME,
            version=version.version,
            key="weather_features",
            value="true",
        )
        client.set_model_version_tag(
            name=REGISTERED_MODEL_NAME,
            version=version.version,
            key="promotion_decision",
            value=promotion_decision,
        )
        client.set_model_version_tag(
            name=REGISTERED_MODEL_NAME,
            version=version.version,
            key="reason",
            value=decision_note,
        )

        result = {
            "run_name": run_name,
            "model_type": model_type,
            "train_start_ny": window["train_start_ny"],
            "train_end_ny": window["train_end_ny"],
            "test_start_ny": window["test_start_ny"],
            "test_end_ny": window["test_end_ny"],
            "rows_train": len(train),
            "rows_test": len(test),
            "mae_delta": mae,
            "rmse_delta": rmse,
            "bias_delta": bias,
            "mlflow_run_id": run_id,
            "registered_model_name": REGISTERED_MODEL_NAME,
            "registered_model_version": str(version.version),
            "promotion_decision": promotion_decision,
            "notes": decision_note,
        }

        eval_id = write_eval_to_postgres(conn, result)

    print("LIGHTGBM_DELTA_WEATHER_OK")
    print(f"postgres_eval_id={eval_id}")
    print(f"mlflow_run_id={run_id}")
    print(f"registered_model_name={REGISTERED_MODEL_NAME}")
    print(f"registered_model_version={version.version}")
    print(f"run_name={run_name}")
    print(f"rows_train={len(train)}")
    print(f"rows_test={len(test)}")
    print(f"mae_delta={mae:.6f}")
    print(f"rmse_delta={rmse:.6f}")
    print(f"bias_delta={bias:.6f}")
    print(f"promotion_decision={promotion_decision}")


if __name__ == "__main__":
    main()
