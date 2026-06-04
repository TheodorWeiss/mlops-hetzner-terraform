#!/usr/bin/env python3

import argparse
import math
import os
from typing import Optional

import mlflow
import numpy as np
import pandas as pd
import psycopg2
from sklearn.metrics import mean_absolute_error, mean_squared_error


os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", ""))
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")

TRACKING_URI = "http://mlflow:5000"
REGISTERED_MODEL_NAME = "bikeml_delta_bikes_forecaster"

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


def load_test_df(conn) -> pd.DataFrame:
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
            delta_bikes
        FROM station_delta_training_features
        WHERE split = 'test'
          AND lat IS NOT NULL
          AND lon IS NOT NULL
          AND capacity IS NOT NULL
          AND temperature_2m IS NOT NULL
          AND precipitation IS NOT NULL
          AND wind_speed_10m IS NOT NULL
          AND weather_code IS NOT NULL
          AND is_rain IS NOT NULL
        ORDER BY bucket_start_ny, gbfs_station_id;
    """
    return pd.read_sql_query(sql, conn)


def write_eval(conn, result):
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
            NULL,
            NULL,
            %s,
            %s,
            NULL,
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
                result["test_start_ny"],
                result["test_end_ny"],
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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-uri", required=True)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--promotion-decision", default="champion_reeval_on_current_test")
    return parser.parse_args()


def main():
    args = parse_args()

    mlflow.set_tracking_uri(TRACKING_URI)

    with get_connection() as conn:
        print("loading_test_df...")
        df = load_test_df(conn)

        if df.empty:
            raise RuntimeError("Current test split is empty.")

        X_test = df[FEATURE_COLUMNS]
        y_test = df["delta_bikes"]

        test_start = df["bucket_start_ny"].min()
        test_end = df["bucket_start_ny"].max() + pd.Timedelta(hours=1)
        test_month = pd.Timestamp(test_start).strftime("%Y%m")

        print(f"model_uri={args.model_uri}")
        print(f"rows_test={len(df)}")
        print(f"test_start_ny={test_start}")
        print(f"test_end_ny={test_end}")

        model = mlflow.pyfunc.load_model(args.model_uri)
        y_pred = model.predict(X_test)

        mae = float(mean_absolute_error(y_test, y_pred))
        rmse = float(math.sqrt(mean_squared_error(y_test, y_pred)))
        bias = float(np.mean(y_pred - y_test))

        run_name = f"{args.label}_reeval_test_{test_month}"
        notes = (
            f"Re-evaluation of {args.model_uri} on the exact current test_df. "
            f"Feature columns: {','.join(FEATURE_COLUMNS)}."
        )

        result = {
            "run_name": run_name,
            "model_type": args.label,
            "test_start_ny": test_start,
            "test_end_ny": test_end,
            "rows_test": len(df),
            "mae_delta": mae,
            "rmse_delta": rmse,
            "bias_delta": bias,
            "mlflow_run_id": None,
            "registered_model_name": REGISTERED_MODEL_NAME,
            "registered_model_version": args.model_version,
            "promotion_decision": args.promotion_decision,
            "notes": notes,
        }

        eval_id = write_eval(conn, result)

    print("DELTA_MODEL_REEVAL_OK")
    print(f"postgres_eval_id={eval_id}")
    print(f"model_uri={args.model_uri}")
    print(f"model_version={args.model_version}")
    print(f"label={args.label}")
    print(f"rows_test={len(df)}")
    print(f"test_start_ny={test_start}")
    print(f"test_end_ny={test_end}")
    print(f"mae_delta={mae:.6f}")
    print(f"rmse_delta={rmse:.6f}")
    print(f"bias_delta={bias:.6f}")


if __name__ == "__main__":
    main()
