#!/usr/bin/env python3

import math
import os
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error


FEATURE_COLUMNS = [
    "hour_ny",
    "day_of_week_ny",
    "is_weekend_ny",
    "month_ny",
    "lat",
    "lon",
    "capacity",
    "lag_1h_departures",
    "lag_24h_departures",
    "lag_168h_departures",
    "rolling_24h_departures",
    "lag_1h_returns",
    "lag_24h_returns",
    "lag_168h_returns",
    "rolling_24h_returns",
]


def getenv_first(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def get_connection():
    host = getenv_first("POSTGRES_HOST", "PGHOST", default="postgres")
    port = getenv_first("POSTGRES_PORT", "PGPORT", default="5432")
    dbname = getenv_first("BIKEML_DB", "POSTGRES_DB", "PGDATABASE", default="bikeml")
    user = getenv_first("POSTGRES_USER", "PGUSER", default="bikeml_admin")
    password = getenv_first("POSTGRES_PASSWORD", "PGPASSWORD")

    if not password:
        raise RuntimeError(
            "POSTGRES_PASSWORD is not set. Run inside Airflow container with env_file=.env."
        )

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


def load_dataset(conn) -> pd.DataFrame:
    sql = """
        SELECT
            bucket_start_ny,
            gbfs_station_id,
            departures,
            returns,
            hour_ny,
            day_of_week_ny,
            is_weekend_ny,
            month_ny,
            lat,
            lon,
            capacity,
            lag_1h_departures,
            lag_24h_departures,
            lag_168h_departures,
            rolling_24h_departures,
            lag_1h_returns,
            lag_24h_returns,
            lag_168h_returns,
            rolling_24h_returns
        FROM station_hourly_features
        WHERE bucket_start_ny >= TIMESTAMP '2026-02-01 00:00:00'
          AND bucket_start_ny <  TIMESTAMP '2026-05-01 00:00:00'
          AND lag_168h_departures IS NOT NULL
          AND lag_168h_returns IS NOT NULL
          AND rolling_24h_departures IS NOT NULL
          AND rolling_24h_returns IS NOT NULL
          AND lat IS NOT NULL
          AND lon IS NOT NULL
          AND capacity IS NOT NULL;
    """
    return pd.read_sql_query(sql, conn)


def safe_mape(y_true, y_pred) -> Optional[float]:
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)

    mask = y_true > 0
    if mask.sum() == 0:
        return None

    return float(np.mean(np.abs(y_true[mask] - y_pred[mask]) / y_true[mask]))


def train_and_eval(df: pd.DataFrame, target_name: str):
    train_mask = (
        (df["bucket_start_ny"] >= "2026-02-01 00:00:00")
        & (df["bucket_start_ny"] < "2026-04-01 00:00:00")
    )
    test_mask = (
        (df["bucket_start_ny"] >= "2026-04-01 00:00:00")
        & (df["bucket_start_ny"] < "2026-05-01 00:00:00")
    )

    train_df = df.loc[train_mask].copy()
    test_df = df.loc[test_mask].copy()

    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df[target_name]

    X_test = test_df[FEATURE_COLUMNS]
    y_test = test_df[target_name]

    model = LGBMRegressor(
        objective="poisson",
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=50,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=42,
        n_jobs=-1,
        verbosity=-1,
    )

    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_pred = np.clip(y_pred, 0, None)

    mae = float(mean_absolute_error(y_test, y_pred))
    rmse = float(math.sqrt(mean_squared_error(y_test, y_pred)))
    mape = safe_mape(y_test, y_pred)

    return {
        "target_name": target_name,
        "rows_train": len(train_df),
        "rows_test": len(test_df),
        "mae": mae,
        "rmse": rmse,
        "mape": mape,
    }


def write_eval(conn, result):
    sql = """
        INSERT INTO model_evaluation_runs (
            run_name,
            model_type,
            target_name,
            train_start_ny,
            train_end_ny,
            test_start_ny,
            test_end_ny,
            rows_test,
            mae,
            rmse,
            mape,
            notes
        )
        VALUES (
            %s,
            %s,
            %s,
            TIMESTAMP '2026-02-01 00:00:00',
            TIMESTAMP '2026-04-01 00:00:00',
            TIMESTAMP '2026-04-01 00:00:00',
            TIMESTAMP '2026-05-01 00:00:00',
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
                f"lightgbm_poisson_{result['target_name']}_202604",
                "lightgbm_poisson",
                result["target_name"],
                result["rows_test"],
                result["mae"],
                result["rmse"],
                result["mape"],
                f"LightGBM Poisson. rows_train={result['rows_train']}. Basic NY-time, station metadata and lag features.",
            ),
        )
        run_id = cur.fetchone()[0]

    conn.commit()
    return run_id


def main():
    with get_connection() as conn:
        print("loading_dataset...")
        df = load_dataset(conn)

        print(f"dataset_rows={len(df)}")
        print(f"min_bucket_ny={df['bucket_start_ny'].min()}")
        print(f"max_bucket_ny={df['bucket_start_ny'].max()}")

        for target_name in ["departures", "returns"]:
            result = train_and_eval(df, target_name)
            run_id = write_eval(conn, result)

            print("LIGHTGBM_EVAL_OK")
            print(f"id={run_id}")
            print(f"target_name={target_name}")
            print(f"rows_train={result['rows_train']}")
            print(f"rows_test={result['rows_test']}")
            print(f"mae={result['mae']:.6f}")
            print(f"rmse={result['rmse']:.6f}")
            print(f"mape={result['mape']:.6f}" if result["mape"] is not None else "mape=None")


if __name__ == "__main__":
    main()
