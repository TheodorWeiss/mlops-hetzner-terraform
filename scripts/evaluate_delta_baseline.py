#!/usr/bin/env python3

import math
import os
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2
from sklearn.metrics import mean_absolute_error, mean_squared_error


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


def safe_eval(y_true, y_pred):
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(math.sqrt(mean_squared_error(y_true, y_pred)))
    bias = float(np.mean(np.asarray(y_pred) - np.asarray(y_true)))
    return mae, rmse, bias


def write_result(conn, run_name, model_type, rows_test, mae, rmse, bias, notes):
    sql = """
        INSERT INTO delta_model_evaluation_runs (
            run_name,
            model_type,
            target_name,
            train_start_ny,
            train_end_ny,
            test_start_ny,
            test_end_ny,
            rows_test,
            mae_delta,
            rmse_delta,
            bias_delta,
            notes
        )
        VALUES (
            %s,
            %s,
            'delta_bikes',
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
        cur.execute(sql, (run_name, model_type, rows_test, mae, rmse, bias, notes))
        result_id = cur.fetchone()[0]
    conn.commit()
    return result_id


def main():
    sql = """
        SELECT
            bucket_start_ny,
            gbfs_station_id,
            hour_ny,
            day_of_week_ny,
            delta_bikes,
            split
        FROM station_delta_training_features
        WHERE split IN ('train', 'test');
    """

    with get_connection() as conn:
        df = pd.read_sql_query(sql, conn)

        train = df[df["split"] == "train"].copy()
        test = df[df["split"] == "test"].copy()

        y_test = test["delta_bikes"].values

        # Baseline 1: no net change.
        pred_zero = np.zeros(len(test))
        mae, rmse, bias = safe_eval(y_test, pred_zero)
        id_zero = write_result(
            conn,
            "delta_baseline_zero_change_202604",
            "delta_baseline_zero_change",
            len(test),
            mae,
            rmse,
            bias,
            "Naive baseline: predicted delta_bikes = 0 for every station-hour.",
        )

        print("DELTA_BASELINE_OK")
        print(f"id={id_zero}")
        print("model_type=delta_baseline_zero_change")
        print(f"rows_test={len(test)}")
        print(f"mae_delta={mae:.6f}")
        print(f"rmse_delta={rmse:.6f}")
        print(f"bias_delta={bias:.6f}")

        # Baseline 2: train mean by hour and day_of_week.
        global_profile = (
            train.groupby(["hour_ny", "day_of_week_ny"])["delta_bikes"]
            .mean()
            .reset_index()
            .rename(columns={"delta_bikes": "profile_delta_hour_dow"})
        )

        merged = test.merge(global_profile, on=["hour_ny", "day_of_week_ny"], how="left")
        pred_profile = merged["profile_delta_hour_dow"].fillna(0.0).values

        mae, rmse, bias = safe_eval(merged["delta_bikes"].values, pred_profile)
        id_profile = write_result(
            conn,
            "delta_baseline_global_hour_dow_202604",
            "delta_baseline_global_hour_dow",
            len(test),
            mae,
            rmse,
            bias,
            "Naive baseline: predicted delta is train mean by hour_ny and day_of_week_ny.",
        )

        print("DELTA_BASELINE_OK")
        print(f"id={id_profile}")
        print("model_type=delta_baseline_global_hour_dow")
        print(f"rows_test={len(test)}")
        print(f"mae_delta={mae:.6f}")
        print(f"rmse_delta={rmse:.6f}")
        print(f"bias_delta={bias:.6f}")


if __name__ == "__main__":
    main()
