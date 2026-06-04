#!/usr/bin/env python3

import math
import os
from typing import Optional

import psycopg2


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


def evaluate_baseline(conn, target_name: str, prediction_column: str, model_type: str, notes: str):
    sql = f"""
        WITH eval_data AS (
            SELECT
                bucket_start_ny,
                gbfs_station_id,
                {target_name}::DOUBLE PRECISION AS y_true,
                {prediction_column}::DOUBLE PRECISION AS y_pred
            FROM station_hourly_features
            WHERE bucket_start_ny >= TIMESTAMP '2026-04-01 00:00:00'
              AND bucket_start_ny <  TIMESTAMP '2026-05-01 00:00:00'
              AND {prediction_column} IS NOT NULL
        ),
        metrics AS (
            SELECT
                COUNT(*) AS rows_test,
                AVG(ABS(y_true - y_pred)) AS mae,
                SQRT(AVG(POWER(y_true - y_pred, 2))) AS rmse,
                AVG(
                    CASE
                        WHEN y_true > 0 THEN ABS(y_true - y_pred) / y_true
                        ELSE NULL
                    END
                ) AS mape
            FROM eval_data
        )
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
        SELECT
            %s,
            %s,
            %s,
            TIMESTAMP '2026-02-01 00:00:00',
            TIMESTAMP '2026-04-01 00:00:00',
            TIMESTAMP '2026-04-01 00:00:00',
            TIMESTAMP '2026-05-01 00:00:00',
            rows_test,
            mae,
            rmse,
            mape,
            %s
        FROM metrics
        RETURNING id, rows_test, mae, rmse, mape;
    """

    run_name = f"{model_type}_{target_name}_202604"

    with conn.cursor() as cur:
        cur.execute(sql, (run_name, model_type, target_name, notes))
        row = cur.fetchone()

    conn.commit()

    return row


def main() -> None:
    baselines = [
        ("departures", "lag_24h_departures", "baseline_lag_24h", "Prediction equals same station previous day same hour."),
        ("departures", "lag_168h_departures", "baseline_lag_168h", "Prediction equals same station previous week same hour."),
        ("returns", "lag_24h_returns", "baseline_lag_24h", "Prediction equals same station previous day same hour."),
        ("returns", "lag_168h_returns", "baseline_lag_168h", "Prediction equals same station previous week same hour."),
    ]

    with get_connection() as conn:
        for target_name, prediction_column, model_type, notes in baselines:
            run_id, rows_test, mae, rmse, mape = evaluate_baseline(
                conn,
                target_name,
                prediction_column,
                model_type,
                notes,
            )

            print("BASELINE_EVAL_OK")
            print(f"id={run_id}")
            print(f"model_type={model_type}")
            print(f"target_name={target_name}")
            print(f"rows_test={rows_test}")
            print(f"mae={mae:.6f}")
            print(f"rmse={rmse:.6f}")
            print(f"mape={mape:.6f}" if mape is not None else "mape=None")


if __name__ == "__main__":
    main()
