#!/usr/bin/env python3

import math
import os
from typing import Optional

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import psycopg2
from mlflow.tracking import MlflowClient
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor


# Force correct MinIO/S3 credentials for MLflow artifacts.
os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", ""))
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")


TRACKING_URI = "http://mlflow:5000"
EXPERIMENT_NAME = "bikeml-demand-forecasting"

TRAIN_START_NY = "2026-02-01 00:00:00"
TRAIN_END_NY = "2026-04-01 00:00:00"
TEST_START_NY = "2026-04-01 00:00:00"
TEST_END_NY = "2026-05-01 00:00:00"

MIN_MAE_IMPROVEMENT_RATIO = 0.01

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

REGISTERED_MODELS = {
    "departures": "bikeml_departures_forecaster",
    "returns": "bikeml_returns_forecaster",
}


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


def get_champion_mae(conn, target_name: str) -> Optional[float]:
    sql = """
        SELECT mae
        FROM model_evaluation_runs
        WHERE model_type = 'lightgbm_poisson_mlflow'
          AND target_name = %s
          AND test_start_ny = TIMESTAMP '2026-04-01 00:00:00'
          AND test_end_ny = TIMESTAMP '2026-05-01 00:00:00'
        ORDER BY created_at DESC
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (target_name,))
        row = cur.fetchone()

    return float(row[0]) if row else None


def train_eval_log_xgboost(df: pd.DataFrame, target_name: str):
    train_mask = (
        (df["bucket_start_ny"] >= TRAIN_START_NY)
        & (df["bucket_start_ny"] < TRAIN_END_NY)
    )
    test_mask = (
        (df["bucket_start_ny"] >= TEST_START_NY)
        & (df["bucket_start_ny"] < TEST_END_NY)
    )

    train_df = df.loc[train_mask].copy()
    test_df = df.loc[test_mask].copy()

    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df[target_name]

    X_test = test_df[FEATURE_COLUMNS]
    y_test = test_df[target_name]

    params = {
        "objective": "count:poisson",
        "n_estimators": 300,
        "learning_rate": 0.05,
        "max_depth": 6,
        "min_child_weight": 5,
        "subsample": 0.9,
        "colsample_bytree": 0.9,
        "reg_lambda": 1.0,
        "random_state": 42,
        "n_jobs": -1,
        "tree_method": "hist",
        "eval_metric": "mae",
    }

    model = XGBRegressor(**params)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_pred = np.clip(y_pred, 0, None)

    mae = float(mean_absolute_error(y_test, y_pred))
    rmse = float(math.sqrt(mean_squared_error(y_test, y_pred)))
    mape = safe_mape(y_test, y_pred)

    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    run_name = f"xgboost_poisson_{target_name}_202604_challenger"

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id

        mlflow.log_params(params)
        mlflow.log_param("role", "challenger")
        mlflow.log_param("target_name", target_name)
        mlflow.log_param("train_start_ny", TRAIN_START_NY)
        mlflow.log_param("train_end_ny", TRAIN_END_NY)
        mlflow.log_param("test_start_ny", TEST_START_NY)
        mlflow.log_param("test_end_ny", TEST_END_NY)
        mlflow.log_param("rows_train", len(train_df))
        mlflow.log_param("rows_test", len(test_df))
        mlflow.log_param("feature_columns", ",".join(FEATURE_COLUMNS))

        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)
        if mape is not None:
            mlflow.log_metric("mape", mape)

        input_example = X_test.head(5)
        mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            input_example=input_example,
        )

    return {
        "mlflow_run_id": run_id,
        "target_name": target_name,
        "rows_train": len(train_df),
        "rows_test": len(test_df),
        "mae": mae,
        "rmse": rmse,
        "mape": mape,
    }


def write_eval_to_postgres(conn, result, champion_mae, promotion_decision, reason):
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

    notes = (
        f"XGBoost challenger logged to MLflow. "
        f"mlflow_run_id={result['mlflow_run_id']}. "
        f"rows_train={result['rows_train']}. "
        f"champion_mae={champion_mae}. "
        f"promotion_decision={promotion_decision}. "
        f"reason={reason}."
    )

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                f"xgboost_poisson_challenger_{result['target_name']}_202604",
                "xgboost_poisson_challenger",
                result["target_name"],
                result["rows_test"],
                result["mae"],
                result["rmse"],
                result["mape"],
                notes,
            ),
        )
        eval_id = cur.fetchone()[0]

    conn.commit()
    return eval_id


def register_challenger(result, promotion_decision, reason):
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient(tracking_uri=TRACKING_URI)

    target_name = result["target_name"]
    model_name = REGISTERED_MODELS[target_name]
    run_id = result["mlflow_run_id"]
    source_uri = f"runs:/{run_id}/model"

    version = mlflow.register_model(
        model_uri=source_uri,
        name=model_name,
    )

    client.set_registered_model_alias(
        name=model_name,
        alias="challenger",
        version=version.version,
    )

    client.set_model_version_tag(
        name=model_name,
        version=version.version,
        key="target_name",
        value=target_name,
    )
    client.set_model_version_tag(
        name=model_name,
        version=version.version,
        key="role",
        value="challenger",
    )
    client.set_model_version_tag(
        name=model_name,
        version=version.version,
        key="source_run_id",
        value=run_id,
    )
    client.set_model_version_tag(
        name=model_name,
        version=version.version,
        key="promotion_decision",
        value=promotion_decision,
    )
    client.set_model_version_tag(
        name=model_name,
        version=version.version,
        key="promotion_reason",
        value=reason,
    )

    return version.version


def main():
    with get_connection() as conn:
        print("loading_dataset...")
        df = load_dataset(conn)

        print(f"dataset_rows={len(df)}")
        print(f"min_bucket_ny={df['bucket_start_ny'].min()}")
        print(f"max_bucket_ny={df['bucket_start_ny'].max()}")

        for target_name in ["departures", "returns"]:
            champion_mae = get_champion_mae(conn, target_name)
            if champion_mae is None:
                raise RuntimeError(f"No champion MAE found in Postgres for target={target_name}")

            result = train_eval_log_xgboost(df, target_name)

            required_mae = champion_mae * (1.0 - MIN_MAE_IMPROVEMENT_RATIO)

            if result["mae"] < required_mae:
                promotion_decision = "eligible_for_promotion"
                reason = (
                    f"challenger_mae={result['mae']:.6f} is better than "
                    f"required_mae={required_mae:.6f}"
                )
            else:
                promotion_decision = "rejected_keep_champion"
                reason = (
                    f"challenger_mae={result['mae']:.6f} is not better than "
                    f"required_mae={required_mae:.6f}"
                )

            model_version = register_challenger(result, promotion_decision, reason)
            eval_id = write_eval_to_postgres(
                conn,
                result,
                champion_mae,
                promotion_decision,
                reason,
            )

            print("XGBOOST_CHALLENGER_OK")
            print(f"postgres_eval_id={eval_id}")
            print(f"mlflow_run_id={result['mlflow_run_id']}")
            print(f"target_name={target_name}")
            print(f"registered_model={REGISTERED_MODELS[target_name]}")
            print(f"registered_version={model_version}")
            print(f"alias=challenger")
            print(f"rows_train={result['rows_train']}")
            print(f"rows_test={result['rows_test']}")
            print(f"champion_mae={champion_mae:.6f}")
            print(f"challenger_mae={result['mae']:.6f}")
            print(f"rmse={result['rmse']:.6f}")
            print(f"mape={result['mape']:.6f}" if result["mape"] is not None else "mape=None")
            print(f"promotion_decision={promotion_decision}")
            print(f"reason={reason}")


if __name__ == "__main__":
    main()
