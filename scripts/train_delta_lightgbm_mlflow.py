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

TRAIN_START_NY = "2026-02-01 00:00:00"
TRAIN_END_NY = "2026-04-01 00:00:00"
TEST_START_NY = "2026-04-01 00:00:00"
TEST_END_NY = "2026-05-01 00:00:00"

BASELINE_MAE_THRESHOLD = 2.227188

FEATURE_COLUMNS = [
    "hour_ny",
    "day_of_week_ny",
    "is_weekend_ny",
    "month_ny",
    "lat",
    "lon",
    "capacity",
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
            delta_bikes,
            split
        FROM station_delta_training_features
        WHERE split IN ('train', 'test')
          AND lat IS NOT NULL
          AND lon IS NOT NULL
          AND capacity IS NOT NULL;
    """
    return pd.read_sql_query(sql, conn)


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
            TIMESTAMP '2026-02-01 00:00:00',
            TIMESTAMP '2026-04-01 00:00:00',
            TIMESTAMP '2026-04-01 00:00:00',
            TIMESTAMP '2026-05-01 00:00:00',
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
                "lightgbm_delta_time_geo_202604",
                "lightgbm_delta_time_geo",
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

        train = df[df["split"] == "train"].copy()
        test = df[df["split"] == "test"].copy()

        X_train = train[FEATURE_COLUMNS]
        y_train = train["delta_bikes"]

        X_test = test[FEATURE_COLUMNS]
        y_test = test["delta_bikes"]

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
            promotion_decision = "eligible_for_champion"
            decision_note = f"mae_delta={mae:.6f} beats zero-change baseline={BASELINE_MAE_THRESHOLD:.6f}"
        else:
            promotion_decision = "rejected_baseline_not_beaten"
            decision_note = f"mae_delta={mae:.6f} does not beat zero-change baseline={BASELINE_MAE_THRESHOLD:.6f}"

        with mlflow.start_run(run_name="lightgbm_delta_time_geo_202604") as run:
            run_id = run.info.run_id

            mlflow.log_params(params)
            mlflow.log_param("target_name", "delta_bikes")
            mlflow.log_param("feature_columns", ",".join(FEATURE_COLUMNS))
            mlflow.log_param("train_start_ny", TRAIN_START_NY)
            mlflow.log_param("train_end_ny", TRAIN_END_NY)
            mlflow.log_param("test_start_ny", TEST_START_NY)
            mlflow.log_param("test_end_ny", TEST_END_NY)
            mlflow.log_param("rows_train", len(train))
            mlflow.log_param("rows_test", len(test))
            mlflow.log_param("baseline_mae_threshold", BASELINE_MAE_THRESHOLD)
            mlflow.log_param("promotion_decision", promotion_decision)

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
            alias="champion" if promotion_decision == "eligible_for_champion" else "candidate",
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
            value="serving_delta_model",
        )
        client.set_model_version_tag(
            name=REGISTERED_MODEL_NAME,
            version=version.version,
            key="promotion_decision",
            value=promotion_decision,
        )

        result = {
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

    print("LIGHTGBM_DELTA_OK")
    print(f"postgres_eval_id={eval_id}")
    print(f"mlflow_run_id={run_id}")
    print(f"registered_model_name={REGISTERED_MODEL_NAME}")
    print(f"registered_model_version={version.version}")
    print(f"rows_train={len(train)}")
    print(f"rows_test={len(test)}")
    print(f"mae_delta={mae:.6f}")
    print(f"rmse_delta={rmse:.6f}")
    print(f"bias_delta={bias:.6f}")
    print(f"promotion_decision={promotion_decision}")
    print(f"notes={decision_note}")


if __name__ == "__main__":
    main()
