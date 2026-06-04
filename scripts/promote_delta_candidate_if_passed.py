#!/usr/bin/env python3

import os
from typing import Optional

import mlflow
import psycopg2
from mlflow.tracking import MlflowClient


os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", ""))
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")


TRACKING_URI = "http://mlflow:5000"
REGISTERED_MODEL_NAME = "bikeml_delta_bikes_forecaster"

MIN_MAE_IMPROVEMENT_RATIO = 0.01
BASELINE_MAE_THRESHOLD = 2.227188


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


def get_latest_eval(conn, *, model_type: str, version: str):
    sql = """
        SELECT
            id,
            rows_test,
            test_start_ny,
            test_end_ny,
            mae_delta,
            rmse_delta,
            bias_delta
        FROM delta_model_evaluation_runs
        WHERE model_type = %s
          AND registered_model_version = %s
        ORDER BY created_at DESC
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (model_type, version))
        row = cur.fetchone()
    return row


def update_eval_decision(conn, *, eval_id: int, decision: str, note: str):
    sql = """
        UPDATE delta_model_evaluation_runs
        SET
            promotion_decision = %s,
            notes = %s || ' ' || COALESCE(notes, '')
        WHERE id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (decision, note, eval_id))
    conn.commit()


def main():
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient(TRACKING_URI)

    champion = client.get_model_version_by_alias(REGISTERED_MODEL_NAME, "champion")
    candidate = client.get_model_version_by_alias(REGISTERED_MODEL_NAME, "candidate")

    champion_version = str(champion.version)
    candidate_version = str(candidate.version)

    with get_connection() as conn:
        champion_eval = get_latest_eval(
            conn,
            model_type="current_champion_reeval_on_test",
            version=champion_version,
        )

        candidate_eval = get_latest_eval(
            conn,
            model_type="lightgbm_delta_time_geo_weather",
            version=candidate_version,
        )

        if champion_eval is None:
            raise RuntimeError(
                f"Нет re-eval записи для текущего champion version={champion_version}. "
                "Сначала запусти evaluate_current_champion_on_new_test.py"
            )

        if candidate_eval is None:
            raise RuntimeError(
                f"Нет eval записи для candidate version={candidate_version}. "
                "Сначала запусти train_delta_lightgbm_weather_mlflow.py"
            )

        (
            champion_eval_id,
            champion_rows_test,
            champion_test_start,
            champion_test_end,
            champion_mae,
            champion_rmse,
            champion_bias,
        ) = champion_eval

        (
            candidate_eval_id,
            candidate_rows_test,
            candidate_test_start,
            candidate_test_end,
            candidate_mae,
            candidate_rmse,
            candidate_bias,
        ) = candidate_eval

        if (
            champion_rows_test != candidate_rows_test
            or champion_test_start != candidate_test_start
            or champion_test_end != candidate_test_end
        ):
            raise RuntimeError(
                "Нечестное сравнение: champion и candidate оценены на разных test split. "
                f"champion rows/start/end={champion_rows_test}/{champion_test_start}/{champion_test_end}; "
                f"candidate rows/start/end={candidate_rows_test}/{candidate_test_start}/{candidate_test_end}"
            )

        champion_mae = float(champion_mae)
        candidate_mae = float(candidate_mae)
        candidate_bias = float(candidate_bias)

        required_mae = champion_mae * (1.0 - MIN_MAE_IMPROVEMENT_RATIO)

        passes_champion_gate = candidate_mae <= required_mae
        passes_baseline_gate = candidate_mae < BASELINE_MAE_THRESHOLD

        print("FAIR_PROMOTION_GATE")
        print(f"champion_version={champion_version}")
        print(f"candidate_version={candidate_version}")
        print(f"rows_test={candidate_rows_test}")
        print(f"test_start_ny={candidate_test_start}")
        print(f"test_end_ny={candidate_test_end}")
        print(f"champion_mae={champion_mae:.6f}")
        print(f"candidate_mae={candidate_mae:.6f}")
        print(f"required_mae={required_mae:.6f}")
        print(f"baseline_mae={BASELINE_MAE_THRESHOLD:.6f}")
        print(f"candidate_bias={candidate_bias:.6f}")

        if passes_champion_gate and passes_baseline_gate:
            decision = "promoted_after_fair_gate"
            note = (
                f"Честный gate пройден: candidate_mae={candidate_mae:.6f} <= "
                f"required_mae={required_mae:.6f} и лучше baseline={BASELINE_MAE_THRESHOLD:.6f}. "
                f"Champion переключён с v{champion_version} на v{candidate_version}."
            )

            client.set_registered_model_alias(
                name=REGISTERED_MODEL_NAME,
                alias="champion",
                version=candidate_version,
            )

            client.set_model_version_tag(
                name=REGISTERED_MODEL_NAME,
                version=candidate_version,
                key="role",
                value="serving_delta_weather_champion",
            )
            client.set_model_version_tag(
                name=REGISTERED_MODEL_NAME,
                version=candidate_version,
                key="promotion_decision",
                value=decision,
            )
            client.set_model_version_tag(
                name=REGISTERED_MODEL_NAME,
                version=candidate_version,
                key="reason",
                value=note,
            )

            client.set_model_version_tag(
                name=REGISTERED_MODEL_NAME,
                version=champion_version,
                key="role",
                value="previous_delta_weather_champion",
            )

            update_eval_decision(
                conn,
                eval_id=candidate_eval_id,
                decision=decision,
                note=note,
            )

            print("PROMOTION_OK")
            print(note)

        else:
            decision = "rejected_keep_champion"
            reason_parts = []

            if not passes_champion_gate:
                reason_parts.append(
                    f"candidate_mae={candidate_mae:.6f} не лучше required_mae={required_mae:.6f}"
                )

            if not passes_baseline_gate:
                reason_parts.append(
                    f"candidate_mae={candidate_mae:.6f} не лучше baseline={BASELINE_MAE_THRESHOLD:.6f}"
                )

            note = (
                "Честный gate не пройден; текущий champion сохранён. "
                + "; ".join(reason_parts)
            )

            client.set_model_version_tag(
                name=REGISTERED_MODEL_NAME,
                version=candidate_version,
                key="role",
                value="serving_delta_weather_candidate",
            )
            client.set_model_version_tag(
                name=REGISTERED_MODEL_NAME,
                version=candidate_version,
                key="promotion_decision",
                value=decision,
            )
            client.set_model_version_tag(
                name=REGISTERED_MODEL_NAME,
                version=candidate_version,
                key="reason",
                value=note,
            )

            update_eval_decision(
                conn,
                eval_id=candidate_eval_id,
                decision=decision,
                note=note,
            )

            print("PROMOTION_REJECTED_KEEP_CHAMPION")
            print(note)


if __name__ == "__main__":
    main()
