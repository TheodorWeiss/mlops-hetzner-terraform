#!/usr/bin/env python3

import os
import subprocess

import mlflow
from mlflow.tracking import MlflowClient


os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", ""))
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")


TRACKING_URI = "http://mlflow:5000"
REGISTERED_MODEL_NAME = "bikeml_delta_bikes_forecaster"
ALIAS = "champion"


def main():
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient(TRACKING_URI)

    champion = client.get_model_version_by_alias(REGISTERED_MODEL_NAME, ALIAS)
    champion_version = str(champion.version)
    model_uri = f"models:/{REGISTERED_MODEL_NAME}/{champion_version}"

    print("ПЕРЕОЦЕНКА_ТЕКУЩЕГО_CHAMPION")
    print(f"model_name={REGISTERED_MODEL_NAME}")
    print(f"alias={ALIAS}")
    print(f"champion_version={champion_version}")
    print(f"champion_run_id={champion.run_id}")
    print(f"model_uri={model_uri}")

    subprocess.run(
        [
            "python",
            "/opt/airflow/scripts/evaluate_delta_model_on_current_test.py",
            "--model-uri",
            model_uri,
            "--model-version",
            champion_version,
            "--label",
            "current_champion_reeval_on_test",
            "--promotion-decision",
            "champion_reevaluated_on_current_test",
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
