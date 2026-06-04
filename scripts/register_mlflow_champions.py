#!/usr/bin/env python3

import os

from mlflow.tracking import MlflowClient
import mlflow


# MinIO/S3 artifact access from Airflow container.
os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", ""))
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")


TRACKING_URI = "http://mlflow:5000"
EXPERIMENT_NAME = "bikeml-demand-forecasting"

MODELS = {
    "departures": {
        "registered_model_name": "bikeml_departures_forecaster",
        "run_name": "lightgbm_poisson_departures_202604",
    },
    "returns": {
        "registered_model_name": "bikeml_returns_forecaster",
        "run_name": "lightgbm_poisson_returns_202604",
    },
}


def find_latest_finished_run(client: MlflowClient, experiment_id: str, run_name: str):
    runs = client.search_runs(
        experiment_ids=[experiment_id],
        filter_string=f"tags.mlflow.runName = '{run_name}' and attributes.status = 'FINISHED'",
        order_by=["attributes.start_time DESC"],
        max_results=1,
    )

    if not runs:
        raise RuntimeError(f"No FINISHED run found for run_name={run_name}")

    return runs[0]


def ensure_registered_model(client: MlflowClient, model_name: str) -> None:
    try:
        client.get_registered_model(model_name)
    except Exception:
        client.create_registered_model(model_name)


def main() -> None:
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient(tracking_uri=TRACKING_URI)

    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is None:
        raise RuntimeError(f"Experiment not found: {EXPERIMENT_NAME}")

    for target_name, config in MODELS.items():
        model_name = config["registered_model_name"]
        run_name = config["run_name"]

        run = find_latest_finished_run(client, experiment.experiment_id, run_name)
        run_id = run.info.run_id
        source_uri = f"runs:/{run_id}/model"

        ensure_registered_model(client, model_name)

        version = mlflow.register_model(
            model_uri=source_uri,
            name=model_name,
        )

        client.set_registered_model_alias(
            name=model_name,
            alias="champion",
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
            key="source_run_id",
            value=run_id,
        )

        client.set_model_version_tag(
            name=model_name,
            version=version.version,
            key="train_period_ny",
            value="2026-02-01..2026-04-01",
        )

        client.set_model_version_tag(
            name=model_name,
            version=version.version,
            key="test_period_ny",
            value="2026-04-01..2026-05-01",
        )

        print("MLFLOW_MODEL_REGISTERED_OK")
        print(f"target_name={target_name}")
        print(f"model_name={model_name}")
        print(f"version={version.version}")
        print(f"alias=champion")
        print(f"source_run_id={run_id}")
        print(f"source_uri={source_uri}")


if __name__ == "__main__":
    main()
