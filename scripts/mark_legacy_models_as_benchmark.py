#!/usr/bin/env python3

import mlflow
from mlflow.tracking import MlflowClient


TRACKING_URI = "http://mlflow:5000"

LEGACY_MODELS = [
    "bikeml_departures_forecaster",
    "bikeml_returns_forecaster",
]


def main():
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient(TRACKING_URI)

    for model_name in LEGACY_MODELS:
        model = client.get_registered_model(model_name)

        client.set_registered_model_tag(
            name=model_name,
            key="role",
            value="offline_upper_bound_benchmark",
        )
        client.set_registered_model_tag(
            name=model_name,
            key="serving",
            value="false",
        )
        client.set_registered_model_tag(
            name=model_name,
            key="reason_not_serving",
            value="uses trip-flow lag features unavailable at live inference time",
        )

        print(f"MODEL_MARKED_AS_BENCHMARK {model.name}")

        for version in model.latest_versions:
            client.set_model_version_tag(
                name=model_name,
                version=version.version,
                key="role",
                value="offline_upper_bound_benchmark",
            )
            client.set_model_version_tag(
                name=model_name,
                version=version.version,
                key="serving",
                value="false",
            )
            print(f"  version={version.version} marked")


if __name__ == "__main__":
    main()
