from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator


MARKER_FILE = "/mnt/mlops-data/incoming-raw/tripdata/latest_new_file.txt"


default_args = {
    "owner": "bikeml",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def has_new_tripdata_file() -> bool:
    try:
        with open(MARKER_FILE, "r", encoding="utf-8") as f:
            path = f.read().strip()
    except FileNotFoundError:
        return False

    return bool(path)


with DAG(
    dag_id="monthly_tripdata_ingestion",
    description=(
        "Check Citi Bike S3 for new monthly tripdata, ingest it, fetch matching "
        "NYC weather archive, and rebuild hourly demand/features."
    ),
    default_args=default_args,
    start_date=datetime(2026, 6, 3),
    schedule_interval="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bikeml", "tripdata", "weather", "monthly"],
) as dag:

    check_and_download_tripdata = BashOperator(
        task_id="check_and_download_tripdata",
        bash_command="python /opt/airflow/scripts/check_and_download_tripdata.py",
        execution_timeout=timedelta(minutes=20),
    )

    continue_if_new_file = ShortCircuitOperator(
        task_id="continue_if_new_file",
        python_callable=has_new_tripdata_file,
    )

    parse_new_tripdata = BashOperator(
        task_id="parse_new_tripdata",
        bash_command=(
            "NEW_FILE=$(cat /mnt/mlops-data/incoming-raw/tripdata/latest_new_file.txt) && "
            "echo \"Parsing $NEW_FILE\" && "
            "python /opt/airflow/scripts/parse_trip_csv_hourly.py \"$NEW_FILE\""
        ),
        execution_timeout=timedelta(hours=1),
    )

    fetch_weather_for_new_tripdata = BashOperator(
        task_id="fetch_weather_for_new_tripdata",
        bash_command="python /opt/airflow/scripts/fetch_weather_for_latest_tripdata.py",
        execution_timeout=timedelta(minutes=10),
    )

    build_station_hourly_features = BashOperator(
        task_id="build_station_hourly_features",
        bash_command="python /opt/airflow/scripts/build_station_hourly_features.py",
        execution_timeout=timedelta(minutes=30),
    )
    build_delta_training_features = BashOperator(
        task_id="build_delta_training_features",
        bash_command="python /opt/airflow/scripts/build_delta_training_features.py",
        execution_timeout=timedelta(minutes=30),
    )

    evaluate_current_champion_on_new_test = BashOperator(
        task_id="evaluate_current_champion_on_new_test",
        bash_command="python /opt/airflow/scripts/evaluate_current_champion_on_new_test.py",
        execution_timeout=timedelta(minutes=20),
    )

    promote_delta_candidate_if_passed = BashOperator(
        task_id="promote_delta_candidate_if_passed",
        bash_command="python /opt/airflow/scripts/promote_delta_candidate_if_passed.py",
        execution_timeout=timedelta(minutes=10),
    )
    train_delta_lightgbm_weather = BashOperator(
        task_id="train_delta_lightgbm_weather",
        bash_command="python /opt/airflow/scripts/train_delta_lightgbm_weather_mlflow.py",
        execution_timeout=timedelta(hours=1),
    )

    train_delta_xgboost_weather_challenger = BashOperator(
        task_id="train_delta_xgboost_weather_challenger",
        bash_command="python /opt/airflow/scripts/train_delta_xgboost_weather_challenger.py",
        execution_timeout=timedelta(hours=1),
    )
    (
        check_and_download_tripdata
        >> continue_if_new_file
        >> parse_new_tripdata
        >> fetch_weather_for_new_tripdata
        >> build_station_hourly_features
        >> build_delta_training_features
        >> evaluate_current_champion_on_new_test
        >> train_delta_lightgbm_weather
        >> promote_delta_candidate_if_passed
        >> train_delta_xgboost_weather_challenger
    )
