from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "bikeml",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id="gbfs_ingestion_bridge",
    description="Sync raw GBFS files from Persistent Raw Ingestion Server to MinIO raw zone",
    default_args=default_args,
    start_date=datetime(2026, 6, 3),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bikeml", "gbfs", "ingestion", "raw"],
) as dag:

    sync_gbfs_raw = BashOperator(
        task_id="sync_gbfs_raw_to_minio",
        # NOTE: The trailing space is intentional.
        # Without it, Airflow may treat a command ending with ".sh" as a Jinja template file.
        bash_command="bash /opt/airflow/scripts/sync_gbfs_raw.sh ",
        execution_timeout=timedelta(minutes=20),
    )

    check_upstream_freshness = BashOperator(
        task_id="check_upstream_freshness",
        bash_command="python /opt/airflow/scripts/check_upstream_freshness.py",
        execution_timeout=timedelta(minutes=3),
    )

    parse_station_information = BashOperator(
        task_id="parse_station_information",
        bash_command="python /opt/airflow/scripts/parse_station_information.py",
    )

    parse_station_status = BashOperator(
        task_id="parse_station_status",
        bash_command="python /opt/airflow/scripts/parse_station_status.py",
    )
    
    build_gbfs_online_features = BashOperator(
        task_id="build_gbfs_online_features",
        bash_command="python /opt/airflow/scripts/build_gbfs_online_features.py",
        execution_timeout=timedelta(minutes=5),
    )
    sync_gbfs_raw >> check_upstream_freshness >> parse_station_information >> parse_station_status >> build_gbfs_online_features

