#!/usr/bin/env python3

import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values


DEFAULT_LATEST_FILE = Path(
    "/mnt/mlops-data/incoming-raw/gbfs/latest/station_status_latest.json"
)
DEFAULT_ARCHIVE_DIR = Path(
    "/mnt/mlops-data/incoming-raw/gbfs/station_status"
)

DEFAULT_STALE_MAX_AGE_SECONDS = 3600


def getenv_first(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def find_station_status_file() -> Path:
    latest_file = Path(os.getenv("STATION_STATUS_LATEST_FILE", str(DEFAULT_LATEST_FILE)))

    if latest_file.exists():
        return latest_file

    archive_dir = Path(os.getenv("STATION_STATUS_ARCHIVE_DIR", str(DEFAULT_ARCHIVE_DIR)))
    candidates = sorted(
        archive_dir.glob("**/*.json.gz"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not candidates:
        raise FileNotFoundError(
            f"No station_status file found. Checked latest={latest_file} "
            f"and archive_dir={archive_dir}"
        )

    return candidates[0]


def load_json(path: Path) -> Dict[str, Any]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def epoch_to_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None

    text = str(value).strip()
    return text if text else None


def normalize_int(value: Any) -> Optional[int]:
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def prepare_rows(
    payload: Dict[str, Any],
    source_file: Path,
) -> Tuple[List[Tuple[Any, ...]], Dict[str, Any]]:
    stations = payload.get("data", {}).get("stations", [])
    if not isinstance(stations, list):
        raise ValueError("Invalid station_status JSON: data.stations is not a list")

    snapshot_epoch = payload.get("last_updated")
    snapshot_time = epoch_to_utc(snapshot_epoch)

    if snapshot_time is None:
        raise ValueError("Invalid station_status JSON: missing or invalid last_updated")

    stale_max_age_seconds = int(
        os.getenv("STATION_STATUS_STALE_MAX_AGE_SECONDS", str(DEFAULT_STALE_MAX_AGE_SECONDS))
    )

    rows: List[Tuple[Any, ...]] = []

    rows_total = len(stations)
    rows_filtered_inactive = 0
    rows_filtered_bad_last_reported = 0
    rows_filtered_stale = 0
    rows_filtered_ebikes_gt_bikes = 0

    for station in stations:
        if not isinstance(station, dict):
            continue

        gbfs_station_id = normalize_text(station.get("station_id"))
        if not gbfs_station_id:
            continue

        is_installed = normalize_int(station.get("is_installed"))
        is_renting = normalize_int(station.get("is_renting"))
        is_returning = normalize_int(station.get("is_returning"))

        if is_installed != 1 or is_renting != 1 or is_returning != 1:
            rows_filtered_inactive += 1
            continue

        last_reported = normalize_int(station.get("last_reported"))

        if last_reported is None or last_reported <= 0 or last_reported == 86400:
            rows_filtered_bad_last_reported += 1
            continue

        data_age_seconds = int(int(snapshot_epoch) - int(last_reported))

        if data_age_seconds < 0 or data_age_seconds > stale_max_age_seconds:
            rows_filtered_stale += 1
            continue

        num_bikes_available = normalize_int(station.get("num_bikes_available"))
        num_ebikes_available = normalize_int(station.get("num_ebikes_available"))
        num_docks_available = normalize_int(station.get("num_docks_available"))
        num_bikes_disabled = normalize_int(station.get("num_bikes_disabled"))
        num_docks_disabled = normalize_int(station.get("num_docks_disabled"))

        if (
            num_bikes_available is not None
            and num_ebikes_available is not None
            and num_ebikes_available > num_bikes_available
        ):
            rows_filtered_ebikes_gt_bikes += 1
            continue

        rows.append(
            (
                snapshot_time,
                gbfs_station_id,
                num_bikes_available,
                num_ebikes_available,
                num_docks_available,
                num_bikes_disabled,
                num_docks_disabled,
                is_installed,
                is_renting,
                is_returning,
                last_reported,
                data_age_seconds,
                str(source_file),
            )
        )

    metrics = {
        "rows_total": rows_total,
        "rows_valid": len(rows),
        "rows_filtered_inactive": rows_filtered_inactive,
        "rows_filtered_bad_last_reported": rows_filtered_bad_last_reported,
        "rows_filtered_stale": rows_filtered_stale,
        "rows_filtered_ebikes_gt_bikes": rows_filtered_ebikes_gt_bikes,
        "snapshot_time": snapshot_time,
        "stale_max_age_seconds": stale_max_age_seconds,
    }

    return rows, metrics


def insert_status_snapshots(
    rows: List[Tuple[Any, ...]],
    metrics: Dict[str, Any],
    source_file: Path,
) -> int:
    insert_sql = """
        INSERT INTO gbfs_status_snapshots (
            snapshot_time,
            gbfs_station_id,
            num_bikes_available,
            num_ebikes_available,
            num_docks_available,
            num_bikes_disabled,
            num_docks_disabled,
            is_installed,
            is_renting,
            is_returning,
            last_reported,
            data_age_seconds,
            source_file
        )
        VALUES %s
        ON CONFLICT (snapshot_time, gbfs_station_id)
        DO UPDATE SET
            num_bikes_available = EXCLUDED.num_bikes_available,
            num_ebikes_available = EXCLUDED.num_ebikes_available,
            num_docks_available = EXCLUDED.num_docks_available,
            num_bikes_disabled = EXCLUDED.num_bikes_disabled,
            num_docks_disabled = EXCLUDED.num_docks_disabled,
            is_installed = EXCLUDED.is_installed,
            is_renting = EXCLUDED.is_renting,
            is_returning = EXCLUDED.is_returning,
            last_reported = EXCLUDED.last_reported,
            data_age_seconds = EXCLUDED.data_age_seconds,
            source_file = EXCLUDED.source_file,
            ingested_at = now();
    """

    log_sql = """
        INSERT INTO gbfs_ingestion_log (
            dag_run_id,
            task_id,
            source_file,
            file_type,
            rows_total,
            rows_valid,
            rows_filtered_inactive,
            rows_filtered_bad_last_reported,
            rows_filtered_stale,
            rows_filtered_ebikes_gt_bikes,
            status,
            error
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    status = "success"
    error = None

    with get_connection() as conn:
        with conn.cursor() as cur:
            if rows:
                execute_values(cur, insert_sql, rows, page_size=1000)

            cur.execute(
                log_sql,
                (
                    os.getenv("AIRFLOW_CTX_DAG_RUN_ID"),
                    os.getenv("AIRFLOW_CTX_TASK_ID", "manual_parse_station_status"),
                    str(source_file),
                    "station_status",
                    metrics["rows_total"],
                    metrics["rows_valid"],
                    metrics["rows_filtered_inactive"],
                    metrics["rows_filtered_bad_last_reported"],
                    metrics["rows_filtered_stale"],
                    metrics["rows_filtered_ebikes_gt_bikes"],
                    status,
                    error,
                ),
            )

        conn.commit()

    return len(rows)


def main() -> None:
    source_file = find_station_status_file()
    payload = load_json(source_file)
    rows, metrics = prepare_rows(payload, source_file)
    inserted_rows = insert_status_snapshots(rows, metrics, source_file)

    print("STATION_STATUS_PARSE_OK")
    print(f"source_file={source_file}")
    print(f"snapshot_time_utc={metrics['snapshot_time'].isoformat()}")
    print(f"rows_total={metrics['rows_total']}")
    print(f"rows_valid={metrics['rows_valid']}")
    print(f"rows_filtered_inactive={metrics['rows_filtered_inactive']}")
    print(f"rows_filtered_bad_last_reported={metrics['rows_filtered_bad_last_reported']}")
    print(f"rows_filtered_stale={metrics['rows_filtered_stale']}")
    print(f"rows_filtered_ebikes_gt_bikes={metrics['rows_filtered_ebikes_gt_bikes']}")
    print(f"stale_max_age_seconds={metrics['stale_max_age_seconds']}")
    print(f"inserted_rows={inserted_rows}")


if __name__ == "__main__":
    main()
