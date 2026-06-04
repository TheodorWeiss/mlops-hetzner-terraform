#!/usr/bin/env python3

import argparse
import csv
import os
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values


NY_TZ = ZoneInfo("America/New_York")


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


def load_station_mapping(conn) -> Dict[str, str]:
    sql = """
        SELECT legacy_station_id, gbfs_station_id
        FROM station_id_mapping
        WHERE legacy_station_id IS NOT NULL;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    mapping = {legacy_id: gbfs_id for legacy_id, gbfs_id in rows}

    if not mapping:
        raise RuntimeError("station_id_mapping is empty. Run station_information parser first.")

    return mapping


def parse_local_ny_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            naive = datetime.strptime(text, fmt)
            return naive.replace(tzinfo=NY_TZ)
        except ValueError:
            continue

    return None


def bucket_hour(dt_ny: datetime) -> Tuple[datetime, datetime]:
    bucket_ny_aware = dt_ny.replace(minute=0, second=0, microsecond=0)
    bucket_utc = bucket_ny_aware.astimezone(timezone.utc)
    bucket_ny_naive = bucket_ny_aware.replace(tzinfo=None)
    return bucket_utc, bucket_ny_naive


def open_csv_rows(path: Path) -> Iterable[dict]:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as z:
            csv_names = sorted([n for n in z.namelist() if n.lower().endswith(".csv")])
            if not csv_names:
                raise ValueError(f"No CSV files found inside ZIP: {path}")

            for csv_name in csv_names:
                print(f"reading_csv_part={csv_name}")
                with z.open(csv_name) as f:
                    wrapper = (line.decode("utf-8-sig") for line in f)
                    reader = csv.DictReader(wrapper)
                    for row in reader:
                        yield row
    else:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row


def update_min_max(current_min, current_max, value):
    if value is None:
        return current_min, current_max

    if current_min is None or value < current_min:
        current_min = value

    if current_max is None or value > current_max:
        current_max = value

    return current_min, current_max


def aggregate_trip_file(path: Path, mapping: Dict[str, str], max_rows: Optional[int]):
    departures = Counter()
    returns = Counter()

    metrics = {
        "rows_total": 0,
        "rows_valid": 0,
        "rows_bad_started_at": 0,
        "rows_bad_ended_at": 0,
        "rows_missing_start_station_id": 0,
        "rows_missing_end_station_id": 0,
        "rows_unmapped_start_station_id": 0,
        "rows_unmapped_end_station_id": 0,
    }

    min_started_at_utc = None
    max_started_at_utc = None
    min_ended_at_utc = None
    max_ended_at_utc = None

    for row in open_csv_rows(path):
        metrics["rows_total"] += 1

        started_at = parse_local_ny_datetime(row.get("started_at"))
        ended_at = parse_local_ny_datetime(row.get("ended_at"))

        start_station_id = (row.get("start_station_id") or "").strip()
        end_station_id = (row.get("end_station_id") or "").strip()

        row_has_valid_event = False

        if started_at is None:
            metrics["rows_bad_started_at"] += 1
        elif not start_station_id:
            metrics["rows_missing_start_station_id"] += 1
        elif start_station_id not in mapping:
            metrics["rows_unmapped_start_station_id"] += 1
        else:
            bucket_utc, bucket_ny = bucket_hour(started_at)
            gbfs_station_id = mapping[start_station_id]
            departures[(bucket_utc, bucket_ny, gbfs_station_id, start_station_id)] += 1
            min_started_at_utc, max_started_at_utc = update_min_max(
                min_started_at_utc,
                max_started_at_utc,
                started_at.astimezone(timezone.utc),
            )
            row_has_valid_event = True

        if ended_at is None:
            metrics["rows_bad_ended_at"] += 1
        elif not end_station_id:
            metrics["rows_missing_end_station_id"] += 1
        elif end_station_id not in mapping:
            metrics["rows_unmapped_end_station_id"] += 1
        else:
            bucket_utc, bucket_ny = bucket_hour(ended_at)
            gbfs_station_id = mapping[end_station_id]
            returns[(bucket_utc, bucket_ny, gbfs_station_id, end_station_id)] += 1
            min_ended_at_utc, max_ended_at_utc = update_min_max(
                min_ended_at_utc,
                max_ended_at_utc,
                ended_at.astimezone(timezone.utc),
            )
            row_has_valid_event = True

        if row_has_valid_event:
            metrics["rows_valid"] += 1

        if metrics["rows_total"] % 500000 == 0:
            print(f"processed_rows={metrics['rows_total']}")

        if max_rows is not None and metrics["rows_total"] >= max_rows:
            print(f"max_rows_reached={max_rows}")
            break

    combined = defaultdict(lambda: {"departures": 0, "returns": 0})

    for key, value in departures.items():
        combined[key]["departures"] += value

    for key, value in returns.items():
        combined[key]["returns"] += value

    metrics["min_started_at_utc"] = min_started_at_utc
    metrics["max_started_at_utc"] = max_started_at_utc
    metrics["min_ended_at_utc"] = min_ended_at_utc
    metrics["max_ended_at_utc"] = max_ended_at_utc

    return combined, metrics


def write_results(conn, path: Path, aggregates, metrics, source_label: str):
    delete_sql = """
        DELETE FROM station_hourly_demand
        WHERE source_file = %s;
    """

    insert_sql = """
        INSERT INTO station_hourly_demand (
            bucket_start_utc,
            bucket_start_ny,
            gbfs_station_id,
            legacy_station_id,
            departures,
            returns,
            source_file
        )
        VALUES %s
        ON CONFLICT (bucket_start_utc, gbfs_station_id)
        DO UPDATE SET
            bucket_start_ny = EXCLUDED.bucket_start_ny,
            legacy_station_id = EXCLUDED.legacy_station_id,
            departures = EXCLUDED.departures,
            returns = EXCLUDED.returns,
            source_file = EXCLUDED.source_file,
            ingested_at = now();
    """

    log_sql = """
        INSERT INTO trip_ingestion_log (
            source_file,
            rows_total,
            rows_valid,
            rows_bad_started_at,
            rows_bad_ended_at,
            rows_missing_start_station_id,
            rows_missing_end_station_id,
            rows_unmapped_start_station_id,
            rows_unmapped_end_station_id,
            min_started_at_utc,
            max_started_at_utc,
            min_ended_at_utc,
            max_ended_at_utc,
            status,
            error
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    rows = [
        (
            bucket_utc,
            bucket_ny,
            gbfs_station_id,
            legacy_station_id,
            values["departures"],
            values["returns"],
            source_label,
        )
        for (bucket_utc, bucket_ny, gbfs_station_id, legacy_station_id), values in aggregates.items()
    ]

    with conn.cursor() as cur:
        cur.execute(delete_sql, (source_label,))

        if rows:
            execute_values(cur, insert_sql, rows, page_size=5000)

        cur.execute(
            log_sql,
            (
                source_label,
                metrics["rows_total"],
                metrics["rows_valid"],
                metrics["rows_bad_started_at"],
                metrics["rows_bad_ended_at"],
                metrics["rows_missing_start_station_id"],
                metrics["rows_missing_end_station_id"],
                metrics["rows_unmapped_start_station_id"],
                metrics["rows_unmapped_end_station_id"],
                metrics["min_started_at_utc"],
                metrics["max_started_at_utc"],
                metrics["min_ended_at_utc"],
                metrics["max_ended_at_utc"],
                "success",
                None,
            ),
        )

    conn.commit()

    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path",
        help="Path to Citi Bike trip CSV or ZIP file",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional smoke-test row limit",
    )
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        raise FileNotFoundError(path)

    source_label = str(path)
    if args.max_rows is not None:
        source_label = f"{path}::sample_{args.max_rows}"

    with get_connection() as conn:
        mapping = load_station_mapping(conn)
        print(f"station_mapping_size={len(mapping)}")

        aggregates, metrics = aggregate_trip_file(path, mapping, args.max_rows)
        inserted_rows = write_results(conn, path, aggregates, metrics, source_label)

    print("TRIP_CSV_HOURLY_PARSE_OK")
    print(f"source_file={source_label}")
    print(f"rows_total={metrics['rows_total']}")
    print(f"rows_valid={metrics['rows_valid']}")
    print(f"rows_bad_started_at={metrics['rows_bad_started_at']}")
    print(f"rows_bad_ended_at={metrics['rows_bad_ended_at']}")
    print(f"rows_missing_start_station_id={metrics['rows_missing_start_station_id']}")
    print(f"rows_missing_end_station_id={metrics['rows_missing_end_station_id']}")
    print(f"rows_unmapped_start_station_id={metrics['rows_unmapped_start_station_id']}")
    print(f"rows_unmapped_end_station_id={metrics['rows_unmapped_end_station_id']}")
    print(f"min_started_at_utc={metrics['min_started_at_utc']}")
    print(f"max_started_at_utc={metrics['max_started_at_utc']}")
    print(f"min_ended_at_utc={metrics['min_ended_at_utc']}")
    print(f"max_ended_at_utc={metrics['max_ended_at_utc']}")
    print(f"hourly_rows_inserted={inserted_rows}")


if __name__ == "__main__":
    main()
