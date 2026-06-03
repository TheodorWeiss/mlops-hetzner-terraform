#!/usr/bin/env python3

import gzip
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values


DEFAULT_LATEST_FILE = Path(
    "/mnt/mlops-data/incoming-raw/gbfs/latest/station_information_latest.json"
)
DEFAULT_ARCHIVE_DIR = Path(
    "/mnt/mlops-data/incoming-raw/gbfs/station_information"
)


def getenv_first(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def find_station_information_file() -> Path:
    latest_file = Path(os.getenv("STATION_INFORMATION_LATEST_FILE", str(DEFAULT_LATEST_FILE)))

    if latest_file.exists():
        return latest_file

    archive_dir = Path(os.getenv("STATION_INFORMATION_ARCHIVE_DIR", str(DEFAULT_ARCHIVE_DIR)))
    candidates = sorted(
        archive_dir.glob("**/*.json.gz"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not candidates:
        raise FileNotFoundError(
            f"No station_information file found. Checked latest={latest_file} "
            f"and archive_dir={archive_dir}"
        )

    return candidates[0]


def load_json(path: Path) -> Dict[str, Any]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def epoch_to_timestamptz(value: Any) -> Optional[datetime]:
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


def normalize_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def prepare_rows(
    payload: Dict[str, Any],
    source_file: Path,
) -> Tuple[List[Tuple[Any, ...]], Dict[str, Any]]:
    stations = payload.get("data", {}).get("stations", [])
    if not isinstance(stations, list):
        raise ValueError("Invalid station_information JSON: data.stations is not a list")

    station_information_updated_at = epoch_to_timestamptz(payload.get("last_updated"))

    rows: List[Tuple[Any, ...]] = []
    short_names: List[str] = []

    for station in stations:
        if not isinstance(station, dict):
            continue

        gbfs_station_id = normalize_text(station.get("station_id"))
        if not gbfs_station_id:
            continue

        legacy_station_id = normalize_text(station.get("short_name"))

        if legacy_station_id:
            short_names.append(legacy_station_id)

        rental_methods = station.get("rental_methods")
        if rental_methods is None:
            rental_methods_json = None
        else:
            rental_methods_json = Json(rental_methods)

        rows.append(
            (
                gbfs_station_id,
                legacy_station_id,
                normalize_text(station.get("name")),
                normalize_float(station.get("lat")),
                normalize_float(station.get("lon")),
                normalize_int(station.get("capacity")),
                normalize_text(station.get("region_id")),
                rental_methods_json,
                str(source_file),
                station_information_updated_at,
            )
        )

    stations_total = len(stations)
    stations_with_short_name = len(short_names)
    unique_short_names = len(set(short_names))
    duplicate_short_names = sum(
        count - 1 for count in Counter(short_names).values() if count > 1
    )
    coverage_ratio = (
        stations_with_short_name / stations_total
        if stations_total > 0
        else 0.0
    )

    metrics = {
        "stations_total": stations_total,
        "stations_with_short_name": stations_with_short_name,
        "unique_short_names": unique_short_names,
        "duplicate_short_names": duplicate_short_names,
        "coverage_ratio": coverage_ratio,
        "valid_rows": len(rows),
    }

    return rows, metrics


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


def upsert_mapping(rows: List[Tuple[Any, ...]], metrics: Dict[str, Any], source_file: Path) -> int:
    if not rows:
        raise ValueError("No valid station rows to upsert")

    upsert_sql = """
        INSERT INTO station_id_mapping (
            gbfs_station_id,
            legacy_station_id,
            name,
            lat,
            lon,
            capacity,
            region_id,
            rental_methods,
            source_file,
            station_information_updated_at
        )
        VALUES %s
        ON CONFLICT (gbfs_station_id)
        DO UPDATE SET
            legacy_station_id = EXCLUDED.legacy_station_id,
            name = EXCLUDED.name,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            capacity = EXCLUDED.capacity,
            region_id = EXCLUDED.region_id,
            rental_methods = EXCLUDED.rental_methods,
            source_file = EXCLUDED.source_file,
            station_information_updated_at = EXCLUDED.station_information_updated_at,
            ingested_at = now();
    """

    quality_sql = """
        INSERT INTO mapping_quality_report (
            station_information_file,
            stations_total,
            stations_with_short_name,
            unique_short_names,
            duplicate_short_names,
            coverage_ratio,
            notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    notes = (
        "station_information parsed successfully; "
        f"valid_rows={metrics['valid_rows']}"
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows, page_size=1000)

            cur.execute(
                quality_sql,
                (
                    str(source_file),
                    metrics["stations_total"],
                    metrics["stations_with_short_name"],
                    metrics["unique_short_names"],
                    metrics["duplicate_short_names"],
                    metrics["coverage_ratio"],
                    notes,
                ),
            )

        conn.commit()

    return len(rows)


def main() -> None:
    source_file = find_station_information_file()
    payload = load_json(source_file)
    rows, metrics = prepare_rows(payload, source_file)
    upserted_rows = upsert_mapping(rows, metrics, source_file)

    print("STATION_INFORMATION_PARSE_OK")
    print(f"source_file={source_file}")
    print(f"stations_total={metrics['stations_total']}")
    print(f"stations_with_short_name={metrics['stations_with_short_name']}")
    print(f"unique_short_names={metrics['unique_short_names']}")
    print(f"duplicate_short_names={metrics['duplicate_short_names']}")
    print(f"coverage_ratio={metrics['coverage_ratio']:.6f}")
    print(f"upserted_rows={upserted_rows}")


if __name__ == "__main__":
    main()
