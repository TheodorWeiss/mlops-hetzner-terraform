#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


LOCAL_RAW_ROOT = Path("/mnt/mlops-data/incoming-raw/gbfs")
COLLECTOR_STATUS_PATH = LOCAL_RAW_ROOT / "latest" / "collector_status.json"
STATION_STATUS_ROOT = LOCAL_RAW_ROOT / "station_status"

FILENAME_PATTERN = re.compile(r"station_status_(\d{8})_(\d{6})\.json\.gz$")


def parse_iso_utc(value: str) -> datetime:
    if not value:
        raise ValueError("empty timestamp")

    # Supports both "...Z" and "+00:00"
    value = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def parse_snapshot_time_from_filename(path: Path) -> datetime:
    match = FILENAME_PATTERN.search(path.name)
    if not match:
        raise ValueError(f"Cannot parse snapshot timestamp from filename: {path}")

    date_part, time_part = match.groups()
    dt = datetime.strptime(date_part + time_part, "%Y%m%d%H%M%S")
    return dt.replace(tzinfo=timezone.utc)


def find_latest_station_status_file() -> Path:
    files = sorted(STATION_STATUS_ROOT.rglob("station_status_*.json.gz"))
    if not files:
        raise FileNotFoundError(f"No station_status files found under {STATION_STATUS_ROOT}")
    return files[-1]


def fail(message: str) -> None:
    print(f"FRESHNESS_CHECK_FAILED: {message}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    max_age_minutes = int(os.getenv("UPSTREAM_FRESHNESS_MAX_AGE_MINUTES", "30"))
    max_age_seconds = max_age_minutes * 60

    now = datetime.now(timezone.utc)

    print("=== BikeML upstream freshness check started ===")
    print(f"now_utc={now.isoformat()}")
    print(f"max_age_minutes={max_age_minutes}")

    if not COLLECTOR_STATUS_PATH.exists():
        fail(f"collector_status.json not found: {COLLECTOR_STATUS_PATH}")

    with COLLECTOR_STATUS_PATH.open("r", encoding="utf-8") as f:
        status_doc = json.load(f)

    status = status_doc.get("status")
    last_success_utc_raw = status_doc.get("last_success_utc")
    station_status_rows = int(status_doc.get("station_status_rows") or 0)
    last_station_status_file = status_doc.get("last_station_status_file")
    error = status_doc.get("error")

    print(f"collector_status.status={status}")
    print(f"collector_status.last_success_utc={last_success_utc_raw}")
    print(f"collector_status.station_status_rows={station_status_rows}")
    print(f"collector_status.last_station_status_file={last_station_status_file}")
    print(f"collector_status.error={error}")

    if status != "ok":
        fail(f"collector status is not ok: status={status}, error={error}")

    if station_status_rows <= 0:
        fail(f"station_status_rows must be > 0, got {station_status_rows}")

    try:
        last_success_utc = parse_iso_utc(last_success_utc_raw)
    except Exception as exc:
        fail(f"cannot parse last_success_utc={last_success_utc_raw!r}: {exc}")

    last_success_age_seconds = (now - last_success_utc).total_seconds()
    print(f"last_success_age_seconds={int(last_success_age_seconds)}")

    if last_success_age_seconds < 0:
        fail(f"last_success_utc is in the future: age={last_success_age_seconds}")

    if last_success_age_seconds > max_age_seconds:
        fail(
            f"collector last_success_utc is stale: "
            f"age={int(last_success_age_seconds)}s > threshold={max_age_seconds}s"
        )

    latest_file = find_latest_station_status_file()
    latest_file_time = parse_snapshot_time_from_filename(latest_file)
    latest_file_age_seconds = (now - latest_file_time).total_seconds()

    print(f"latest_station_status_file={latest_file}")
    print(f"latest_station_status_file_time_utc={latest_file_time.isoformat()}")
    print(f"latest_station_status_file_age_seconds={int(latest_file_age_seconds)}")

    if latest_file_age_seconds < 0:
        fail(f"latest station_status file timestamp is in the future: age={latest_file_age_seconds}")

    if latest_file_age_seconds > max_age_seconds:
        fail(
            f"latest station_status file is stale: "
            f"age={int(latest_file_age_seconds)}s > threshold={max_age_seconds}s"
        )

    print("FRESHNESS_CHECK_OK")
    print("=== BikeML upstream freshness check finished ===")


if __name__ == "__main__":
    main()
