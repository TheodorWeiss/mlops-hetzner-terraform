#!/usr/bin/env python3

import re
import subprocess
from datetime import date, timedelta
from pathlib import Path


MARKER_FILE = Path("/mnt/mlops-data/incoming-raw/tripdata/latest_new_file.txt")


def main():
    tripdata_path = MARKER_FILE.read_text(encoding="utf-8").strip()
    if not tripdata_path:
        raise RuntimeError(f"Marker file is empty: {MARKER_FILE}")

    basename = Path(tripdata_path).name
    match = re.search(r"(20[0-9]{4})-citibike-tripdata", basename)
    if not match:
        raise RuntimeError(f"Could not extract YYYYMM from tripdata file name: {basename}")

    yyyymm = match.group(1)
    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])

    start = date(year, month, 1)
    next_month = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    end = next_month - timedelta(days=1)

    print("WEATHER_FOR_TRIPDATA_MONTH")
    print(f"tripdata_file={tripdata_path}")
    print(f"yyyymm={yyyymm}")
    print(f"start_date={start.isoformat()}")
    print(f"end_date={end.isoformat()}")

    subprocess.run(
        [
            "python",
            "/opt/airflow/scripts/fetch_weather_hourly_nyc.py",
            "--start-date",
            start.isoformat(),
            "--end-date",
            end.isoformat(),
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
