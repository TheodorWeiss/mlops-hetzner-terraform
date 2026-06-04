#!/usr/bin/env python3

import re
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path


TRIPDATA_BASE_URL = "https://s3.amazonaws.com/tripdata"
LOCAL_DIR = Path("/mnt/mlops-data/incoming-raw/tripdata")
MARKER_FILE = LOCAL_DIR / "latest_new_file.txt"


def parse_yyyymm(filename: str):
    match = re.match(r"^(20\d{4})-citibike-tripdata", filename)
    return int(match.group(1)) if match else None


def next_month(yyyymm: int) -> int:
    year = yyyymm // 100
    month = yyyymm % 100
    month += 1
    if month == 13:
        year += 1
        month = 1
    return year * 100 + month


def month_iter(start_yyyymm: int, end_yyyymm: int):
    current = start_yyyymm
    while current <= end_yyyymm:
        yield f"{current}"
        current = next_month(current)


def url_exists(url: str) -> bool:
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return 200 <= response.status < 400
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise
    except urllib.error.URLError:
        return False


def download_file(url: str, target: Path) -> None:
    tmp_target = target.with_suffix(target.suffix + ".tmp")
    urllib.request.urlretrieve(url, tmp_target)
    tmp_target.replace(target)


def main() -> int:
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    local_zip_files = [p.name for p in LOCAL_DIR.glob("*.zip")]
    local_months = [parse_yyyymm(name) for name in local_zip_files]
    local_months = [m for m in local_months if m is not None]

    if not local_months:
        print("NO_LOCAL_TRIPDATA_FILES_FOUND")
        print("For safety, automatic backfill is disabled. Download an initial month manually first.")
        MARKER_FILE.write_text("", encoding="utf-8")
        return 0

    latest_local = max(local_months)
    start_check = next_month(latest_local)

    now = datetime.utcnow()
    current_yyyymm = now.year * 100 + now.month

    if start_check > current_yyyymm:
        print("NO_NEW_TRIPDATA_FILE")
        print(f"latest_local={latest_local}")
        MARKER_FILE.write_text("", encoding="utf-8")
        return 0

    print(f"latest_local={latest_local}")
    print(f"checking_from={start_check}")
    print(f"checking_to={current_yyyymm}")

    for yyyymm in month_iter(start_check, current_yyyymm):
        candidates = [
            f"{yyyymm}-citibike-tripdata.zip",
            f"{yyyymm}-citibike-tripdata.csv.zip",
        ]

        for filename in candidates:
            url = f"{TRIPDATA_BASE_URL}/{filename}"
            if url_exists(url):
                target = LOCAL_DIR / filename

                print(f"NEW_TRIPDATA_FILE_FOUND={filename}")
                print(f"DOWNLOAD_URL={url}")
                print(f"TARGET={target}")

                download_file(url, target)
                MARKER_FILE.write_text(str(target), encoding="utf-8")

                print("TRIPDATA_DOWNLOAD_OK")
                print(f"downloaded_file={target}")
                return 0

    print("NO_NEW_TRIPDATA_FILE")
    print(f"latest_local={latest_local}")
    MARKER_FILE.write_text("", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
