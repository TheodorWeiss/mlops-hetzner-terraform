#!/usr/bin/env python3

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values


LAT = 40.7128
LON = -74.0060
TIMEZONE = "America/New_York"

HOURLY = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]


def getenv_first(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def get_connection():
    return psycopg2.connect(
        host=getenv_first("POSTGRES_HOST", "PGHOST", default="postgres"),
        port=getenv_first("POSTGRES_PORT", "PGPORT", default="5432"),
        dbname=getenv_first("BIKEML_DB", "POSTGRES_DB", "PGDATABASE", default="bikeml"),
        user=getenv_first("POSTGRES_USER", "PGUSER", default="bikeml_admin"),
        password=getenv_first("POSTGRES_PASSWORD", "PGPASSWORD"),
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD")
    return parser.parse_args()


def fetch_weather(start_date: str, end_date: str):
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY),
        "timezone": TIMEZONE,
    }

    url = "https://archive-api.open-meteo.com/v1/archive?" + urllib.parse.urlencode(params)
    print("FETCH_URL=" + url, flush=True)

    with urllib.request.urlopen(url, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8"))

    return payload


def main():
    args = parse_args()

    payload = fetch_weather(args.start_date, args.end_date)
    hourly = payload["hourly"]

    times = hourly["time"]
    temps = hourly["temperature_2m"]
    precs = hourly["precipitation"]
    winds = hourly["wind_speed_10m"]
    codes = hourly["weather_code"]

    ny_tz = ZoneInfo(TIMEZONE)
    rows = []

    for t, temp, prec, wind, code in zip(times, temps, precs, winds, codes):
        local_dt = datetime.fromisoformat(t)
        aware_ny = local_dt.replace(tzinfo=ny_tz)
        utc_dt = aware_ny.astimezone(ZoneInfo("UTC"))

        is_rain = 1 if (prec is not None and prec > 0) else 0

        rows.append(
            (
                local_dt,
                utc_dt,
                temp,
                prec,
                wind,
                code,
                is_rain,
            )
        )

    sql = """
        INSERT INTO weather_hourly_nyc (
            weather_time_ny,
            weather_time_utc,
            temperature_2m,
            precipitation,
            wind_speed_10m,
            weather_code,
            is_rain
        )
        VALUES %s
        ON CONFLICT (weather_time_ny) DO UPDATE SET
            weather_time_utc = EXCLUDED.weather_time_utc,
            temperature_2m = EXCLUDED.temperature_2m,
            precipitation = EXCLUDED.precipitation,
            wind_speed_10m = EXCLUDED.wind_speed_10m,
            weather_code = EXCLUDED.weather_code,
            is_rain = EXCLUDED.is_rain;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS rows_for_period,
                    MIN(weather_time_ny) AS min_time,
                    MAX(weather_time_ny) AS max_time,
                    ROUND(AVG(temperature_2m)::numeric, 2) AS avg_temp,
                    ROUND(AVG(precipitation)::numeric, 4) AS avg_precip,
                    SUM(is_rain) AS rainy_hours
                FROM weather_hourly_nyc
                WHERE weather_time_ny >= %s::timestamp
                  AND weather_time_ny < (%s::date + INTERVAL '1 day');
                """,
                (args.start_date, args.end_date),
            )
            result = cur.fetchone()

    print("WEATHER_HOURLY_NYC_FETCH_OK")
    print(f"start_date={args.start_date}")
    print(f"end_date={args.end_date}")
    print(f"rows_for_period={result[0]}")
    print(f"min_time={result[1]}")
    print(f"max_time={result[2]}")
    print(f"avg_temp={result[3]}")
    print(f"avg_precip={result[4]}")
    print(f"rainy_hours={result[5]}")


if __name__ == "__main__":
    main()
