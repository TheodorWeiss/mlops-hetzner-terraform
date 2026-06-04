#!/usr/bin/env python3

import os
from typing import Optional

import psycopg2


MIN_TRAIN_START_NY = "2026-02-01 00:00:00"


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


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Important:
            # We determine the latest complete month from source_file, not from max(bucket_start_ny).
            # Monthly trip files can contain small spillover rows into the next month.
            cur.execute(
                """
                WITH source_months AS (
                    SELECT DISTINCT
                        SUBSTRING(source_file FROM '(20[0-9]{4})-citibike-tripdata') AS yyyymm
                    FROM station_hourly_demand
                    WHERE source_file IS NOT NULL
                )
                SELECT MAX(yyyymm)
                FROM source_months
                WHERE yyyymm IS NOT NULL;
                """
            )
            latest_yyyymm = cur.fetchone()[0]

            if latest_yyyymm is None:
                raise RuntimeError("Could not determine latest complete month from source_file.")

            latest_year = int(latest_yyyymm[:4])
            latest_month = int(latest_yyyymm[4:6])

            cur.execute(
                """
                SELECT
                    TO_DATE(%s, 'YYYYMM')::timestamp AS test_start_ny,
                    (TO_DATE(%s, 'YYYYMM') + INTERVAL '1 month')::timestamp AS test_end_ny,
                    %s::timestamp AS train_start_ny,
                    TO_DATE(%s, 'YYYYMM')::timestamp AS train_end_ny;
                """,
                (
                    latest_yyyymm,
                    latest_yyyymm,
                    MIN_TRAIN_START_NY,
                    latest_yyyymm,
                ),
            )
            test_start_ny, test_end_ny, train_start_ny, train_end_ny = cur.fetchone()

            if train_start_ny >= train_end_ny:
                raise RuntimeError(
                    f"Not enough history for train/test split: "
                    f"train_start={train_start_ny}, train_end={train_end_ny}"
                )

            print("DELTA_FEATURE_WINDOW")
            print(f"latest_complete_source_month={latest_yyyymm}")
            print(f"train_start_ny={train_start_ny}")
            print(f"train_end_ny={train_end_ny}")
            print(f"test_start_ny={test_start_ny}")
            print(f"test_end_ny={test_end_ny}")

            cur.execute("TRUNCATE TABLE station_delta_training_features;")

            cur.execute(
                """
                INSERT INTO station_delta_training_features (
                    bucket_start_utc,
                    bucket_start_ny,
                    gbfs_station_id,
                    legacy_station_id,
                    station_name,
                    lat,
                    lon,
                    capacity,
                    hour_ny,
                    day_of_week_ny,
                    is_weekend_ny,
                    month_ny,
                    temperature_2m,
                    precipitation,
                    wind_speed_10m,
                    weather_code,
                    is_rain,
                    departures,
                    returns,
                    delta_bikes,
                    split
                )
                SELECT
                    d.bucket_start_utc,
                    d.bucket_start_ny,
                    d.gbfs_station_id,
                    d.legacy_station_id,
                    m.name AS station_name,
                    m.lat,
                    m.lon,
                    m.capacity,

                    EXTRACT(HOUR FROM d.bucket_start_ny)::INT AS hour_ny,
                    EXTRACT(DOW FROM d.bucket_start_ny)::INT AS day_of_week_ny,
                    CASE
                        WHEN EXTRACT(DOW FROM d.bucket_start_ny)::INT IN (0, 6)
                        THEN 1 ELSE 0
                    END AS is_weekend_ny,
                    EXTRACT(MONTH FROM d.bucket_start_ny)::INT AS month_ny,

                    w.temperature_2m,
                    w.precipitation,
                    w.wind_speed_10m,
                    w.weather_code,
                    w.is_rain,

                    d.departures,
                    d.returns,
                    d.returns - d.departures AS delta_bikes,

                    CASE
                        WHEN d.bucket_start_ny >= %s
                         AND d.bucket_start_ny <  %s
                        THEN 'train'
                        WHEN d.bucket_start_ny >= %s
                         AND d.bucket_start_ny <  %s
                        THEN 'test'
                        ELSE 'unused'
                    END AS split
                FROM station_hourly_demand d
                LEFT JOIN station_id_mapping m
                    ON d.gbfs_station_id = m.gbfs_station_id
                LEFT JOIN weather_hourly_nyc w
                    ON d.bucket_start_ny = w.weather_time_ny
                WHERE d.bucket_start_ny >= %s
                  AND d.bucket_start_ny <  %s
                  AND m.lat IS NOT NULL
                  AND m.lon IS NOT NULL
                  AND m.capacity IS NOT NULL;
                """,
                (
                    train_start_ny,
                    train_end_ny,
                    test_start_ny,
                    test_end_ny,
                    train_start_ny,
                    test_end_ny,
                ),
            )

            conn.commit()

            cur.execute(
                """
                SELECT
                    split,
                    COUNT(*) AS rows_count,
                    MIN(bucket_start_ny) AS min_time,
                    MAX(bucket_start_ny) AS max_time,
                    ROUND(AVG(delta_bikes)::numeric, 4) AS avg_delta,
                    MIN(delta_bikes) AS min_delta,
                    MAX(delta_bikes) AS max_delta,
                    ROUND(
                        100.0 * SUM(CASE WHEN temperature_2m IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
                        2
                    ) AS weather_coverage_pct
                FROM station_delta_training_features
                GROUP BY split
                ORDER BY split;
                """
            )
            rows = cur.fetchall()

    print("DELTA_TRAINING_FEATURES_BUILD_OK")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
