#!/usr/bin/env python3

import os
from typing import Optional

import psycopg2


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


def build_features(conn) -> None:
    sql = """
        TRUNCATE TABLE station_hourly_features;

        INSERT INTO station_hourly_features (
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
            departures,
            returns,
            lag_1h_departures,
            lag_24h_departures,
            lag_168h_departures,
            rolling_24h_departures,
            lag_1h_returns,
            lag_24h_returns,
            lag_168h_returns,
            rolling_24h_returns,
            source_file
        )
        WITH base AS (
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

                d.departures,
                d.returns,
                d.source_file
            FROM station_hourly_demand d
            LEFT JOIN station_id_mapping m
                ON d.gbfs_station_id = m.gbfs_station_id
            WHERE d.bucket_start_ny >= TIMESTAMP '2026-02-01 00:00:00'
        ),
        lagged AS (
            SELECT
                base.*,

                LAG(departures, 1) OVER (
                    PARTITION BY gbfs_station_id
                    ORDER BY bucket_start_utc
                ) AS lag_1h_departures,

                LAG(departures, 24) OVER (
                    PARTITION BY gbfs_station_id
                    ORDER BY bucket_start_utc
                ) AS lag_24h_departures,

                LAG(departures, 168) OVER (
                    PARTITION BY gbfs_station_id
                    ORDER BY bucket_start_utc
                ) AS lag_168h_departures,

                AVG(departures) OVER (
                    PARTITION BY gbfs_station_id
                    ORDER BY bucket_start_utc
                    ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
                ) AS rolling_24h_departures,

                LAG(returns, 1) OVER (
                    PARTITION BY gbfs_station_id
                    ORDER BY bucket_start_utc
                ) AS lag_1h_returns,

                LAG(returns, 24) OVER (
                    PARTITION BY gbfs_station_id
                    ORDER BY bucket_start_utc
                ) AS lag_24h_returns,

                LAG(returns, 168) OVER (
                    PARTITION BY gbfs_station_id
                    ORDER BY bucket_start_utc
                ) AS lag_168h_returns,

                AVG(returns) OVER (
                    PARTITION BY gbfs_station_id
                    ORDER BY bucket_start_utc
                    ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
                ) AS rolling_24h_returns
            FROM base
        )
        SELECT
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
            departures,
            returns,
            lag_1h_departures,
            lag_24h_departures,
            lag_168h_departures,
            rolling_24h_departures,
            lag_1h_returns,
            lag_24h_returns,
            lag_168h_returns,
            rolling_24h_returns,
            source_file
        FROM lagged;
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()


def main() -> None:
    with get_connection() as conn:
        build_features(conn)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS feature_rows,
                    COUNT(DISTINCT gbfs_station_id) AS stations_total,
                    MIN(bucket_start_ny) AS min_bucket_ny,
                    MAX(bucket_start_ny) AS max_bucket_ny,
                    SUM(CASE WHEN lag_1h_departures IS NULL THEN 1 ELSE 0 END) AS null_lag_1h_dep,
                    SUM(CASE WHEN lag_24h_departures IS NULL THEN 1 ELSE 0 END) AS null_lag_24h_dep,
                    SUM(CASE WHEN lag_168h_departures IS NULL THEN 1 ELSE 0 END) AS null_lag_168h_dep
                FROM station_hourly_features;
                """
            )
            row = cur.fetchone()

    print("STATION_HOURLY_FEATURES_BUILD_OK")
    print(f"feature_rows={row[0]}")
    print(f"stations_total={row[1]}")
    print(f"min_bucket_ny={row[2]}")
    print(f"max_bucket_ny={row[3]}")
    print(f"null_lag_1h_departures={row[4]}")
    print(f"null_lag_24h_departures={row[5]}")
    print(f"null_lag_168h_departures={row[6]}")


if __name__ == "__main__":
    main()
