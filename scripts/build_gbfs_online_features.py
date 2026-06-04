#!/usr/bin/env python3

import os
import time
from typing import Optional

import psycopg2


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


def run_step(cur, name: str, sql: str) -> None:
    print(f"START {name}", flush=True)
    start = time.time()
    cur.execute(sql)
    print(f"DONE {name} in {time.time() - start:.2f}s", flush=True)


def build_online_features(conn) -> None:
    with conn.cursor() as cur:
        run_step(cur, "drop temp", "DROP TABLE IF EXISTS tmp_gbfs_online_base;")

        run_step(
            cur,
            "create temp base",
            """
            CREATE TEMP TABLE tmp_gbfs_online_base AS
            SELECT
                ROW_NUMBER() OVER (
                    PARTITION BY s.gbfs_station_id
                    ORDER BY s.snapshot_time
                ) AS rn,

                s.snapshot_time,
                (s.snapshot_time AT TIME ZONE 'America/New_York') AS snapshot_time_ny,
                s.gbfs_station_id,

                m.legacy_station_id,
                m.name AS station_name,
                m.lat,
                m.lon,
                m.capacity,

                s.num_bikes_available AS current_bikes,
                s.num_ebikes_available AS current_ebikes,
                s.num_docks_available AS current_docks,
                s.num_bikes_disabled AS bikes_disabled,
                s.num_docks_disabled AS docks_disabled,
                s.data_age_seconds,
                s.source_file
            FROM gbfs_status_snapshots s
            LEFT JOIN station_id_mapping m
                ON s.gbfs_station_id = m.gbfs_station_id;
            """,
        )

        run_step(
            cur,
            "index temp",
            """
            CREATE INDEX idx_tmp_gbfs_online_base_station_rn
                ON tmp_gbfs_online_base (gbfs_station_id, rn);
            """,
        )

        run_step(cur, "truncate target", "TRUNCATE TABLE gbfs_online_features;")

        run_step(
            cur,
            "insert online features",
            """
            INSERT INTO gbfs_online_features (
                snapshot_time,
                snapshot_time_ny,
                gbfs_station_id,
                legacy_station_id,
                station_name,
                lat,
                lon,
                capacity,
                current_bikes,
                current_ebikes,
                current_docks,
                bikes_disabled,
                docks_disabled,
                bikes_24h_ago,
                docks_24h_ago,
                ebikes_24h_ago,
                delta_bikes_24h,
                delta_docks_24h,
                delta_ebikes_24h,
                hour_ny,
                day_of_week_ny,
                is_weekend_ny,
                month_ny,
                data_age_seconds,
                source_file
            )
            SELECT
                b.snapshot_time,
                b.snapshot_time_ny,
                b.gbfs_station_id,
                b.legacy_station_id,
                b.station_name,
                b.lat,
                b.lon,
                b.capacity,

                b.current_bikes,
                b.current_ebikes,
                b.current_docks,
                b.bikes_disabled,
                b.docks_disabled,

                l.current_bikes AS bikes_24h_ago,
                l.current_docks AS docks_24h_ago,
                l.current_ebikes AS ebikes_24h_ago,

                CASE WHEN l.current_bikes IS NULL OR b.current_bikes IS NULL
                    THEN NULL ELSE b.current_bikes - l.current_bikes END,

                CASE WHEN l.current_docks IS NULL OR b.current_docks IS NULL
                    THEN NULL ELSE b.current_docks - l.current_docks END,

                CASE WHEN l.current_ebikes IS NULL OR b.current_ebikes IS NULL
                    THEN NULL ELSE b.current_ebikes - l.current_ebikes END,

                EXTRACT(HOUR FROM b.snapshot_time_ny)::INT,
                EXTRACT(DOW FROM b.snapshot_time_ny)::INT,
                CASE WHEN EXTRACT(DOW FROM b.snapshot_time_ny)::INT IN (0, 6)
                    THEN 1 ELSE 0 END,
                EXTRACT(MONTH FROM b.snapshot_time_ny)::INT,

                b.data_age_seconds,
                b.source_file
            FROM tmp_gbfs_online_base b
            LEFT JOIN tmp_gbfs_online_base l
                ON l.gbfs_station_id = b.gbfs_station_id
               AND l.rn = b.rn - 288;
            """,
        )

    conn.commit()


def main() -> None:
    with get_connection() as conn:
        build_online_features(conn)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS rows_total,
                    COUNT(DISTINCT snapshot_time) AS snapshots,
                    COUNT(DISTINCT gbfs_station_id) AS stations_total,
                    MIN(snapshot_time) AS min_snapshot_time,
                    MAX(snapshot_time) AS max_snapshot_time,
                    SUM(CASE WHEN bikes_24h_ago IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_24h_lag,
                    SUM(CASE WHEN bikes_24h_ago IS NULL THEN 1 ELSE 0 END) AS rows_without_24h_lag
                FROM gbfs_online_features;
                """
            )
            row = cur.fetchone()

    print("GBFS_ONLINE_FEATURES_BUILD_OK")
    print(f"rows_total={row[0]}")
    print(f"snapshots={row[1]}")
    print(f"stations_total={row[2]}")
    print(f"min_snapshot_time={row[3]}")
    print(f"max_snapshot_time={row[4]}")
    print(f"rows_with_24h_lag={row[5]}")
    print(f"rows_without_24h_lag={row[6]}")


if __name__ == "__main__":
    main()
