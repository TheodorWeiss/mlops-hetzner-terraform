#!/usr/bin/env python3

from __future__ import annotations

import os
from typing import Optional

import psycopg2


TOLERANCE_MINUTES = int(os.getenv("ONLINE_EVAL_TOLERANCE_MINUTES", "15"))


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


def main() -> None:
    sql = """
        WITH matched AS (
            SELECT DISTINCT ON (p.id)
                p.id AS prediction_id,
                f.current_bikes AS actual_bikes,
                f.current_docks AS actual_docks,
                f.snapshot_time AS actual_snapshot_time,
                p.predicted_bikes_clipped,
                (p.predicted_bikes_clipped - f.current_bikes) AS availability_error
            FROM prediction_log p
            JOIN gbfs_online_features f
              ON f.gbfs_station_id = p.gbfs_station_id
             AND f.snapshot_time >= p.target_time
             AND f.snapshot_time <= p.target_time + (%s || ' minutes')::interval
            WHERE p.evaluated_at IS NULL
              AND p.target_time <= now()
            ORDER BY p.id, f.snapshot_time ASC
        )
        UPDATE prediction_log p
        SET
            actual_bikes = matched.actual_bikes,
            actual_docks = matched.actual_docks,
            availability_error = matched.availability_error,
            evaluated_at = now()
        FROM matched
        WHERE p.id = matched.prediction_id
        RETURNING
            p.id,
            p.gbfs_station_id,
            p.legacy_station_id,
            p.station_name,
            p.target_time,
            matched.actual_snapshot_time,
            p.predicted_bikes_clipped,
            p.actual_bikes,
            p.availability_error;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (TOLERANCE_MINUTES,))
            rows = cur.fetchall()

        conn.commit()

    print("ONLINE_PREDICTION_EVALUATION_DONE")
    print(f"tolerance_minutes={TOLERANCE_MINUTES}")
    print(f"evaluated_predictions={len(rows)}")

    if rows:
        print("sample_rows:")
        for row in rows[:10]:
            print(row)
    else:
        print("Нет созревших прогнозов с подходящим фактическим GBFS snapshot. Это нормально, если target_time ещё в будущем.")


if __name__ == "__main__":
    main()
