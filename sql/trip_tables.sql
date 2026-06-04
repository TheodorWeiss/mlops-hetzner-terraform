-- Historical Citi Bike trip targets layer.
-- Clean demand/return labels are built from trip CSV.
-- Storage time is UTC, ML/business local time is America/New_York.

CREATE TABLE IF NOT EXISTS station_hourly_demand (
    bucket_start_utc TIMESTAMPTZ NOT NULL,
    bucket_start_ny TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    gbfs_station_id TEXT NOT NULL,
    legacy_station_id TEXT NOT NULL,

    departures INTEGER NOT NULL DEFAULT 0,
    returns INTEGER NOT NULL DEFAULT 0,

    source_file TEXT,
    ingested_at TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT pk_station_hourly_demand
        PRIMARY KEY (bucket_start_utc, gbfs_station_id)
);

CREATE INDEX IF NOT EXISTS idx_station_hourly_demand_station_time
    ON station_hourly_demand (gbfs_station_id, bucket_start_utc);

CREATE INDEX IF NOT EXISTS idx_station_hourly_demand_legacy_station_time
    ON station_hourly_demand (legacy_station_id, bucket_start_utc);

CREATE INDEX IF NOT EXISTS idx_station_hourly_demand_bucket_start_ny
    ON station_hourly_demand (bucket_start_ny);


CREATE TABLE IF NOT EXISTS trip_ingestion_log (
    id BIGSERIAL PRIMARY KEY,

    source_file TEXT NOT NULL,
    rows_total BIGINT,
    rows_valid BIGINT,

    rows_bad_started_at BIGINT,
    rows_bad_ended_at BIGINT,
    rows_missing_start_station_id BIGINT,
    rows_missing_end_station_id BIGINT,
    rows_unmapped_start_station_id BIGINT,
    rows_unmapped_end_station_id BIGINT,

    min_started_at_utc TIMESTAMPTZ,
    max_started_at_utc TIMESTAMPTZ,
    min_ended_at_utc TIMESTAMPTZ,
    max_ended_at_utc TIMESTAMPTZ,

    status TEXT NOT NULL,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trip_ingestion_log_created_at
    ON trip_ingestion_log (created_at);

CREATE INDEX IF NOT EXISTS idx_trip_ingestion_log_source_file
    ON trip_ingestion_log (source_file);
