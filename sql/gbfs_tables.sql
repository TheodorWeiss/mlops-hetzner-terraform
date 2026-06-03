-- Structured GBFS layer for BikeML
-- Creates idempotent tables in the bikeml database.

CREATE TABLE IF NOT EXISTS station_id_mapping (
    gbfs_station_id TEXT PRIMARY KEY,
    legacy_station_id TEXT,
    name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    capacity INTEGER,
    region_id TEXT,
    rental_methods JSONB,
    source_file TEXT,
    station_information_updated_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_station_id_mapping_legacy_station_id
    ON station_id_mapping (legacy_station_id);

CREATE INDEX IF NOT EXISTS idx_station_id_mapping_gbfs_station_id
    ON station_id_mapping (gbfs_station_id);


CREATE TABLE IF NOT EXISTS gbfs_status_snapshots (
    snapshot_time TIMESTAMPTZ NOT NULL,
    gbfs_station_id TEXT NOT NULL,
    num_bikes_available INTEGER,
    num_ebikes_available INTEGER,
    num_docks_available INTEGER,
    num_bikes_disabled INTEGER,
    num_docks_disabled INTEGER,
    is_installed INTEGER,
    is_renting INTEGER,
    is_returning INTEGER,
    last_reported BIGINT,
    data_age_seconds INTEGER,
    source_file TEXT,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_gbfs_status_snapshots_snapshot_station
        UNIQUE (snapshot_time, gbfs_station_id)
);

CREATE INDEX IF NOT EXISTS idx_gbfs_status_snapshots_snapshot_time
    ON gbfs_status_snapshots (snapshot_time);

CREATE INDEX IF NOT EXISTS idx_gbfs_status_snapshots_gbfs_station_id
    ON gbfs_status_snapshots (gbfs_station_id);

CREATE INDEX IF NOT EXISTS idx_gbfs_status_snapshots_station_time
    ON gbfs_status_snapshots (gbfs_station_id, snapshot_time);


CREATE TABLE IF NOT EXISTS gbfs_ingestion_log (
    id BIGSERIAL PRIMARY KEY,
    dag_run_id TEXT,
    task_id TEXT,
    source_file TEXT,
    file_type TEXT,
    rows_total INTEGER,
    rows_valid INTEGER,
    rows_filtered_inactive INTEGER,
    rows_filtered_bad_last_reported INTEGER,
    rows_filtered_stale INTEGER,
    rows_filtered_ebikes_gt_bikes INTEGER,
    status TEXT,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_gbfs_ingestion_log_created_at
    ON gbfs_ingestion_log (created_at);

CREATE INDEX IF NOT EXISTS idx_gbfs_ingestion_log_file_type
    ON gbfs_ingestion_log (file_type);


CREATE TABLE IF NOT EXISTS mapping_quality_report (
    id BIGSERIAL PRIMARY KEY,
    report_time TIMESTAMPTZ DEFAULT now(),
    station_information_file TEXT,
    stations_total INTEGER,
    stations_with_short_name INTEGER,
    unique_short_names INTEGER,
    duplicate_short_names INTEGER,
    coverage_ratio DOUBLE PRECISION,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_mapping_quality_report_report_time
    ON mapping_quality_report (report_time);
