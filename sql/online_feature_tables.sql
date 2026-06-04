CREATE TABLE IF NOT EXISTS gbfs_online_features (
    snapshot_time TIMESTAMPTZ NOT NULL,
    snapshot_time_ny TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    gbfs_station_id TEXT NOT NULL,
    legacy_station_id TEXT,
    station_name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    capacity INTEGER,

    current_bikes INTEGER,
    current_ebikes INTEGER,
    current_docks INTEGER,
    bikes_disabled INTEGER,
    docks_disabled INTEGER,

    bikes_24h_ago INTEGER,
    docks_24h_ago INTEGER,
    ebikes_24h_ago INTEGER,

    delta_bikes_24h INTEGER,
    delta_docks_24h INTEGER,
    delta_ebikes_24h INTEGER,

    hour_ny INTEGER NOT NULL,
    day_of_week_ny INTEGER NOT NULL,
    is_weekend_ny INTEGER NOT NULL,
    month_ny INTEGER NOT NULL,

    data_age_seconds INTEGER,
    source_file TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT pk_gbfs_online_features
        PRIMARY KEY (snapshot_time, gbfs_station_id)
);

CREATE INDEX IF NOT EXISTS idx_gbfs_online_features_station_time
    ON gbfs_online_features (gbfs_station_id, snapshot_time);

CREATE INDEX IF NOT EXISTS idx_gbfs_online_features_snapshot_time
    ON gbfs_online_features (snapshot_time);

CREATE INDEX IF NOT EXISTS idx_gbfs_online_features_snapshot_time_ny
    ON gbfs_online_features (snapshot_time_ny);
