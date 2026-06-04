-- Feature layer for BikeML baseline/model training.
-- Time-derived ML features are based on New York local time.

CREATE TABLE IF NOT EXISTS station_hourly_features (
    bucket_start_utc TIMESTAMPTZ NOT NULL,
    bucket_start_ny TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    gbfs_station_id TEXT NOT NULL,
    legacy_station_id TEXT NOT NULL,

    station_name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    capacity INTEGER,

    hour_ny INTEGER NOT NULL,
    day_of_week_ny INTEGER NOT NULL,
    is_weekend_ny INTEGER NOT NULL,
    month_ny INTEGER NOT NULL,

    departures INTEGER NOT NULL,
    returns INTEGER NOT NULL,

    lag_1h_departures INTEGER,
    lag_24h_departures INTEGER,
    lag_168h_departures INTEGER,
    rolling_24h_departures DOUBLE PRECISION,

    lag_1h_returns INTEGER,
    lag_24h_returns INTEGER,
    lag_168h_returns INTEGER,
    rolling_24h_returns DOUBLE PRECISION,

    source_file TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT pk_station_hourly_features
        PRIMARY KEY (bucket_start_utc, gbfs_station_id)
);

CREATE INDEX IF NOT EXISTS idx_station_hourly_features_station_time
    ON station_hourly_features (gbfs_station_id, bucket_start_utc);

CREATE INDEX IF NOT EXISTS idx_station_hourly_features_bucket_start_ny
    ON station_hourly_features (bucket_start_ny);

CREATE INDEX IF NOT EXISTS idx_station_hourly_features_legacy_station_time
    ON station_hourly_features (legacy_station_id, bucket_start_utc);
