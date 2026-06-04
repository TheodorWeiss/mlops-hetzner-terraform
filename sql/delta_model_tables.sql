CREATE TABLE IF NOT EXISTS station_delta_training_features (
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

    temperature_2m DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    wind_speed_10m DOUBLE PRECISION,
    weather_code INTEGER,
    is_rain INTEGER,

    departures INTEGER NOT NULL,
    returns INTEGER NOT NULL,
    delta_bikes INTEGER NOT NULL,

    split TEXT NOT NULL,

    created_at TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT pk_station_delta_training_features
        PRIMARY KEY (bucket_start_utc, gbfs_station_id)
);

CREATE INDEX IF NOT EXISTS idx_station_delta_training_features_split
    ON station_delta_training_features (split);

CREATE INDEX IF NOT EXISTS idx_station_delta_training_features_station_time
    ON station_delta_training_features (gbfs_station_id, bucket_start_utc);


CREATE TABLE IF NOT EXISTS delta_model_evaluation_runs (
    id BIGSERIAL PRIMARY KEY,

    run_name TEXT NOT NULL,
    model_type TEXT NOT NULL,
    target_name TEXT NOT NULL DEFAULT 'delta_bikes',

    train_start_ny TIMESTAMP WITHOUT TIME ZONE,
    train_end_ny TIMESTAMP WITHOUT TIME ZONE,
    test_start_ny TIMESTAMP WITHOUT TIME ZONE,
    test_end_ny TIMESTAMP WITHOUT TIME ZONE,

    rows_train BIGINT,
    rows_test BIGINT,

    mae_delta DOUBLE PRECISION,
    rmse_delta DOUBLE PRECISION,
    bias_delta DOUBLE PRECISION,

    mlflow_run_id TEXT,
    registered_model_name TEXT,
    registered_model_version TEXT,
    promotion_decision TEXT,

    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);


CREATE TABLE IF NOT EXISTS prediction_log (
    id BIGSERIAL PRIMARY KEY,

    predicted_at TIMESTAMPTZ NOT NULL,
    target_time TIMESTAMPTZ NOT NULL,

    gbfs_station_id TEXT NOT NULL,
    legacy_station_id TEXT,
    station_name TEXT,

    model_name TEXT,
    model_version TEXT,
    mlflow_run_id TEXT,

    current_bikes INTEGER,
    current_docks INTEGER,
    capacity INTEGER,

    predicted_delta_bikes DOUBLE PRECISION,
    predicted_bikes_raw DOUBLE PRECISION,
    predicted_bikes_clipped DOUBLE PRECISION,

    predicted_docks_raw DOUBLE PRECISION,
    predicted_docks_clipped DOUBLE PRECISION,

    stale_state BOOLEAN DEFAULT false,
    state_age_seconds INTEGER,

    actual_bikes INTEGER,
    actual_docks INTEGER,
    availability_error DOUBLE PRECISION,
    evaluated_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_prediction_log_target_time
    ON prediction_log (target_time);

CREATE INDEX IF NOT EXISTS idx_prediction_log_station_time
    ON prediction_log (gbfs_station_id, target_time);
