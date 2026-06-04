CREATE TABLE IF NOT EXISTS weather_hourly_nyc (
    weather_time_ny TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
    weather_time_utc TIMESTAMPTZ,

    temperature_2m DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    wind_speed_10m DOUBLE PRECISION,
    weather_code INTEGER,
    is_rain INTEGER,

    source TEXT DEFAULT 'open-meteo historical archive',
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_weather_hourly_nyc_time_utc
    ON weather_hourly_nyc (weather_time_utc);
