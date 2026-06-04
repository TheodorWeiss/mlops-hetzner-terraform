CREATE TABLE IF NOT EXISTS model_evaluation_runs (
    id BIGSERIAL PRIMARY KEY,
    run_name TEXT NOT NULL,
    model_type TEXT NOT NULL,
    target_name TEXT NOT NULL,
    train_start_ny TIMESTAMP WITHOUT TIME ZONE,
    train_end_ny TIMESTAMP WITHOUT TIME ZONE,
    test_start_ny TIMESTAMP WITHOUT TIME ZONE,
    test_end_ny TIMESTAMP WITHOUT TIME ZONE,
    rows_test BIGINT,
    mae DOUBLE PRECISION,
    rmse DOUBLE PRECISION,
    mape DOUBLE PRECISION,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_model_evaluation_runs_created_at
    ON model_evaluation_runs (created_at);

CREATE INDEX IF NOT EXISTS idx_model_evaluation_runs_model_target
    ON model_evaluation_runs (model_type, target_name);
