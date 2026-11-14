-- Separate, versioned destination for PrepAPig transformation outputs.
-- Apply with: python -m pipeline.load --migrate-only
CREATE SCHEMA IF NOT EXISTS prepapig_etl;

CREATE TABLE IF NOT EXISTS prepapig_etl.schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS prepapig_etl.pig_batches (
    pipeline_run_id text NOT NULL,
    transformation_run_id text NOT NULL,
    id uuid NOT NULL,
    batch_code varchar(50) NOT NULL,
    pig_count integer CHECK (pig_count IS NULL OR pig_count >= 0),
    breed varchar(100),
    start_weight numeric(10,2) CHECK (start_weight IS NULL OR start_weight >= 0),
    current_weight numeric(10,2) CHECK (current_weight IS NULL OR current_weight >= 0),
    date_acquired date,
    status varchar(20),
    created_at timestamptz NOT NULL,
    weight_change numeric(10,2),
    source_record jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_run_id, id),
    UNIQUE (pipeline_run_id, batch_code)
);

CREATE TABLE IF NOT EXISTS prepapig_etl.feed_records (
    pipeline_run_id text NOT NULL,
    transformation_run_id text NOT NULL,
    id uuid NOT NULL,
    batch_id uuid NOT NULL,
    feed_type varchar(100) NOT NULL,
    quantity_kg numeric(10,2) NOT NULL CHECK (quantity_kg >= 0),
    feeding_date date NOT NULL,
    feeding_time time,
    notes text,
    created_at timestamptz NOT NULL,
    source_record jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_run_id, id),
    FOREIGN KEY (pipeline_run_id, batch_id)
        REFERENCES prepapig_etl.pig_batches (pipeline_run_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS prepapig_etl.vaccination_records (
    pipeline_run_id text NOT NULL,
    transformation_run_id text NOT NULL,
    id uuid NOT NULL,
    batch_id uuid NOT NULL,
    vaccine_name varchar(100) NOT NULL,
    vaccination_date date NOT NULL,
    next_due_date date,
    administered_by varchar(100),
    dosage varchar(50),
    notes text,
    status varchar(20),
    created_at timestamptz NOT NULL,
    source_record jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_run_id, id),
    FOREIGN KEY (pipeline_run_id, batch_id)
        REFERENCES prepapig_etl.pig_batches (pipeline_run_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS prepapig_etl.expenses (
    pipeline_run_id text NOT NULL,
    transformation_run_id text NOT NULL,
    id uuid NOT NULL,
    batch_id uuid,
    expense_type varchar(100) NOT NULL,
    amount numeric(10,2) NOT NULL CHECK (amount >= 0),
    expense_date date NOT NULL,
    description text,
    created_at timestamptz NOT NULL,
    source_record jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_run_id, id),
    FOREIGN KEY (pipeline_run_id, batch_id)
        REFERENCES prepapig_etl.pig_batches (pipeline_run_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS prepapig_etl.feed_summary (
    pipeline_run_id text NOT NULL,
    transformation_run_id text NOT NULL,
    batch_id uuid NOT NULL,
    total_feed_kg numeric(20,2) NOT NULL CHECK (total_feed_kg >= 0),
    window_start text,
    window_end text,
    source_record jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_run_id, batch_id),
    FOREIGN KEY (pipeline_run_id, batch_id)
        REFERENCES prepapig_etl.pig_batches (pipeline_run_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS prepapig_etl.expense_summary (
    pipeline_run_id text NOT NULL,
    transformation_run_id text NOT NULL,
    batch_id uuid NOT NULL,
    total_expense numeric(20,2) NOT NULL CHECK (total_expense >= 0),
    window_start text,
    window_end text,
    source_record jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_run_id, batch_id),
    FOREIGN KEY (pipeline_run_id, batch_id)
        REFERENCES prepapig_etl.pig_batches (pipeline_run_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS prepapig_etl.farm_expense_summary (
    pipeline_run_id text NOT NULL,
    transformation_run_id text NOT NULL,
    expense_type varchar(100) NOT NULL,
    record_count integer NOT NULL CHECK (record_count > 0),
    total_expense numeric(20,2) NOT NULL CHECK (total_expense >= 0),
    window_start text,
    window_end text,
    source_record jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_run_id, expense_type)
);

CREATE TABLE IF NOT EXISTS prepapig_etl.load_runs (
    load_run_id text PRIMARY KEY,
    pipeline_run_id text NOT NULL,
    transformation_run_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    started_at timestamptz NOT NULL,
    ended_at timestamptz NOT NULL,
    row_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_message text NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_etl_feed_records_batch
    ON prepapig_etl.feed_records (pipeline_run_id, batch_id);
CREATE INDEX IF NOT EXISTS idx_etl_vaccination_records_batch
    ON prepapig_etl.vaccination_records (pipeline_run_id, batch_id);
CREATE INDEX IF NOT EXISTS idx_etl_expenses_batch
    ON prepapig_etl.expenses (pipeline_run_id, batch_id);
CREATE INDEX IF NOT EXISTS idx_etl_load_runs_pipeline
    ON prepapig_etl.load_runs (pipeline_run_id, started_at DESC);

INSERT INTO prepapig_etl.schema_migrations (version)
VALUES ('001_pipeline_load')
ON CONFLICT (version) DO NOTHING;
