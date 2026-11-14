# Data-Pipeline

## Group Members

- De Lunas, Maris N.
- Domingo, Yhuan Miguelle D.
- Elola, Isabella Grace M.
- Patal, Mark Angelo G.

## Pipeline Overview

The **PrepAPig Batch Data Pipeline** extracts accumulated pig-farming data from the PrepAPig operational database, validates and transforms it into run-scoped outputs, and can load approved outputs into a separate PostgreSQL destination. An optional Node.js Cron scheduler runs the stages in order when started with a configured daily time and IANA timezone; no schedule is hard-coded or started automatically.

The pipeline covers these farming-related datasets:

- Pig batches
- Feed records
- Vaccination records
- Expenses

An expense may be batch-specific or farm-level. A NULL `expenses.batch_id` is preserved in the transformed detail CSV and summarized separately by `expense_type`; it is not assigned to a pig batch.

User credentials, authentication tokens, and unrelated profile data are outside the ETL scope.

## Pipeline Architecture

The extraction and transformation stages can be run on demand or invoked by the Node.js Cron scheduler. The scheduler is not an always-on service; start it under a process supervisor after configuring its schedule. The pipeline stages perform:

1. Extract accumulated farming records from the PrepAPig operational database.
2. Validate and clean the extracted data.
3. Calculate, transform, and aggregate the records.
4. Write run-scoped transformed CSV files, quality reports, and audit logs.
5. Load only a PASS/APPROVED run into the configured separate PostgreSQL destination.

**Implemented:** load transformed data into the separate `prepapig_etl` PostgreSQL schema. The destination is configured with `PIPELINE_DATABASE_URL`; apply the migration once with `python -m pipeline.load --migrate-only`. Re-loading a pipeline run atomically replaces only that run's records. Start the optional daily scheduler with `npm run schedule` after setting `PREPAPIG_DAILY_TIME` and `PREPAPIG_TIMEZONE`.

### Technologies

| Technology | Role / status |
|---|---|
| PrepAPig operational database | Source of accumulated farming data |
| Python | Implemented extraction, validation, transformation, calculations, summaries, and CSV/audit output |
| Scheduler | Node.js Cron entry point in `scheduler.js`; requires `PREPAPIG_DAILY_TIME` and `PREPAPIG_TIMEZONE`, and runs only when explicitly started |
| PostgreSQL | Separate destination implemented in the `prepapig_etl` schema; set `PIPELINE_DATABASE_URL` and apply `migrations/001_pipeline_load.sql` before loading |

## Data Lineage

```text
PrepAPig operational database
            │
            ▼
   Python batch extraction
            │
            ▼
 Validation, handover, transformation
            │
            ▼
 Run-scoped CSV outputs and audit logs  [implemented]
             │
             ▼
 Separate PostgreSQL database → reports and analytics  [optional load stage; configured separately]
```

The extraction and transformation stages can be invoked on demand. PostgreSQL loading is optional and requires the separate destination URL and migration. The scheduler is configurable but is not running unless started by a long-lived host process.

## Extraction run logging

Each run creates a unique `pipeline_run_id`, records `SUCCESS`, `WARNING`, or `FAILED`, and produces table-level metadata, validation results, and a handover decision (`APPROVED`, `MANUAL REVIEW`, or `REJECTED`). Operational logs and audit records follow the storage and retention rules in [`docs/extraction/data_extraction.md`](docs/extraction/data_extraction.md).

## Current implementation status

- **Implemented:** extraction handover checks, transformation, farm-level expense summaries, quality reports, run-scoped audit output, and PostgreSQL loading. The load contract and setup steps are in [`docs/loading.md`](docs/loading.md).
- **Scheduler:** configurable Node.js Cron orchestration is in [`scheduler.js`](scheduler.js); setup, stage order, and retries are documented in [`docs/scheduling.md`](docs/scheduling.md).
- **Manual load:** `python -m pipeline.load data/transformed/<pipeline_run_id>` loads only a PASS/APPROVED bundle after the migration is applied and `PIPELINE_DATABASE_URL` is set.
- **Start schedule:** `npm run schedule` after setting `PREPAPIG_DAILY_TIME` and `PREPAPIG_TIMEZONE`; run `npm run validate-schedule` first.
