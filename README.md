# Data-Pipeline

## Group Members

- De Lunas, Maris N.
- Domingo, Yhuan Miguelle D.
- Elola, Isabella Grace M.
- Patal, Mark Angelo G.

## Pipeline Overview

The **PrepAPig Batch Data Pipeline** extracts accumulated pig-farming data from the PrepAPig operational database, validates it, and transforms it into run-scoped CSV outputs. The PostgreSQL destination loader and recurring scheduler are not enabled yet; both remain pending their configuration and approval.

The pipeline covers these farming-related datasets:

- Pig batches
- Feed records
- Vaccination records
- Expenses

An expense may be batch-specific or farm-level. A NULL `expenses.batch_id` is preserved in the transformed detail CSV and summarized separately by `expense_type`; it is not assigned to a pig batch.

User credentials, authentication tokens, and unrelated profile data are outside the ETL scope.

## Pipeline Architecture

The implemented pipeline is run on demand; there is no recurring scheduler registered by this repository. The current Python stages perform:

1. Extract accumulated farming records from the PrepAPig operational database.
2. Validate and clean the extracted data.
3. Calculate, transform, and aggregate the records.
4. Write run-scoped transformed CSV files, quality reports, and audit logs.

**Planned, not enabled:** load transformed data into a separate PostgreSQL database and configure a recurring schedule after the destination schema, load strategy, run time, and timezone are approved. The loader currently stops safely rather than writing to an unconfigured destination.

### Technologies

| Technology | Role / status |
|---|---|
| PrepAPig operational database | Source of accumulated farming data |
| Python | Implemented extraction, validation, transformation, calculations, summaries, and CSV/audit output |
| Scheduler | Not implemented; the schedule command is a one-shot entry point and does not register a recurring job |
| PostgreSQL | Planned destination; the load stage is disabled until its schema and upsert/transaction strategy are approved |

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
            :
            : (planned after destination approval)
            ▼
 Separate PostgreSQL database → reports and analytics
```

The current pipeline is invoked on demand and depends on the PrepAPig operational database, the Python environment, configuration settings, and valid application data. Recurring scheduling and PostgreSQL loading are not active dependencies until those stages are configured.

## Extraction run logging

Each run creates a unique `pipeline_run_id`, records `SUCCESS`, `WARNING`, or `FAILED`, and produces table-level metadata, validation results, and a handover decision (`APPROVED`, `MANUAL REVIEW`, or `REJECTED`). Operational logs and audit records follow the storage and retention rules in [`docs/extraction/data_extraction.md`](docs/extraction/data_extraction.md).

## Current implementation status

- **Implemented:** extraction handover checks, transformation, farm-level expense summaries, quality reports, and run-scoped audit output.
- **Not enabled:** PostgreSQL loading and recurring scheduling. `pipeline/transformation/load.py` fails closed until a destination contract is approved; `schedule.py` validates settings and invokes one run but does not schedule future runs.
- **Manual run:** invoke the orchestrator with the extracted CSV directory and its metadata file: `python -m pipeline.transformation.orchestrate --input-dir <extraction-csv-dir> --metadata <extraction-metadata.json> --output-dir data`.
