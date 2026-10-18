# Extraction Log Retention

## Purpose

Each extraction run produces traceable metadata and operational logs so that a run can be identified, audited, and diagnosed without relying on console output alone.

## Run identity and status

Every run receives a unique `pipeline_run_id` in the format:

```text
PREPAPIG_YYYYMMDD_HHMMSS_<unique-suffix>
```

Each run records exactly one final `extraction_status`:

- `SUCCESS` — extraction and validation completed without warnings.
- `WARNING` — extraction and validation completed, but non-blocking data-quality warnings were recorded.
- `FAILED` — extraction could not complete safely, or validation found a blocking failure.

The status is also written to the operational event log and the run's audit record.

## Stored artifacts

For an output directory named `data`, the extractor writes:

```text
data/
├── metadata/<pipeline_run_id>.json
└── logs/extraction/
    ├── operational/extraction_YYYY-MM-DD.jsonl
    └── audit/<pipeline_run_id>.json
```

- `metadata/<pipeline_run_id>.json` is the run metadata used by downstream processing.
- `logs/extraction/operational/*.jsonl` is append-only and stores one `extraction_finished` event per run.
- `logs/extraction/audit/<pipeline_run_id>.json` stores the complete metadata snapshot for that run.

## Retention policy

- Keep operational JSONL logs for **90 days**.
- Keep audit JSON records for **1 year**.
- Keep raw CSV output according to the downstream reporting and storage policy; raw data is not deleted automatically by the extractor.
- Retention cleanup must be performed by the deployment or storage administrator after confirming that the files are no longer needed for audit, incident review, or reporting.
- Never commit generated raw data, metadata, or logs to the repository. The repository `.gitignore` excludes these generated artifacts.

A scheduled cleanup may delete only files older than the applicable retention period and must preserve the current run's files. Cleanup failures must be reported separately from extraction failures.
