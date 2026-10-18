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

The status is also written to the operational log and the run's audit record.

## Stored artifacts

For an output directory named `data`, the extractor writes:

```text
data/
├── logs/extraction/
│   ├── operational/extraction.log
│   └── audit/<pipeline_run_id>.json
├── metadata/watermark.json
└── raw/<pipeline_run_id>/<table>.csv
```

- `logs/extraction/operational/extraction.log` is the append-only operational log for the whole extraction process. It accumulates every run as markdown tables.
- `logs/extraction/audit/<pipeline_run_id>.json` stores the complete run metadata for audit and recovery.
- `metadata/watermark.json` stores `last_successful_extraction_timestamp` for incremental extraction. It is pipeline state, not a log, and is advanced only after a successful run.
- `raw/<pipeline_run_id>/` holds the extracted CSV data.

### Log content

The file starts with the `# PrepAPig Extraction Log` heading. Every run appends a `## RUN <pipeline_run_id>` section containing three markdown tables, so the file renders as tables in a markdown viewer and can be pasted directly into the documentation.

| Table | Rows |
|---|---|
| **Run Information** | `pipeline_run_id`, mode, `extraction_start`, `extraction_end`, `extraction_duration_seconds`, `extraction_status`, `validation_result`, `window_start`, `window_end`, `raw_output`, `error_message` |
| **Table Extraction** | One row per source table reached by the run: `table_name`, `extraction_status`, `records_read`, `records_extracted`, `records_rejected`, `extraction_start`, `extraction_end`, `duration_seconds`, `validation_result` |
| **Validation Findings** | One row per validation failure or warning with its level, or a single `PASS` row when nothing was raised |

Sections are separated by a horizontal rule. The row for a source table is written before extraction starts, so a run that dies on a table still records which table failed: its `extraction_status` is `FAILED`, its `validation_result` is `FAIL`, and its record counts stay empty. A failed run appends the same three tables with `extraction_status=FAILED` and the captured `error_message`, so no run is missing from the log.

The terminal keeps streaming timestamped progress lines while the run executes. Run with `--quiet` to suppress the terminal output; the log tables are still written.

## Retention policy

- Keep the extraction log for **1 year**.
- Keep raw CSV output according to the downstream reporting and storage policy; raw data is not deleted automatically by the extractor.
- Retention cleanup must be performed by the deployment or storage administrator after confirming that the files are no longer needed for audit, incident review, or reporting.
- Never commit generated raw data, watermark state, or logs to the repository. The repository `.gitignore` excludes these generated artifacts.

A scheduled cleanup may delete only files older than the applicable retention period and must preserve the current run's lines. Cleanup failures must be reported separately from extraction failures.
