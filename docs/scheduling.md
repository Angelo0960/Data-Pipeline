# Recurring Pipeline Scheduler

## Configuration

The Node.js scheduler has no hard-coded run time and does not start as a background service by itself. Configure these environment variables in the host that will keep the scheduler process running:

- `PREPAPIG_DAILY_TIME`: required 24-hour `HH:MM` local clock time.
- `PREPAPIG_TIMEZONE`: required IANA timezone (for example `Asia/Manila`).
- `PIPELINE_DATA_DIR`: optional pipeline data root; defaults to `pipeline/data` inside the repository. Extraction log lines always append to the fixed repository file `pipeline/data/extraction/extraction.log`; JSON run metadata is stored under `<data-dir>/extraction/audit/` for scheduler handoff and retry.
- `PIPELINE_PYTHON`: optional Python executable; defaults to `python`.
- `PIPELINE_DATABASE_URL`: required by the final Load stage; use only a separate PostgreSQL destination and never commit or print the connection string.
- `PREPAPIG_SCHEDULER_LOG`: optional JSONL audit path; defaults to `<data-dir>/logs/scheduler/runs.jsonl`.

Apply the PostgreSQL migration and set its destination connection before enabling the full scheduled chain; see [`loading.md`](loading.md). The source Supabase configuration remains in the Extraction Stage's environment.

## Start and validate

Install the Node dependency and validate the configuration first:

```sh
npm install
npm run validate-schedule
npm run schedule
```

`npm run schedule` is a long-running process. Use the deployment host's process supervisor to keep exactly one scheduler instance running. The scheduler uses the configured timezone and skips overlapping runs within that process. Cross-host coordination is not enabled in this configuration, so run one scheduler instance unless deployment coordination is explicitly configured. No daily time or timezone is assumed by the repository.

## Stage order and failure behavior

Each scheduled invocation performs these steps in order:

1. Run Python Extraction into the configured data directory.
2. Read the newly written extraction audit and require `SUCCESS`, `PASS`, and `APPROVED` before continuing.
3. Run the handover and Transformation Stage using that extraction run's CSV path and metadata.
4. Verify the resulting transformation metadata is `PASS`/`APPROVED` with zero quarantined rows.
5. Load that exact run-scoped bundle into PostgreSQL.

The scheduler stops at the first failed or unapproved stage. Each attempt appends a JSONL record with scheduler run ID, stage statuses, failed stage, extraction (`pipeline_run_id`), transformation and load run IDs when available, timestamps, and a sanitized error. The Extraction Stage retains its own watermark and audit records.

## Safe retry

For a downstream Transformation or Load failure, retry the *same approved extraction run* without running Extraction again:

```sh
npm run retry -- PREPAPIG_<run-id>
```

This command verifies the saved extraction audit is still `SUCCESS`/`PASS`/`APPROVED`, then re-runs Transformation and Load for its existing CSVs. The PostgreSQL loader replaces only that extraction run's destination rows transactionally, so retrying does not append duplicates or alter the successful Extraction watermark. A failed extraction is not eligible for this retry command; a later scheduled attempt can retry extraction from the last successful watermark.
