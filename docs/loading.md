# PostgreSQL Load Stage

## Destination and configuration

The loader writes only to a separate PostgreSQL destination; it never writes to the PrepAPig source database. Set `PIPELINE_DATABASE_URL` in the process environment to the destination connection string. Do not place credentials in source files, command arguments, logs, or this document.

The first migration creates the `prepapig_etl` schema and its run-scoped tables. Install Python dependencies with `python -m pip install -r requirements.txt`, then apply the migration once:

```sh
python -m pipeline.load --migrate-only
```

The migration is also available at `migrations/001_pipeline_load.sql`. It is idempotent (`IF NOT EXISTS` and a version ledger); run it against the separate destination with a role authorized to create the schema and tables.

## Load behavior

Load a transformed bundle by its run folder:

```sh
python -m pipeline.load data/transformed/<pipeline_run_id>
```

The loader rejects a bundle unless its transformation metadata and `transformation_log.csv` agree on the extraction and transformation run IDs, report `PASS`/`APPROVED`, report zero quarantined rows, and all seven detail/summary CSV outputs are present and consistent. It rechecks identifiers, parent-batch references, numeric/date fields, run IDs, and summary-to-detail reconciliation before opening a database transaction.

The destination contains `pig_batches`, `feed_records`, `vaccination_records`, and `expenses`, plus `feed_summary`, `expense_summary`, and `farm_expense_summary`. Rows are keyed by `(pipeline_run_id, source id)` (or their summary key), retaining each extraction run independently. Child foreign keys include `pipeline_run_id`, so every loaded child points to a parent batch in that same approved run. The full transformed CSV row is retained in `source_record` JSONB as well as typed columns, preserving additional source columns without silently dropping them. A NULL `expenses.batch_id` is stored as NULL, remains outside batch-linked expense summaries, and is represented in `farm_expense_summary`.

A retry of the same `pipeline_run_id` deletes and replaces only that run's detail and summary rows inside one transaction. A failed transaction rolls back the replacement, so the prior committed version remains intact. Other extraction runs are not changed. Each attempt receives a distinct `load_run_id`; successful attempts are recorded in `prepapig_etl.load_runs`, and a local JSON audit is written under `data/logs/load/audit/` (override with `PIPELINE_LOAD_LOG_DIR`). Failed attempts are written locally even if the destination cannot be reached; when possible, the database load ledger also records failure, counts, and both source run IDs. Connection strings and secret-like text are redacted from error messages.

The loader does not apply migrations implicitly. If the migration has not been applied, it refuses to load and instructs the operator to run the migration command. The orchestrator's `--load` option stops at the first nonzero stage result; it does not bypass transformation approval.
