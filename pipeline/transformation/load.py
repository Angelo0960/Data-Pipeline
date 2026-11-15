"""Load an approved, run-scoped transformation bundle into PostgreSQL."""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import uuid
from collections import defaultdict
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = PROJECT_ROOT / "migrations" / "001_pipeline_load.sql"
REQUIRED_OUTPUTS = (
    "pig_batches_transformed.csv", "feed_records_transformed.csv",
    "vaccination_records_transformed.csv", "expenses_transformed.csv",
    "feed_summary.csv", "expense_summary.csv", "farm_expense_summary.csv",
)
DETAIL_COLUMNS: dict[str, tuple[str, ...]] = {
    "pig_batches": ("id", "batch_code", "pig_count", "breed", "start_weight", "current_weight", "date_acquired", "status", "created_at", "weight_change"),
    "feed_records": ("id", "batch_id", "feed_type", "quantity_kg", "feeding_date", "feeding_time", "notes", "created_at"),
    "vaccination_records": ("id", "batch_id", "vaccine_name", "vaccination_date", "next_due_date", "administered_by", "dosage", "notes", "status", "created_at"),
    "expenses": ("id", "batch_id", "expense_type", "amount", "expense_date", "description", "created_at"),
}
SUMMARY_FILES = {
    "feed_summary": "feed_summary.csv",
    "expense_summary": "expense_summary.csv",
    "farm_expense_summary": "farm_expense_summary.csv",
}


class LoadError(RuntimeError):
    """Raised when a bundle, target, or database operation is unsafe."""


def safe_error(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"(?i)(password|api[_-]?key|token|secret)\s*[:=]\s*[^\s,;]+", r"\1=[REDACTED]", text)
    text = re.sub(r"(://[^:/\s]+:)[^@/\s]+@", r"\1[REDACTED]@", text)
    return text[:2000]


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LoadError(f"Cannot read {label}: {safe_error(exc)}") from exc
    if not isinstance(result, dict):
        raise LoadError(f"{label} must contain a JSON object")
    return result


def _read_csv(path: Path, required: tuple[str, ...] = ()) -> tuple[list[dict[str, str]], list[str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            headers = list(reader.fieldnames or [])
            if not headers:
                raise LoadError(f"{path.name}: CSV has no header")
            missing = sorted(set(required) - set(headers))
            if missing:
                raise LoadError(f"{path.name}: missing required columns: {', '.join(missing)}")
            rows = []
            for index, row in enumerate(reader, start=2):
                if None in row:
                    raise LoadError(f"{path.name} row {index}: more values than header columns")
                rows.append({str(key): value or "" for key, value in row.items() if key is not None})
            return rows, headers
    except (OSError, csv.Error) as exc:
        raise LoadError(f"Cannot read {path.name}: {safe_error(exc)}") from exc


def inspect_approved_bundle(bundle_dir: Path) -> dict[str, Any]:
    bundle_dir = bundle_dir.resolve()
    metadata = _read_json(bundle_dir / "transformation_metadata.json", "transformation metadata")
    run_id = str(metadata.get("pipeline_run_id") or "").strip()
    transform_id = str(metadata.get("transformation_run_id") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id) or ".." in run_id:
        raise LoadError("Transformation metadata has an invalid pipeline_run_id")
    if not transform_id or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", transform_id) or ".." in transform_id:
        raise LoadError("Transformation metadata has an invalid transformation_run_id")
    if bundle_dir.name != run_id:
        raise LoadError("Bundle folder name does not match transformation metadata pipeline_run_id")
    if metadata.get("handover_decision") != "APPROVED" or metadata.get("transformation_status") != "PASS":
        raise LoadError("Only PASS/APPROVED transformation bundles may be loaded")
    if int(metadata.get("quarantined_rows", -1)) != 0:
        raise LoadError("Transformation bundle contains quarantined rows and requires manual review")
    missing = [name for name in REQUIRED_OUTPUTS if not (bundle_dir / name).is_file()]
    if missing:
        raise LoadError("Bundle is incomplete: " + ", ".join(missing))

    log_rows, _ = _read_csv(bundle_dir / "transformation_log.csv", (
        "pipeline_run_id", "transformation_run_id", "transformation_status", "handover_decision",
    ))
    if len(log_rows) != 1:
        raise LoadError("transformation_log.csv must contain exactly one run record")
    log = log_rows[0]
    if (log["pipeline_run_id"] != run_id or log["transformation_run_id"] != transform_id
            or log["transformation_status"] != "PASS" or log["handover_decision"] != "APPROVED"):
        raise LoadError("Transformation log does not agree with the approved metadata")

    tables: dict[str, list[dict[str, str]]] = {}
    for table, columns in DETAIL_COLUMNS.items():
        rows, _ = _read_csv(bundle_dir / f"{table}_transformed.csv", columns)
        expected = metadata.get("counts", {}).get(table, {}).get("accepted_records")
        if expected is not None and len(rows) != int(expected):
            raise LoadError(f"{table}: accepted row count does not match transformation metadata")
        ids = [row["id"].strip() for row in rows]
        if any(not item for item in ids) or len(ids) != len(set(ids)):
            raise LoadError(f"{table}: missing or duplicate primary key")
        for row in rows:
            _parse_uuid(row["id"], f"{table}.id")
        tables[table] = rows
    batches = tables["pig_batches"]
    batch_ids = {row["id"].strip() for row in batches}
    batch_codes = [row["batch_code"].strip() for row in batches]
    if any(not code for code in batch_codes) or len(batch_codes) != len(set(batch_codes)):
        raise LoadError("pig_batches: batch_code must be nonblank and unique after trimming")
    for table in ("feed_records", "vaccination_records", "expenses"):
        for row in tables[table]:
            batch_id = row.get("batch_id", "").strip()
            if batch_id:
                _parse_uuid(batch_id, f"{table}.batch_id")
                if batch_id not in batch_ids:
                    raise LoadError(f"{table}: batch_id does not reference a batch in this run")
            elif table != "expenses":
                raise LoadError(f"{table}: batch_id is required")

    summaries: dict[str, list[dict[str, str]]] = {}
    required_summary_cols = {
        "feed_summary": ("batch_id", "total_feed_kg", "pipeline_run_id", "window_start", "window_end"),
        "expense_summary": ("batch_id", "total_expense", "pipeline_run_id", "window_start", "window_end"),
        "farm_expense_summary": ("expense_type", "record_count", "total_expense", "pipeline_run_id", "window_start", "window_end"),
    }
    for name, filename in SUMMARY_FILES.items():
        rows, _ = _read_csv(bundle_dir / filename, required_summary_cols[name])
        if any(row["pipeline_run_id"] != run_id for row in rows):
            raise LoadError(f"{filename}: pipeline_run_id does not match this bundle")
        summaries[name] = rows
    _validate_summary_totals(tables, summaries)
    return {
        "metadata": metadata, "pipeline_run_id": run_id,
        "transformation_run_id": transform_id, "tables": tables,
        "summaries": summaries, "bundle_dir": str(bundle_dir),
    }


def _parse_uuid(value: str, label: str) -> uuid.UUID:
    try:
        return uuid.UUID(value.strip())
    except (ValueError, AttributeError) as exc:
        raise LoadError(f"{label} is not a valid UUID") from exc


def _optional_text(value: str) -> str | None:
    return value if value.strip() else None


def _decimal(value: str, label: str, *, required: bool = False) -> Decimal | None:
    text = (value or "").strip()
    if not text:
        if required:
            raise LoadError(f"{label} is required")
        return None
    try:
        number = Decimal(text)
        quantized = number.quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError) as exc:
        raise LoadError(f"{label} is not a valid two-decimal number") from exc
    if not number.is_finite() or number != quantized:
        raise LoadError(f"{label} must be finite and have no more than two decimal places")
    return quantized


def _date(value: str, label: str) -> date | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise LoadError(f"{label} is not an ISO date") from exc


def _time(value: str, label: str) -> time | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return time.fromisoformat(text)
    except ValueError as exc:
        raise LoadError(f"{label} is not an ISO time") from exc


def _timestamp(value: str, label: str) -> datetime:
    text = (value or "").strip()
    if not text:
        raise LoadError(f"{label} is required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LoadError(f"{label} is not an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LoadError(f"{label} must include a timezone offset for PostgreSQL TIMESTAMPTZ")
    return parsed


def _validate_summary_totals(
    tables: dict[str, list[dict[str, str]]], summaries: dict[str, list[dict[str, str]]],
) -> None:
    expected_feed: defaultdict[str, Decimal] = defaultdict(Decimal)
    expected_expense: defaultdict[str, Decimal] = defaultdict(Decimal)
    expected_farm: defaultdict[str, tuple[int, Decimal]] = defaultdict(lambda: (0, Decimal(0)))
    for row in tables["feed_records"]:
        expected_feed[row["batch_id"].strip()] += _decimal(row["quantity_kg"], "feed_records.quantity_kg", required=True) or Decimal(0)
    for row in tables["expenses"]:
        amount = _decimal(row["amount"], "expenses.amount", required=True) or Decimal(0)
        batch_id = row.get("batch_id", "").strip()
        if batch_id:
            expected_expense[batch_id] += amount
        else:
            count, total = expected_farm[row["expense_type"]]
            expected_farm[row["expense_type"]] = (count + 1, total + amount)
    observed_feed = {r["batch_id"].strip(): _decimal(r["total_feed_kg"], "feed_summary.total_feed_kg", required=True) for r in summaries["feed_summary"]}
    observed_expense = {r["batch_id"].strip(): _decimal(r["total_expense"], "expense_summary.total_expense", required=True) for r in summaries["expense_summary"]}
    observed_farm = {r["expense_type"]: (int(r["record_count"]), _decimal(r["total_expense"], "farm_expense_summary.total_expense", required=True)) for r in summaries["farm_expense_summary"]}
    if expected_feed != observed_feed:
        raise LoadError("feed_summary totals do not reconcile with detail rows")
    if expected_expense != observed_expense:
        raise LoadError("expense_summary totals do not reconcile with detail rows")
    if dict(expected_farm) != observed_farm:
        raise LoadError("farm_expense_summary does not reconcile with NULL-batch expense rows")


def _detail_values(table: str, row: dict[str, str], context: dict[str, Any]) -> tuple[Any, ...]:
    common = (context["pipeline_run_id"], context["transformation_run_id"], _parse_uuid(row["id"], f"{table}.id"))
    if table == "pig_batches":
        values = (
            row["batch_code"].strip(), int(row["pig_count"]) if row.get("pig_count", "").strip() else None,
            _optional_text(row.get("breed", "")), _decimal(row.get("start_weight", ""), "start_weight"),
            _decimal(row.get("current_weight", ""), "current_weight"), _date(row.get("date_acquired", ""), "date_acquired"),
            _optional_text(row.get("status", "")), _timestamp(row["created_at"], "created_at"),
            _decimal(row.get("weight_change", ""), "weight_change"), Jsonb(row),
        )
    elif table == "feed_records":
        values = (
            _parse_uuid(row["batch_id"], "feed_records.batch_id"), row["feed_type"].strip(),
            _decimal(row["quantity_kg"], "quantity_kg", required=True), _date(row["feeding_date"], "feeding_date"),
            _time(row.get("feeding_time", ""), "feeding_time"), _optional_text(row.get("notes", "")),
            _timestamp(row["created_at"], "created_at"), Jsonb(row),
        )
    elif table == "vaccination_records":
        values = (
            _parse_uuid(row["batch_id"], "vaccination_records.batch_id"), row["vaccine_name"].strip(),
            _date(row.get("vaccination_date", ""), "vaccination_date"), _date(row.get("next_due_date", ""), "next_due_date"),
            _optional_text(row.get("administered_by", "")), _optional_text(row.get("dosage", "")),
            _optional_text(row.get("notes", "")), _optional_text(row.get("status", "")),
            _timestamp(row["created_at"], "created_at"), Jsonb(row),
        )
    else:
        batch_id = row.get("batch_id", "").strip()
        values = (
            _parse_uuid(batch_id, "expenses.batch_id") if batch_id else None,
            row["expense_type"].strip(), _decimal(row["amount"], "amount", required=True),
            _date(row["expense_date"], "expense_date"), _optional_text(row.get("description", "")),
            _timestamp(row["created_at"], "created_at"), Jsonb(row),
        )
    return common + values


def _insert_rows(cursor: psycopg.Cursor[Any], table: str, rows: list[dict[str, str]], context: dict[str, Any]) -> None:
    columns_by_table = {
        "pig_batches": "pipeline_run_id, transformation_run_id, id, batch_code, pig_count, breed, start_weight, current_weight, date_acquired, status, created_at, weight_change, source_record",
        "feed_records": "pipeline_run_id, transformation_run_id, id, batch_id, feed_type, quantity_kg, feeding_date, feeding_time, notes, created_at, source_record",
        "vaccination_records": "pipeline_run_id, transformation_run_id, id, batch_id, vaccine_name, vaccination_date, next_due_date, administered_by, dosage, notes, status, created_at, source_record",
        "expenses": "pipeline_run_id, transformation_run_id, id, batch_id, expense_type, amount, expense_date, description, created_at, source_record",
    }
    if rows:
        columns = columns_by_table[table].split(", ")
        placeholders = ", ".join(["%s"] * len(columns))
        cursor.executemany(
            f"INSERT INTO prepapig_etl.{table} ({', '.join(columns)}) VALUES ({placeholders})",
            [_detail_values(table, row, context) for row in rows],
        )


def _insert_summaries(cursor: psycopg.Cursor[Any], name: str, rows: list[dict[str, str]], context: dict[str, Any]) -> None:
    if not rows:
        return
    loaded = []
    for row in rows:
        if name == "feed_summary":
            loaded.append((context["pipeline_run_id"], context["transformation_run_id"], _parse_uuid(row["batch_id"], "feed_summary.batch_id"), _decimal(row["total_feed_kg"], "total_feed_kg", required=True), row["window_start"] or None, row["window_end"] or None, Jsonb(row)))
        elif name == "expense_summary":
            loaded.append((context["pipeline_run_id"], context["transformation_run_id"], _parse_uuid(row["batch_id"], "expense_summary.batch_id"), _decimal(row["total_expense"], "total_expense", required=True), row["window_start"] or None, row["window_end"] or None, Jsonb(row)))
        else:
            loaded.append((context["pipeline_run_id"], context["transformation_run_id"], row["expense_type"], int(row["record_count"]), _decimal(row["total_expense"], "total_expense", required=True), row["window_start"] or None, row["window_end"] or None, Jsonb(row)))
    if name == "feed_summary":
        query = "INSERT INTO prepapig_etl.feed_summary (pipeline_run_id, transformation_run_id, batch_id, total_feed_kg, window_start, window_end, source_record) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    elif name == "expense_summary":
        query = "INSERT INTO prepapig_etl.expense_summary (pipeline_run_id, transformation_run_id, batch_id, total_expense, window_start, window_end, source_record) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    else:
        query = "INSERT INTO prepapig_etl.farm_expense_summary (pipeline_run_id, transformation_run_id, expense_type, record_count, total_expense, window_start, window_end, source_record) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.executemany(query, loaded)


def _counts(context: dict[str, Any]) -> dict[str, int]:
    return {name: len(rows) for name, rows in context["tables"].items()} | {name: len(rows) for name, rows in context["summaries"].items()}


def _write_local_audit(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe = dict(record)
    safe["error_message"] = safe_error(safe.get("error_message", ""))
    temp = path.with_suffix(".json.tmp")
    temp.write_text(json.dumps(safe, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    os.replace(temp, path)


def _delete_run_rows(cursor: psycopg.Cursor[Any], pipeline_run_id: str) -> None:
    for table in ("farm_expense_summary", "expense_summary", "feed_summary", "expenses", "vaccination_records", "feed_records", "pig_batches"):
        cursor.execute(f"DELETE FROM prepapig_etl.{table} WHERE pipeline_run_id = %s", (pipeline_run_id,))


def _record_database_failure(dsn: str, record: dict[str, Any]) -> None:
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO prepapig_etl.load_runs (load_run_id, pipeline_run_id, transformation_run_id, status, started_at, ended_at, row_counts, error_message) "
                    "VALUES (%s,%s,%s,'FAILED',%s,%s,%s,%s)",
                    (record["load_run_id"], record.get("pipeline_run_id") or "unknown", record.get("transformation_run_id") or "unknown",
                     record["started_at"], record["ended_at"], Jsonb(record.get("row_counts", {})), record["error_message"]),
                )
    except Exception:
        # The local audit is authoritative when the destination itself is unavailable.
        return


def migrate_database(database_url: str | None = None) -> None:
    dsn = database_url or os.getenv("PIPELINE_DATABASE_URL", "").strip()
    if not dsn:
        raise LoadError("Set PIPELINE_DATABASE_URL to the separate PostgreSQL destination")
    try:
        migration = MIGRATION_PATH.read_text(encoding="utf-8")
        with psycopg.connect(dsn) as conn, conn.cursor() as cursor:
            cursor.execute(migration, prepare=False)
    except (OSError, psycopg.Error) as exc:
        raise LoadError(f"Could not apply PostgreSQL migration: {safe_error(exc)}") from exc


def load_approved_bundle(bundle_dir: Path, *, database_url: str | None = None) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    load_id = f"LOAD_{started_at.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    audit_dir = Path(os.getenv("PIPELINE_LOAD_LOG_DIR", "data/logs/load/audit"))
    context: dict[str, Any] = {"pipeline_run_id": "unknown", "transformation_run_id": "unknown", "tables": {}, "summaries": {}}
    record: dict[str, Any] = {
        "load_run_id": load_id, "pipeline_run_id": "unknown", "transformation_run_id": "unknown",
        "load_status": "FAILED", "started_at": started_at.isoformat(), "ended_at": "",
        "row_counts": {}, "bundle_dir": str(bundle_dir), "error_message": "",
    }
    dsn = database_url or os.getenv("PIPELINE_DATABASE_URL", "").strip()
    conn: psycopg.Connection[Any] | None = None
    try:
        context = inspect_approved_bundle(bundle_dir)
        record.update({"pipeline_run_id": context["pipeline_run_id"], "transformation_run_id": context["transformation_run_id"], "row_counts": _counts(context)})
        if not dsn:
            raise LoadError("Set PIPELINE_DATABASE_URL to the separate PostgreSQL destination")
        conn = psycopg.connect(dsn)
        with conn.transaction():
            with conn.cursor() as cursor:
                cursor.execute("SELECT to_regclass(%s)", ("prepapig_etl.load_runs",))
                if cursor.fetchone()[0] is None:
                    raise LoadError("Destination schema is not initialized; run `python -m pipeline.load --migrate-only` first")
                _delete_run_rows(cursor, context["pipeline_run_id"])
                _insert_rows(cursor, "pig_batches", context["tables"]["pig_batches"], context)
                for table in ("feed_records", "vaccination_records", "expenses"):
                    _insert_rows(cursor, table, context["tables"][table], context)
                for name in SUMMARY_FILES:
                    _insert_summaries(cursor, name, context["summaries"][name], context)
                cursor.execute(
                    "INSERT INTO prepapig_etl.load_runs (load_run_id, pipeline_run_id, transformation_run_id, status, started_at, ended_at, row_counts, error_message) "
                    "VALUES (%s,%s,%s,'SUCCESS',%s,now(),%s,'')",
                    (load_id, context["pipeline_run_id"], context["transformation_run_id"], started_at, Jsonb(_counts(context))),
                )
        record.update({"load_status": "SUCCESS", "ended_at": datetime.now(timezone.utc).isoformat(), "error_message": ""})
        try:
            _write_local_audit(audit_dir / f"{load_id}.json", record)
        except OSError:
            # The database transaction already committed and has its own load_runs row.
            print("Warning: PostgreSQL load succeeded, but the local audit file could not be written.", file=sys.stderr)
        return record
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except psycopg.Error:
                pass
        record.update({"ended_at": datetime.now(timezone.utc).isoformat(), "error_message": safe_error(exc)})
        try:
            _write_local_audit(audit_dir / f"{load_id}.json", record)
        except OSError:
            print("Warning: local load audit could not be written.", file=sys.stderr)
        if dsn and record["pipeline_run_id"] != "unknown":
            _record_database_failure(dsn, record)
        if isinstance(exc, LoadError):
            raise
        raise LoadError(f"PostgreSQL load failed: {safe_error(exc)}") from exc
    finally:
        if conn is not None:
            conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle_dir", nargs="?", type=Path, help="Validated transformation run folder")
    parser.add_argument("--migrate-only", action="store_true", help="Apply the idempotent PostgreSQL schema migration")
    args = parser.parse_args()
    try:
        if args.migrate_only:
            migrate_database()
            print(json.dumps({"migration": "001_pipeline_load.sql", "status": "SUCCESS"}))
            return 0
        if args.bundle_dir is None:
            parser.error("bundle_dir is required unless --migrate-only is used")
        result = load_approved_bundle(args.bundle_dir)
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0
    except LoadError as exc:
        print(f"Load failed: {safe_error(exc)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
