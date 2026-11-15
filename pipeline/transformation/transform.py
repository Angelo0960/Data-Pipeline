#!/usr/bin/env python3
"""Transform an approved PrepAPig extraction handover into validated CSV outputs.

This stage reads CSV files only. It does not connect to Supabase, update the
extraction watermark, or load data into PostgreSQL.

Example:
    python pipeline/transform.py \
      --input-dir data/raw/PREPAPIG_20261107_020000_ab12cd34 \
      --metadata data/logs/extraction/audit/PREPAPIG_20261107_020000_ab12cd34.json \
      --output-dir data
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import sys
import tempfile
import time
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, time as datetime_time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

try:
    from .business_rules import apply_business_rules
    from .cleaning import BatchCleaningError, profile_and_clean
    from .transform_log import safe_message, write_central_records, write_package_log
    from .transform_validation import (
        OutputValidationError,
        make_temp_dir,
        publish_directory as publish_outputs,
        validate_outputs,
        write_outputs,
    )
except ImportError:  # Support direct execution from pipeline/transformation.
    from business_rules import apply_business_rules
    from cleaning import BatchCleaningError, profile_and_clean
    from transform_log import safe_message, write_central_records, write_package_log
    from transform_validation import (
        OutputValidationError,
        make_temp_dir,
        publish_directory as publish_outputs,
        validate_outputs,
        write_outputs,
    )

TABLES: dict[str, tuple[str, ...]] = {
    "pig_batches": (
        "id", "batch_code", "pig_count", "breed", "start_weight",
        "current_weight", "date_acquired", "status", "created_at",
    ),
    "feed_records": (
        "id", "batch_id", "feed_type", "quantity_kg", "feeding_date",
        "feeding_time", "notes", "created_at",
    ),
    "vaccination_records": (
        "id", "batch_id", "vaccine_name", "vaccination_date",
        "next_due_date", "administered_by", "dosage", "notes", "status",
        "created_at",
    ),
    "expenses": (
        "id", "batch_id", "expense_type", "amount", "expense_date",
        "description", "created_at",
    ),
}

REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "pig_batches": ("id", "batch_code", "pig_count", "created_at"),
    "feed_records": (
        "id", "batch_id", "feed_type", "quantity_kg", "feeding_date", "created_at",
    ),
    "vaccination_records": (
        "id", "batch_id", "vaccine_name", "vaccination_date", "created_at",
    ),
    "expenses": (
        "id", "expense_type", "amount", "expense_date", "created_at",
    ),
}

UUID_FIELDS = {"id", "batch_id"}
DECIMAL_FIELDS = {
    "pig_batches": ("start_weight", "current_weight"),
    "feed_records": ("quantity_kg",),
    "expenses": ("amount",),
}
DATE_FIELDS = {
    "pig_batches": ("date_acquired",),
    "feed_records": ("feeding_date",),
    "vaccination_records": ("vaccination_date", "next_due_date"),
    "expenses": ("expense_date",),
}
TEXT_FIELDS = {
    "pig_batches": ("batch_code", "breed", "status"),
    "feed_records": ("feed_type", "notes"),
    "vaccination_records": ("vaccine_name", "administered_by", "dosage", "notes", "status"),
    "expenses": ("expense_type", "description"),
}
LOG_FIELDS = (
    "transformation_run_id", "pipeline_run_id", "transformation_status",
    "transformation_start", "transformation_end", "duration_seconds",
    "output_location", "message",
)


class TransformError(RuntimeError):
    """Raised when a handover or transformation cannot safely complete."""


class RowIssue(ValueError):
    """Raised for a row-level data quality issue that should be quarantined."""


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def write_json(path: Path, value: Any) -> None:
    atomic_write_text(path, json.dumps(value, indent=2, ensure_ascii=False, default=str) + "\n")


def read_metadata(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TransformError(f"Cannot read extraction metadata: {exc}") from exc
    if not isinstance(value, dict):
        raise TransformError("Extraction metadata must be a JSON object")
    decision = str(value.get("handover_decision", "")).strip().upper()
    if decision != "APPROVED":
        raise TransformError("Extraction handover must have handover_decision=APPROVED")
    status = str(value.get("extraction_status", "")).strip().upper()
    if status and status != "SUCCESS":
        raise TransformError(f"Extraction status is {status}; expected SUCCESS")
    validation = str(value.get("validation_result", "")).strip().upper()
    if validation and validation not in {"PASS", "APPROVED"}:
        raise TransformError(f"Extraction validation is {validation}; expected PASS")
    run_id = str(value.get("pipeline_run_id", "")).strip()
    if not run_id or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id):
        raise TransformError("Metadata must contain a safe pipeline_run_id")
    if run_id in {".", ".."} or ".." in run_id:
        raise TransformError("Unsafe pipeline_run_id")
    return value


def load_csv(path: Path, table: str) -> tuple[list[dict[str, str]], list[str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            headers = reader.fieldnames or []
            if not headers:
                raise TransformError(f"{table}: CSV is empty or has no header")
            missing = sorted(set(TABLES[table]) - set(headers))
            if missing:
                raise TransformError(f"{table}: missing required columns: {', '.join(missing)}")
            rows = [dict(row) for row in reader]
    except (OSError, csv.Error) as exc:
        raise TransformError(f"{table}: cannot read CSV: {exc}") from exc
    if any(None in row for row in rows):
        raise TransformError(f"{table}: a row has more values than the CSV header")
    return rows, headers


def normalize_uuid(value: str, field: str) -> str:
    text = (value or "").strip()
    if not text:
        raise RowIssue(f"{field} is missing")
    try:
        return str(uuid.UUID(text))
    except (ValueError, AttributeError) as exc:
        raise RowIssue(f"{field} is not a valid UUID") from exc


def parse_decimal(value: str, field: str, *, required: bool, nonnegative: bool = True) -> str:
    text = (value or "").strip()
    if not text:
        if required:
            raise RowIssue(f"{field} is missing")
        return ""
    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        raise RowIssue(f"{field} is not a valid decimal") from exc
    if not number.is_finite():
        raise RowIssue(f"{field} must be a finite number")
    if nonnegative and number < 0:
        raise RowIssue(f"{field} must not be negative")
    try:
        quantized = number.quantize(Decimal("0.01"))
    except InvalidOperation as exc:
        raise RowIssue(f"{field} exceeds supported decimal precision") from exc
    if number != quantized:
        raise RowIssue(f"{field} has more than two meaningful decimal places")
    if quantized > Decimal("99999999.99"):
        raise RowIssue(f"{field} exceeds the documented DECIMAL(10,2) range")
    return format(quantized, ".2f")


def parse_date(value: str, field: str, *, required: bool) -> str:
    text = (value or "").strip()
    if not text:
        if required:
            raise RowIssue(f"{field} is missing")
        return ""
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise RowIssue(f"{field} must use an unambiguous YYYY-MM-DD date") from exc


def parse_timestamp(value: str, field: str) -> str:
    text = (value or "").strip()
    if not text:
        raise RowIssue(f"{field} is missing")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RowIssue(f"{field} is not a valid ISO timestamp") from exc
    # Keep the source offset if supplied; do not invent a timezone.
    return parsed.isoformat()


def parse_time(value: str, field: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    try:
        parsed = datetime_time.fromisoformat(text)
    except ValueError as exc:
        raise RowIssue(f"{field} is not a valid time") from exc
    return parsed.isoformat(timespec="seconds" if parsed.second or parsed.microsecond else "minutes")


def cleaned_row(table: str, source: dict[str, str], required: Iterable[str]) -> dict[str, str]:
    row = {key: (value or "") for key, value in source.items()}
    required_set = set(required)
    for key in required_set:
        if not str(row.get(key, "")).strip():
            raise RowIssue(f"{key} is required")
    for field in UUID_FIELDS:
        if field in row and (field in required_set or row[field].strip()):
            row[field] = normalize_uuid(row[field], field)
    if table == "pig_batches":
        try:
            count = int(row["pig_count"].strip())
        except (ValueError, AttributeError) as exc:
            raise RowIssue("pig_count must be a whole number") from exc
        if count < 0:
            raise RowIssue("pig_count must not be negative")
        if count > 2147483647:
            raise RowIssue("pig_count exceeds the documented INTEGER range")
        row["pig_count"] = str(count)
    for field in DECIMAL_FIELDS.get(table, ()):
        row[field] = parse_decimal(row.get(field, ""), field, required=field in required_set)
    for field in DATE_FIELDS.get(table, ()):
        row[field] = parse_date(row.get(field, ""), field, required=field in required_set)
    row["created_at"] = parse_timestamp(row.get("created_at", ""), "created_at")
    if table == "feed_records":
        row["feeding_time"] = parse_time(row.get("feeding_time", ""), "feeding_time")
    for field in TEXT_FIELDS.get(table, ()):
        row[field] = (row.get(field) or "").strip()
    return row


def rule_id_for_issue(table: str, reason: str) -> str:
    message = reason.lower()
    if "batch_id" in message:
        return "R-01"
    if "id" in message and ("uuid" in message or "missing" in message or "required" in message):
        return "C-01"
    if "batch_code" in message:
        return "C-03"
    if any(token in message for token in ("decimal", "numeric", "number", "pig_count", "weight", "quantity", "amount")):
        return "C-05"
    if any(token in message for token in ("date", "time", "timestamp", "created_at")):
        return "C-07"
    if "required" in message or "missing" in message:
        return "C-04"
    return "C-09"


def read_table_set(input_dir: Path, metadata: dict[str, Any]) -> tuple[dict[str, list[dict[str, str]]], dict[str, list[str]]]:
    rows_by_table: dict[str, list[dict[str, str]]] = {}
    headers_by_table: dict[str, list[str]] = {}
    table_meta = metadata.get("tables", [])
    expected_counts: dict[str, int] = {}
    if isinstance(table_meta, list):
        for item in table_meta:
            if isinstance(item, dict) and item.get("table_name"):
                count = item.get("records_extracted")
                if isinstance(count, int) and count >= 0:
                    expected_counts[str(item["table_name"])] = count
    for table in TABLES:
        path = input_dir / f"{table}.csv"
        rows, headers = load_csv(path, table)
        if table in expected_counts and len(rows) != expected_counts[table]:
            raise TransformError(
                f"{table}: CSV has {len(rows)} rows but extraction metadata records {expected_counts[table]}"
            )
        rows_by_table[table] = rows
        headers_by_table[table] = headers
    return rows_by_table, headers_by_table


def validate_window(rows: list[dict[str, str]], table: str, window_start: Any, window_end: Any) -> None:
    """Confirm source created_at values fall within the recorded extraction window."""
    if not window_start and not window_end:
        return
    try:
        start = datetime.fromisoformat(str(window_start).replace("Z", "+00:00")) if window_start else None
        end = datetime.fromisoformat(str(window_end).replace("Z", "+00:00")) if window_end else None
    except ValueError as exc:
        raise TransformError("Extraction metadata contains an invalid window timestamp") from exc
    def is_aware(value: datetime | None) -> bool:
        return value is not None and value.utcoffset() is not None

    if start and end and is_aware(start) != is_aware(end):
        raise TransformError("Extraction window mixes timezone-aware and timezone-naive timestamps")
    if start and end and start > end:
        raise TransformError("window_start cannot be later than window_end")
    for index, row in enumerate(rows, start=2):
        raw = (row.get("created_at") or "").strip()
        if not raw:
            raise TransformError(f"{table} CSV row {index}: created_at missing; cannot verify extraction window")
        try:
            created = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise TransformError(f"{table} CSV row {index}: created_at is not a valid timestamp") from exc
        for bound in (start, end):
            if bound and is_aware(created) != is_aware(bound):
                raise TransformError(
                    f"{table} CSV row {index}: created_at and extraction window use inconsistent timezone information"
                )
        if start and created <= start:
            raise TransformError(f"{table} CSV row {index}: created_at is not after window_start")
        if end and created > end:
            raise TransformError(f"{table} CSV row {index}: created_at is after window_end")


def process_records(
    table: str,
    raw_rows: list[dict[str, str]],
    parent_ids: set[str] | None,
    quarantine: list[dict[str, str]],
) -> tuple[list[dict[str, str]], int, Counter[str]]:
    accepted: list[dict[str, str]] = []
    seen_id_signatures: dict[str, tuple[tuple[str, str], ...]] = {}
    duplicate_count = 0
    rule_counts: Counter[str] = Counter()
    for line_number, raw in enumerate(raw_rows, start=2):
        raw_signature = tuple(sorted((key, value or "") for key, value in raw.items()))
        source_id = str(raw.get("id", "")).strip()
        try:
            normalized_source_id = normalize_uuid(source_id, "id") if source_id else ""
        except RowIssue:
            normalized_source_id = ""
        if normalized_source_id:
            if normalized_source_id in seen_id_signatures:
                if raw_signature == seen_id_signatures[normalized_source_id]:
                    duplicate_count += 1
                    rule_counts["C-02-exact-duplicate"] += 1
                    continue
                raise TransformError(
                    f"{table}: conflicting rows share id {normalized_source_id}; hold the batch for review"
                )
            seen_id_signatures[normalized_source_id] = raw_signature
        try:
            row = cleaned_row(table, raw, REQUIRED_FIELDS[table])
            record_id = row["id"]
            if table != "pig_batches":
                batch_id = row.get("batch_id", "")
                if not batch_id and table != "expenses":
                    raise RowIssue("batch_id is required for a child record")
                if batch_id and parent_ids is not None and batch_id not in parent_ids:
                    raise RowIssue("batch_id does not match an accepted pig_batches.id")
            if table == "pig_batches":
                code = row.get("batch_code", "").strip()
                if not code:
                    raise RowIssue("batch_code is required")
            accepted.append(row)
            if row != raw:
                rule_counts["S-01-standardized"] += 1
        except RowIssue as exc:
            quarantine.append({
                "table_name": table,
                "source_row": str(line_number),
                "source_id": source_id,
                "rule_id": rule_id_for_issue(table, str(exc)),
                "reason": str(exc),
                "record_json": json.dumps(raw, ensure_ascii=False, sort_keys=True),
            })
            rule_counts["quarantined"] += 1
    if table == "pig_batches":
        codes: dict[str, str] = {}
        for row in accepted:
            code = row["batch_code"].strip()
            if code in codes and codes[code] != row["id"]:
                raise TransformError(f"pig_batches: duplicate batch_code after trimming: {code}")
            codes[code] = row["id"]
    return accepted, duplicate_count, rule_counts


def add_weight_change(rows: list[dict[str, str]]) -> None:
    for row in rows:
        start = row.get("start_weight", "")
        current = row.get("current_weight", "")
        if not start or not current:
            row["weight_change"] = ""
        else:
            row["weight_change"] = format(Decimal(current) - Decimal(start), ".2f")


def build_summaries(
    feed_rows: list[dict[str, str]],
    expense_rows: list[dict[str, str]],
    pipeline_run_id: str,
    window_start: Any,
    window_end: Any,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    feed_totals: defaultdict[str, Decimal] = defaultdict(Decimal)
    expense_totals: defaultdict[str, Decimal] = defaultdict(Decimal)
    farm_expense_totals: defaultdict[str, Decimal] = defaultdict(Decimal)
    farm_expense_counts: Counter[str] = Counter()
    for row in feed_rows:
        feed_totals[row["batch_id"]] += Decimal(row["quantity_kg"])
    for row in expense_rows:
        batch_id = row.get("batch_id", "").strip()
        if batch_id:
            expense_totals[batch_id] += Decimal(row["amount"])
        else:
            expense_type = row["expense_type"]
            farm_expense_totals[expense_type] += Decimal(row["amount"])
            farm_expense_counts[expense_type] += 1
    start = str(window_start or "")
    end = str(window_end or "")
    feed_summary = [
        {"batch_id": key, "total_feed_kg": format(value, ".2f"),
         "pipeline_run_id": pipeline_run_id, "window_start": start, "window_end": end}
        for key, value in sorted(feed_totals.items())
    ]
    expense_summary = [
        {"batch_id": key, "total_expense": format(value, ".2f"),
         "pipeline_run_id": pipeline_run_id, "window_start": start, "window_end": end}
        for key, value in sorted(expense_totals.items())
    ]
    farm_expense_summary = [
        {"expense_type": key, "record_count": str(farm_expense_counts[key]),
         "total_expense": format(value, ".2f"), "pipeline_run_id": pipeline_run_id,
         "window_start": start, "window_end": end}
        for key, value in sorted(farm_expense_totals.items())
    ]
    return feed_summary, expense_summary, farm_expense_summary


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def output_columns(table: str, input_headers: list[str], rows: list[dict[str, str]]) -> list[str]:
    # Keep every source column, including source columns not yet listed in the
    # current transformation contract, to avoid silently losing source data.
    columns = list(input_headers)
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    if table == "pig_batches" and "weight_change" not in columns:
        columns.append("weight_change")
    return columns


def write_quarantine(path: Path, records: list[dict[str, str]]) -> None:
    columns = ["pipeline_run_id", "table_name", "source_row", "source_id", "batch_id", "rule_id", "reason", "record_json"]
    write_csv(path, records, columns)


def profile_table(rows: list[dict[str, str]], headers: list[str]) -> dict[str, int]:
    profile = {"rows_read": len(rows), "columns_read": len(headers)}
    for field in headers:
        profile[f"blank_values:{field}"] = sum(1 for row in rows if not (row.get(field) or "").strip())
    ids = [str(row.get("id", "")).strip() for row in rows if str(row.get("id", "")).strip()]
    id_counts = Counter(ids)
    profile["duplicate_id_rows"] = sum(count - 1 for count in id_counts.values() if count > 1)
    row_signatures = Counter(tuple(sorted(row.items())) for row in rows)
    profile["exact_duplicate_rows"] = sum(count - 1 for count in row_signatures.values() if count > 1)
    return profile


def write_quality_report(
    path: Path,
    summary: dict[str, dict[str, int]],
    rule_counts: dict[str, Counter[str]],
    profiles: dict[str, dict[str, int]],
) -> None:
    rows: list[dict[str, str]] = []
    for table, metrics in profiles.items():
        for metric, value in metrics.items():
            rows.append({"table_name": table, "metric": f"profile:{metric}", "count": str(value), "details": ""})
    for table, counts in summary.items():
        for metric, value in counts.items():
            rows.append({"table_name": table, "metric": metric, "count": str(value), "details": ""})
    for table, counts in rule_counts.items():
        for rule, value in counts.items():
            rows.append({"table_name": table, "metric": "rule_count", "count": str(value), "details": rule})
    write_csv(path, rows, ["table_name", "metric", "count", "details"])


def append_operational_log(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existed = path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LOG_FIELDS, extrasaction="ignore")
        if not existed:
            writer.writeheader()
        writer.writerow({key: record.get(key, "") for key in LOG_FIELDS})
        handle.flush()
        os.fsync(handle.fileno())


def publish_directory(temp_dir: Path, final_dir: Path) -> None:
    """Replace a prior run's output only after the new run is fully prepared."""
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    backup = final_dir.with_name(f".{final_dir.name}.{uuid.uuid4().hex}.bak")
    had_previous = final_dir.exists()
    try:
        if had_previous:
            final_dir.rename(backup)
        temp_dir.rename(final_dir)
    except OSError:
        if had_previous and backup.exists() and not final_dir.exists():
            backup.rename(final_dir)
        raise
    if backup.exists():
        shutil.rmtree(backup)


def transform(args: argparse.Namespace) -> dict[str, Any]:
    started = now_utc()
    timer = time.perf_counter()
    transformation_run_id = f"TRN_{started.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    output_root = args.output_dir.resolve()
    pipeline_run_id = "unknown"
    lock_path: Path | None = None
    temp_dir: Path | None = None
    final_dir: Path | None = None
    window_start: Any = ""
    window_end: Any = ""
    try:
        output_root.mkdir(parents=True, exist_ok=True)
        metadata = read_metadata(args.metadata)
        pipeline_run_id = str(metadata["pipeline_run_id"])
        lock_dir = output_root / "locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        lock_path = lock_dir / f"{pipeline_run_id}.lock"
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
        except FileExistsError as exc:
            raise TransformError(f"A transformation for {pipeline_run_id} is already running (or has a stale lock)") from exc

        window_start = metadata.get("window_start")
        window_end = metadata.get("window_end")
        output_parent = output_root / "transformed"
        final_dir = output_parent / pipeline_run_id
        write_central_records(output_root, {
            "transformation_run_id": transformation_run_id,
            "pipeline_run_id": pipeline_run_id,
            "transformation_status": "RUNNING",
            "handover_decision": "APPROVED",
            "transformation_start": iso_utc(started),
            "window_start": window_start,
            "window_end": window_end,
            "output_location": str(final_dir),
            "message": "Transformation started",
        })

        accepted, headers, quarantine, duplicate_counts, rule_counts, profiles = profile_and_clean(
            args.input_dir, metadata
        )
        raw_counts = {table: int(profiles[table]["rows_read"]) for table in TABLES}
        transformed, feed_summary, expense_summary, farm_expense_summary = apply_business_rules(
            accepted, pipeline_run_id, window_start, window_end
        )
        summary_counts = validate_outputs(
            raw_counts, transformed, quarantine, duplicate_counts,
            feed_summary, expense_summary, farm_expense_summary, rule_counts,
        )
        transformation_status = "WARNING" if quarantine else "PASS"
        decision = "MANUAL REVIEW" if quarantine else "APPROVED"
        output_parent.mkdir(parents=True, exist_ok=True)
        target_dir = final_dir
        temp_dir = make_temp_dir(output_parent, pipeline_run_id)
        ended = now_utc()
        result = {
            "transformation_run_id": transformation_run_id,
            "pipeline_run_id": pipeline_run_id,
            "window_start": window_start,
            "window_end": window_end,
            "transformation_start": iso_utc(started),
            "transformation_end": iso_utc(ended),
            "duration_seconds": round(time.perf_counter() - timer, 3),
            "transformation_status": transformation_status,
            "handover_decision": decision,
            "counts": summary_counts,
            "rejection_policy": {
                "automatic_approval_threshold_percent": 0.0,
                "rates_by_table_percent": {
                    table: round((100.0 * values["quarantined_records"] / values["input_records"]), 2)
                    if values["input_records"] else 0.0
                    for table, values in summary_counts.items()
                },
                "action": "Any quarantined row requires MANUAL REVIEW; no quarantined bundle is automatically approved.",
            },
            "summary_rows": {
                "feed_summary": len(feed_summary),
                "expense_summary": len(expense_summary),
                "farm_expense_summary": len(farm_expense_summary),
            },
            "farm_level_expense_records": sum(
                1 for row in transformed["expenses"] if not (row.get("batch_id") or "").strip()
            ),
            "quarantined_rows": len(quarantine),
            "duplicate_copies_removed": sum(duplicate_counts.values()),
            "rules_applied": {table: dict(counts) for table, counts in rule_counts.items()},
            "output_location": str(target_dir),
            "extraction_watermark_changed": False,
            "error_message": "",
        }
        write_outputs(
            temp_dir, transformed, headers, feed_summary, expense_summary,
            farm_expense_summary, quarantine, profiles, rule_counts, result,
        )
        log_record = {
            **result,
            "input_records": sum(item["input_records"] for item in summary_counts.values()),
            "accepted_records": sum(item["accepted_records"] for item in summary_counts.values()),
            "quarantined_records": len(quarantine),
            "duplicate_copies_removed": sum(duplicate_counts.values()),
            "validation_results": {"transformation": transformation_status, "handover": decision},
            "warnings": ["Quarantined rows require review"] if quarantine else [],
            "message": "Transformation output published" if not quarantine else "Output published with quarantined rows; review required",
        }
        write_package_log(temp_dir, log_record)
        publish_outputs(temp_dir, target_dir)
        temp_dir = None
        try:
            write_central_records(output_root, log_record)
        except OSError as exc:
            print(
                f"Warning: central log copy could not be written; per-run log is available in {final_dir}: "
                f"{safe_message(exc)}",
                file=sys.stderr,
            )
        print(json.dumps(log_record, indent=2, ensure_ascii=False, default=str))
        return log_record
    except Exception as exc:
        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        failure = {
            "transformation_run_id": transformation_run_id,
            "pipeline_run_id": pipeline_run_id,
            "transformation_status": "FAIL",
            "transformation_start": iso_utc(started),
            "transformation_end": iso_utc(now_utc()),
            "duration_seconds": round(time.perf_counter() - timer, 3),
            "window_start": window_start,
            "window_end": window_end,
            "output_location": str(final_dir or ""),
            "message": safe_message(exc),
        }
        try:
            write_central_records(output_root, failure)
        except OSError as log_exc:
            print(f"Failure log could not be written: {safe_message(log_exc)}", file=sys.stderr)
        if isinstance(exc, TransformError):
            raise
        if isinstance(exc, (BatchCleaningError, OutputValidationError, csv.Error, OSError, ValueError, RuntimeError)):
            raise TransformError(safe_message(exc)) from exc
        raise
    finally:
        if lock_path and lock_path.exists():
            try:
                lock_path.unlink()
            except OSError as exc:
                print(f"Warning: could not remove transformation lock: {safe_message(exc)}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path, help="Folder containing the four extraction CSV files")
    parser.add_argument("--metadata", required=True, type=Path, help="Approved extraction metadata JSON file")
    parser.add_argument("--output-dir", type=Path, default=Path("data"), help="Base folder for transformed data and logs")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = transform(args)
    except Exception as exc:
        print(f"Transformation failed: {safe_message(exc)}", file=sys.stderr)
        return 1
    return 0 if result["handover_decision"] == "APPROVED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
