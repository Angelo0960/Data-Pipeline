#!/usr/bin/env python3
"""Validate extracted PrepAPig records before handover.

The validator is independent from Supabase requests. It accepts records and
extraction metadata, then returns table-level validation results together with
an overall handover decision.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

TABLES: dict[str, tuple[str, ...]] = {
    "pig_batches": (
        "id",
        "batch_code",
        "pig_count",
        "breed",
        "start_weight",
        "current_weight",
        "date_acquired",
        "status",
        "created_at",
    ),
    "feed_records": (
        "id",
        "batch_id",
        "feed_type",
        "quantity_kg",
        "feeding_date",
        "feeding_time",
        "notes",
        "created_at",
    ),
    "vaccination_records": (
        "id",
        "batch_id",
        "vaccine_name",
        "vaccination_date",
        "next_due_date",
        "administered_by",
        "dosage",
        "notes",
        "status",
        "created_at",
    ),
    "expenses": (
        "id",
        "batch_id",
        "expense_type",
        "amount",
        "expense_date",
        "description",
        "created_at",
    ),
}


class ValidationInputError(ValueError):
    """Raised when validation input cannot be evaluated safely."""


def parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValidationInputError(f"Invalid timestamp: {value}") from exc
    else:
        raise ValidationInputError(f"Unsupported timestamp value: {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _record_timestamp_error(table: str, index: int, message: str) -> str:
    return f"{table}[{index}]: {message}"


def _unique_duplicates(values: list[Any]) -> list[Any]:
    counts: dict[Any, int] = {}
    for value in values:
        if value:
            counts[value] = counts.get(value, 0) + 1
    return sorted(value for value, count in counts.items() if count > 1)


def _available_columns(
    records: list[dict[str, Any]], declared_columns: Mapping[str, list[str]] | None, table: str
) -> set[str]:
    if declared_columns and table in declared_columns:
        return set(declared_columns[table])
    return set().union(*(record.keys() for record in records)) if records else set()


def validate_extraction(
    records_by_table: Mapping[str, list[dict[str, Any]]],
    *,
    window_start: Any = None,
    window_end: Any = None,
    api_counts: Mapping[str, int] | None = None,
    extracted_counts: Mapping[str, int] | None = None,
    csv_counts: Mapping[str, int] | None = None,
    declared_columns: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Validate records and return PASS/WARNING/FAIL plus handover decision.

    `expenses.batch_id` may be null for farm-level expenses and produces a
    warning. All other missing or invalid required relationships fail.
    """
    start = parse_timestamp(window_start)
    end = parse_timestamp(window_end)
    if start and end and start > end:
        raise ValidationInputError("window_start cannot be later than window_end")

    failures: list[str] = []
    warnings: list[str] = []
    table_results: dict[str, dict[str, Any]] = {}
    normalized: dict[str, list[dict[str, Any]]] = {}

    unknown_tables = sorted(set(records_by_table) - set(TABLES))
    for table in unknown_tables:
        warnings.append(f"{table}: table is outside the documented extraction scope")

    for table, required_columns in TABLES.items():
        records = records_by_table.get(table, [])
        if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
            raise ValidationInputError(f"{table}: records must be a list of objects")
        normalized[table] = records

        available = _available_columns(records, declared_columns, table)
        missing_columns = sorted(set(required_columns) - available)
        ids = [record.get("id") for record in records]
        missing_ids = sum(1 for value in ids if not value)
        duplicate_ids = _unique_duplicates(ids)
        table_failures: list[str] = []
        table_warnings: list[str] = []

        if missing_columns:
            table_failures.append(f"{table}: missing columns {', '.join(missing_columns)}")
        if missing_ids:
            table_failures.append(f"{table}: {missing_ids} record(s) missing id")
        if duplicate_ids:
            table_failures.append(f"{table}: duplicate id values detected")

        for index, record in enumerate(records):
            created_at = record.get("created_at")
            if start or end:
                try:
                    created = parse_timestamp(created_at)
                except ValidationInputError as exc:
                    table_failures.append(_record_timestamp_error(table, index, str(exc)))
                    continue
                if created is None:
                    table_failures.append(
                        _record_timestamp_error(table, index, "missing created_at")
                    )
                    continue
                if start and created <= start:
                    table_failures.append(
                        _record_timestamp_error(table, index, "created_at is outside the extraction window")
                    )
                if end and created > end:
                    table_failures.append(
                        _record_timestamp_error(table, index, "created_at is outside the extraction window")
                    )

        table_results[table] = {
            "table_name": table,
            "required_columns_present": not missing_columns,
            "missing_columns": missing_columns,
            "ids_present": missing_ids == 0,
            "duplicate_ids": duplicate_ids,
            "records_read": len(records),
            "records_extracted": len(records),
            "csv_rows_written": None,
            "records_rejected": 0,
            "failures": table_failures,
            "warnings": table_warnings,
            "validation_result": "FAIL" if table_failures else "PASS",
        }
        failures.extend(table_failures)

    batch_ids = {record.get("id") for record in normalized["pig_batches"] if record.get("id")}
    batch_codes = [record.get("batch_code") for record in normalized["pig_batches"]]
    duplicate_codes = _unique_duplicates(batch_codes)
    if duplicate_codes:
        message = "pig_batches: duplicate batch_code values detected"
        failures.append(message)
        table_results["pig_batches"]["failures"].append(message)
        table_results["pig_batches"]["duplicate_batch_codes"] = duplicate_codes

    for table in ("feed_records", "vaccination_records", "expenses"):
        result = table_results[table]
        for index, record in enumerate(normalized[table]):
            batch_id = record.get("batch_id")
            if not batch_id:
                if table == "expenses":
                    message = f"{table}[{index}]: farm-level expense has no batch_id"
                    warnings.append(message)
                    result["warnings"].append(message)
                else:
                    message = f"{table}[{index}]: missing batch_id"
                    failures.append(message)
                    result["failures"].append(message)
                continue
            if batch_id not in batch_ids:
                message = f"{table}[{index}]: batch_id does not reference pig_batches.id"
                failures.append(message)
                result["failures"].append(message)

        result["batch_relationships_valid"] = not result["failures"]
        result["null_batch_ids"] = sum(
            1 for record in normalized[table] if not record.get("batch_id")
        )
        result["validation_result"] = (
            "FAIL" if result["failures"] else "WARNING" if result["warnings"] else "PASS"
        )

    count_maps = {
        "api_records": api_counts,
        "extracted_records": extracted_counts,
        "csv_rows": csv_counts,
    }
    for table in TABLES:
        supplied = {name: values.get(table) for name, values in count_maps.items() if values is not None}
        table_results[table]["counts"] = supplied
        if csv_counts and table in csv_counts:
            table_results[table]["csv_rows_written"] = csv_counts[table]
        if len(supplied) >= 2 and len(set(supplied.values())) != 1:
            message = f"{table}: API, extracted, and CSV record counts do not match"
            failures.append(message)
            table_results[table]["failures"].append(message)
            table_results[table]["validation_result"] = "FAIL"

    validation_result = "FAIL" if failures else "WARNING" if warnings else "PASS"
    handover_decision = (
        "REJECTED" if failures else "MANUAL REVIEW" if warnings else "APPROVED"
    )
    extraction_status = (
        "FAILED" if failures else "WARNING" if warnings else "SUCCESS"
    )

    return {
        "validation_result": validation_result,
        "handover_decision": handover_decision,
        "extraction_status": extraction_status,
        "transformation_allowed": handover_decision == "APPROVED",
        "watermark_update_allowed": handover_decision == "APPROVED",
        "failures": failures,
        "warnings": warnings,
        "tables": table_results,
        "window_start": window_start,
        "window_end": window_end,
    }


def validate_records(records_by_table: Mapping[str, list[dict[str, Any]]], **kwargs: Any) -> dict[str, Any]:
    """Compatibility alias for callers that use the extraction terminology."""
    return validate_extraction(records_by_table, **kwargs)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationInputError(f"Could not read JSON input {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValidationInputError("Validation input must be a JSON object")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="JSON file containing extraction records and metadata")
    parser.add_argument("--output", type=Path, help="Optional JSON output path")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        payload = _load_json(args.input)
        result = validate_extraction(
            payload.get("records_by_table", {}),
            window_start=payload.get("window_start"),
            window_end=payload.get("window_end"),
            api_counts=payload.get("api_counts"),
            extracted_counts=payload.get("extracted_counts"),
            csv_counts=payload.get("csv_counts"),
            declared_columns=payload.get("declared_columns"),
        )
    except ValidationInputError as exc:
        print(f"Validation input failed: {exc}", file=sys.stderr)
        return 2

    serialized = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized, encoding="utf-8")
    else:
        print(serialized, end="")
    return 0 if result["handover_decision"] != "REJECTED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
