"""Input profiling, approved cleaning rules, and quarantine handling."""
from __future__ import annotations

import csv
import json
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

try:
    from .standardize import SOURCE_COLUMNS, RowValidationError, standardize_record, validate_window_timestamp
except ImportError:
    from pipeline.transformation.standardize import SOURCE_COLUMNS, RowValidationError, standardize_record, validate_window_timestamp

TABLES = tuple(SOURCE_COLUMNS)


class BatchCleaningError(ValueError):
    """A dataset-level conflict that cannot be safely resolved row by row."""


def load_source_files(input_dir: Path, metadata: dict[str, Any]) -> tuple[dict[str, list[dict[str, str]]], dict[str, list[str]]]:
    raw: dict[str, list[dict[str, str]]] = {}
    headers_by_table: dict[str, list[str]] = {}
    expected: dict[str, int] = {}
    table_meta = metadata.get("tables", [])
    if isinstance(table_meta, list):
        for item in table_meta:
            if isinstance(item, dict) and item.get("table_name"):
                count = item.get("records_extracted")
                if isinstance(count, int) and count >= 0:
                    expected[str(item["table_name"])] = count
    for table, expected_columns in SOURCE_COLUMNS.items():
        path = input_dir / f"{table}.csv"
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                headers = reader.fieldnames or []
                if not headers:
                    raise BatchCleaningError(f"{table}: CSV has no header")
                missing = sorted(set(expected_columns) - set(headers))
                if missing:
                    raise BatchCleaningError(f"{table}: missing columns: {', '.join(missing)}")
                rows = [dict(row) for row in reader]
        except (OSError, csv.Error) as exc:
            raise BatchCleaningError(f"{table}: could not read CSV: {exc}") from exc
        if any(None in row for row in rows):
            raise BatchCleaningError(f"{table}: a row has more values than the CSV header")
        if table in expected and expected[table] != len(rows):
            raise BatchCleaningError(f"{table}: received {len(rows)} CSV rows but extraction metadata records {expected[table]}")
        raw[table] = rows
        headers_by_table[table] = headers
    return raw, headers_by_table


CATEGORY_PROFILE_FIELDS = {"breed", "status", "feed_type", "vaccine_name", "expense_type"}


def profile_table(rows: list[dict[str, str]], headers: list[str]) -> dict[str, Any]:
    profile: dict[str, Any] = {"rows_read": len(rows), "columns_read": len(headers)}
    for field in headers:
        profile[f"blank_values:{field}"] = sum(1 for row in rows if not (row.get(field) or "").strip())
        if field in CATEGORY_PROFILE_FIELDS:
            profile[f"observed_categories:{field}"] = sorted({
                (row.get(field) or "").strip() for row in rows if (row.get(field) or "").strip()
            })
    ids = [str(row.get("id", "")).strip() for row in rows if str(row.get("id", "")).strip()]
    id_counts = Counter(ids)
    profile["duplicate_id_rows"] = sum(count - 1 for count in id_counts.values() if count > 1)
    signatures = Counter(tuple(sorted(row.items())) for row in rows)
    profile["exact_duplicate_rows"] = sum(count - 1 for count in signatures.values() if count > 1)
    return profile


def validate_extraction_window(rows: list[dict[str, str]], table: str, window_start: Any, window_end: Any) -> None:
    start = validate_window_timestamp(window_start, "window_start") if window_start else None
    end = validate_window_timestamp(window_end, "window_end") if window_end else None
    if start and end:
        if (start.utcoffset() is None) != (end.utcoffset() is None):
            raise BatchCleaningError("Extraction window mixes aware and naive timestamps")
        if start > end:
            raise BatchCleaningError("window_start cannot be later than window_end")
    for line, row in enumerate(rows, start=2):
        try:
            created = validate_window_timestamp(row.get("created_at", ""), "created_at")
        except ValueError as exc:
            raise BatchCleaningError(f"{table} CSV row {line}: {exc}") from exc
        if created is None:
            raise BatchCleaningError(f"{table} CSV row {line}: created_at is missing")
        for bound in (start, end):
            if bound and (created.utcoffset() is None) != (bound.utcoffset() is None):
                raise BatchCleaningError(f"{table} CSV row {line}: created_at timezone does not match extraction window")
        if start and created <= start:
            raise BatchCleaningError(f"{table} CSV row {line}: created_at is outside the extraction window")
        if end and created > end:
            raise BatchCleaningError(f"{table} CSV row {line}: created_at is outside the extraction window")


def clean_records(raw_by_table: dict[str, list[dict[str, str]]], headers_by_table: dict[str, list[str]], pipeline_run_id: str = "") -> tuple[dict[str, list[dict[str, str]]], list[dict[str, str]], dict[str, int], dict[str, Counter[str]], dict[str, dict[str, int]]]:
    accepted: dict[str, list[dict[str, str]]] = {}
    quarantine: list[dict[str, str]] = []
    duplicate_counts: dict[str, int] = {}
    rule_counts: dict[str, Counter[str]] = {}
    profiles = {table: profile_table(raw_by_table[table], headers_by_table[table]) for table in TABLES}
    parent_ids: set[str] = set()

    for table in TABLES:
        rows = raw_by_table[table]
        accepted_rows: list[dict[str, str]] = []
        seen_ids: dict[str, tuple[tuple[str, str], ...]] = {}
        duplicates = 0
        counts: Counter[str] = Counter()
        for line_number, source in enumerate(rows, start=2):
            signature = tuple(sorted((key, value or "") for key, value in source.items()))
            source_id = str(source.get("id", "")).strip()
            try:
                normalized_id = str(uuid.UUID(source_id)) if source_id else ""
            except ValueError:
                normalized_id = ""
            if normalized_id:
                if normalized_id in seen_ids:
                    if signature == seen_ids[normalized_id]:
                        duplicates += 1
                        counts["C-02-exact-duplicate"] += 1
                        continue
                    raise BatchCleaningError(f"{table}: conflicting records share id {normalized_id}; hold batch for review")
                seen_ids[normalized_id] = signature
            try:
                clean = standardize_record(table, source)
                if table != "pig_batches":
                    batch_id = clean.get("batch_id", "")
                    if not batch_id and table != "expenses":
                        raise RowValidationError("R-01", "batch_id", "child record has no batch_id")
                    if batch_id and batch_id not in parent_ids:
                        raise RowValidationError("R-01", "batch_id", "batch_id does not match an accepted pig_batches.id")
                accepted_rows.append(clean)
                if clean != source:
                    counts["S-01-standardized"] += 1
            except RowValidationError as exc:
                quarantine.append({
                    "pipeline_run_id": pipeline_run_id,
                    "table_name": table,
                    "source_row": str(line_number),
                    "source_id": source_id,
                    "rule_id": exc.rule_id,
                    "field": exc.field,
                    "reason": str(exc),
                    "record_json": json.dumps(source, ensure_ascii=False, sort_keys=True),
                })
                counts[exc.rule_id] += 1
                counts["quarantined"] += 1
        if table == "pig_batches":
            codes: dict[str, str] = {}
            for row in accepted_rows:
                code = row["batch_code"].strip()
                if code in codes and codes[code] != row["id"]:
                    raise BatchCleaningError(f"pig_batches: duplicate batch_code after trimming: {code}")
                codes[code] = row["id"]
            parent_ids = {row["id"] for row in accepted_rows}
        accepted[table] = accepted_rows
        duplicate_counts[table] = duplicates
        rule_counts[table] = counts
    return accepted, quarantine, duplicate_counts, rule_counts, profiles


def profile_and_clean(input_dir: Path, metadata: dict[str, Any]) -> tuple[dict[str, list[dict[str, str]]], dict[str, list[str]], list[dict[str, str]], dict[str, int], dict[str, Counter[str]], dict[str, dict[str, int]]]:
    raw, headers = load_source_files(input_dir, metadata)
    for table in TABLES:
        validate_extraction_window(raw[table], table, metadata.get("window_start"), metadata.get("window_end"))
    accepted, quarantine, duplicates, rules, profiles = clean_records(raw, headers, str(metadata.get("pipeline_run_id", "")))
    return accepted, headers, quarantine, duplicates, rules, profiles
