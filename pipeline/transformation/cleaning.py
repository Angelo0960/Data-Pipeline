"""Input profiling, approved cleaning rules, and quarantine handling."""
from __future__ import annotations

import csv
import json
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

try:
    from .standardize import SOURCE_COLUMNS, TEXT_FIELDS, RowValidationError, standardize_record, validate_window_timestamp
except ImportError:
    from pipeline.transformation.standardize import SOURCE_COLUMNS, TEXT_FIELDS, RowValidationError, standardize_record, validate_window_timestamp

TABLES = tuple(SOURCE_COLUMNS)


class BatchCleaningError(ValueError):
    """A dataset-level input or contract failure that prevents safe profiling."""


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
        whitespace_samples = sorted({
            str(row.get(field) or "") for row in rows
            if str(row.get(field) or "") != str(row.get(field) or "").strip()
        })[:5]
        profile[f"whitespace_values:{field}"] = sum(
            1 for row in rows
            if str(row.get(field) or "") != str(row.get(field) or "").strip()
        )
        profile[f"whitespace_samples:{field}"] = whitespace_samples
        if field in CATEGORY_PROFILE_FIELDS:
            values = [(row.get(field) or "").strip() for row in rows if (row.get(field) or "").strip()]
            profile[f"observed_categories:{field}"] = sorted(set(values))
            profile[f"category_counts:{field}"] = dict(sorted(Counter(values).items()))
    ids = [str(row.get("id", "")).strip() for row in rows if str(row.get("id", "")).strip()]
    id_counts = Counter(ids)
    profile["duplicate_id_rows"] = sum(count - 1 for count in id_counts.values() if count > 1)
    profile["duplicate_id_counts"] = {key: count for key, count in sorted(id_counts.items()) if count > 1}
    signatures = Counter(tuple(sorted(row.items())) for row in rows)
    profile["exact_duplicate_rows"] = sum(count - 1 for count in signatures.values() if count > 1)
    return profile


def validate_extraction_window(window_start: Any, window_end: Any) -> None:
    start = validate_window_timestamp(window_start, "window_start") if window_start else None
    end = validate_window_timestamp(window_end, "window_end") if window_end else None
    if start and end:
        if (start.utcoffset() is None) != (end.utcoffset() is None):
            raise BatchCleaningError("Extraction window mixes aware and naive timestamps")
        if start > end:
            raise BatchCleaningError("window_start cannot be later than window_end")


def clean_records(
    raw_by_table: dict[str, list[dict[str, str]]],
    headers_by_table: dict[str, list[str]],
    pipeline_run_id: str = "",
    window_start: Any = None,
    window_end: Any = None,
) -> tuple[dict[str, list[dict[str, str]]], list[dict[str, str]], dict[str, int], dict[str, Counter[str]], dict[str, dict[str, Any]]]:
    accepted: dict[str, list[dict[str, str]]] = {}
    quarantine: list[dict[str, str]] = []
    duplicate_counts: dict[str, int] = {}
    rule_counts: dict[str, Counter[str]] = {}
    profiles = {table: profile_table(raw_by_table[table], headers_by_table[table]) for table in TABLES}
    parent_ids: set[str] = set()
    start = validate_window_timestamp(window_start, "window_start") if window_start else None
    end = validate_window_timestamp(window_end, "window_end") if window_end else None

    for table in TABLES:
        rows = raw_by_table[table]
        accepted_rows: list[dict[str, str]] = []
        counts: Counter[str] = Counter()
        id_groups: dict[str, list[tuple[int, tuple[tuple[str, str], ...]]]] = {}
        for index, source in enumerate(rows):
            source_id = str(source.get("id", "")).strip()
            try:
                normalized_id = str(uuid.UUID(source_id)) if source_id else ""
            except ValueError:
                normalized_id = ""
            if normalized_id:
                signature = tuple(sorted((key, value or "") for key, value in source.items()))
                id_groups.setdefault(normalized_id, []).append((index, signature))

        conflicting: dict[int, tuple[str, str]] = {}
        exact_duplicate_indices: set[int] = set()
        for normalized_id, entries in id_groups.items():
            signatures = {signature for _, signature in entries}
            if len(signatures) > 1:
                for index, _ in entries:
                    conflicting[index] = ("C-02", f"Conflicting rows share source id {normalized_id}; all copies quarantined")
            else:
                exact_duplicate_indices.update(index for index, _ in entries[1:])

        seen_exact_signatures: set[tuple[tuple[str, str], ...]] = set()
        for index, source in enumerate(rows):
            if index in conflicting or index in exact_duplicate_indices:
                continue
            signature = tuple(sorted((key, value or "") for key, value in source.items()))
            if signature in seen_exact_signatures:
                exact_duplicate_indices.add(index)
            else:
                seen_exact_signatures.add(signature)

        if table == "pig_batches":
            code_groups: dict[str, list[int]] = {}
            for index, source in enumerate(rows):
                code = str(source.get("batch_code", "")).strip()
                if code and index not in exact_duplicate_indices and index not in conflicting:
                    code_groups.setdefault(code, []).append(index)
            for code, indices in code_groups.items():
                distinct_ids = {str(rows[index].get("id", "")).strip() for index in indices}
                if len(indices) > 1 and len(distinct_ids) > 1:
                    for index in indices:
                        conflicting[index] = ("C-03", f"Duplicate batch_code after trimming: {code}; all colliding rows quarantined")

        duplicates = len(exact_duplicate_indices)
        counts["C-02-exact-duplicate"] += duplicates
        for line_index, source in enumerate(rows):
            line_number = line_index + 2
            source_id = str(source.get("id", "")).strip()
            if line_index in exact_duplicate_indices:
                continue
            if line_index in conflicting:
                rule_id, reason = conflicting[line_index]
                quarantine.append({
                    "pipeline_run_id": pipeline_run_id,
                    "table_name": table,
                    "source_row": str(line_number),
                    "source_id": source_id,
                    "batch_id": str(source.get("batch_id") or (source_id if table == "pig_batches" else "")),
                    "rule_id": rule_id,
                    "field": "id" if rule_id == "C-02" else "batch_code",
                    "reason": reason,
                    "record_json": json.dumps(source, ensure_ascii=False, sort_keys=True),
                })
                counts[rule_id] += 1
                counts["quarantined"] += 1
                continue
            try:
                clean = standardize_record(table, source)
                if start or end:
                    created = validate_window_timestamp(clean.get("created_at", ""), "created_at")
                    if created is None:
                        raise RowValidationError("C-07", "created_at", "created_at is missing")
                    for bound in (start, end):
                        if bound and (created.utcoffset() is None) != (bound.utcoffset() is None):
                            raise RowValidationError("C-07", "created_at", "created_at timezone does not match extraction window")
                    if (start and created <= start) or (end and created > end):
                        raise RowValidationError("C-07", "created_at", "created_at is outside the extraction window")
                if table != "pig_batches":
                    batch_id = clean.get("batch_id", "")
                    if not batch_id and table != "expenses":
                        raise RowValidationError("R-01", "batch_id", "child record has no batch_id")
                    if batch_id and batch_id not in parent_ids:
                        raise RowValidationError("R-01", "batch_id", "batch_id does not match an accepted pig_batches.id")
                accepted_rows.append(clean)
                if clean != source:
                    counts["S-01-standardized"] += 1
                if any(
                    str(source.get(field) or "") != str(source.get(field) or "").strip()
                    for field in TEXT_FIELDS.get(table, ())
                ):
                    counts["C-06"] += 1
            except RowValidationError as exc:
                quarantine.append({
                    "pipeline_run_id": pipeline_run_id,
                    "table_name": table,
                    "source_row": str(line_number),
                    "source_id": source_id,
                    "batch_id": str(source.get("batch_id") or (source_id if table == "pig_batches" else "")),
                    "rule_id": exc.rule_id,
                    "field": exc.field,
                    "reason": str(exc),
                    "record_json": json.dumps(source, ensure_ascii=False, sort_keys=True),
                })
                counts[exc.rule_id] += 1
                counts["quarantined"] += 1
        if table == "pig_batches":
            parent_ids = {row["id"] for row in accepted_rows}
        accepted[table] = accepted_rows
        duplicate_counts[table] = duplicates
        rule_counts[table] = counts
    return accepted, quarantine, duplicate_counts, rule_counts, profiles


def profile_and_clean(input_dir: Path, metadata: dict[str, Any]) -> tuple[dict[str, list[dict[str, str]]], dict[str, list[str]], list[dict[str, str]], dict[str, int], dict[str, Counter[str]], dict[str, dict[str, Any]]]:
    raw, headers = load_source_files(input_dir, metadata)
    validate_extraction_window(metadata.get("window_start"), metadata.get("window_end"))
    accepted, quarantine, duplicates, rules, profiles = clean_records(
        raw, headers, str(metadata.get("pipeline_run_id", "")), metadata.get("window_start"), metadata.get("window_end")
    )
    return accepted, headers, quarantine, duplicates, rules, profiles
