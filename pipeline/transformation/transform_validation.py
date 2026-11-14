"""Transformation validation, reconciliation, and safe output publishing."""
from __future__ import annotations

import csv
import json
import shutil
import tempfile
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

try:
    from .business_rules import validate_summaries
except ImportError:
    from pipeline.transformation.business_rules import validate_summaries

TABLE_FILES = {
    "pig_batches": "pig_batches_transformed.csv",
    "feed_records": "feed_records_transformed.csv",
    "vaccination_records": "vaccination_records_transformed.csv",
    "expenses": "expenses_transformed.csv",
}


class OutputValidationError(ValueError):
    pass


def validate_outputs(
    raw_counts: dict[str, int],
    transformed: dict[str, list[dict[str, str]]],
    quarantine: list[dict[str, str]],
    duplicates: dict[str, int],
    feed_summary: list[dict[str, str]],
    expense_summary: list[dict[str, str]],
    farm_expense_summary: list[dict[str, str]],
    rule_counts: dict[str, Counter[str]] | None = None,
) -> dict[str, dict[str, int]]:
    errors: list[str] = []
    counts: dict[str, dict[str, int]] = {}
    parents = {row["id"] for row in transformed["pig_batches"]}
    for table, rows in transformed.items():
        ids = [row.get("id", "") for row in rows]
        if len(ids) != len(set(ids)):
            errors.append(f"{table}: duplicate primary keys remain")
        if any(not value for value in ids):
            errors.append(f"{table}: accepted row is missing id")
        if table == "pig_batches":
            codes = [row.get("batch_code", "").strip() for row in rows]
            if len(codes) != len(set(codes)):
                errors.append("pig_batches: duplicate batch_code values remain")
        else:
            for row in rows:
                batch_id = (row.get("batch_id") or "").strip()
                if (table != "expenses" and not batch_id) or (batch_id and batch_id not in parents):
                    errors.append(f"{table}: orphan batch_id remains")
                    break
        rejected = sum(1 for item in quarantine if item.get("table_name") == table)
        duplicate_count = duplicates.get(table, 0)
        accepted_count = len(rows)
        if raw_counts.get(table, 0) != accepted_count + rejected + duplicate_count:
            errors.append(f"{table}: input/accepted/quarantine/duplicate counts do not reconcile")
        counts[table] = {
            "input_records": raw_counts.get(table, 0),
            "accepted_records": accepted_count,
            "quarantined_records": rejected,
            "duplicate_copies_removed": duplicate_count,
            "modified_records": int((rule_counts or {}).get(table, Counter()).get("S-01-standardized", 0)),
        }
    errors.extend(validate_summaries(
        transformed["feed_records"], transformed["expenses"], feed_summary,
        expense_summary, farm_expense_summary,
    ))
    if errors:
        raise OutputValidationError("; ".join(dict.fromkeys(errors)))
    return counts


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def write_outputs(
    folder: Path,
    transformed: dict[str, list[dict[str, str]]],
    headers: dict[str, list[str]],
    feed_summary: list[dict[str, str]],
    expense_summary: list[dict[str, str]],
    farm_expense_summary: list[dict[str, str]],
    quarantine: list[dict[str, str]],
    profiles: dict[str, dict[str, Any]],
    rule_counts: dict[str, Counter[str]],
    metadata: dict[str, Any],
) -> None:
    for table, filename in TABLE_FILES.items():
        columns = list(headers[table])
        for row in transformed[table]:
            for key in row:
                if key not in columns:
                    columns.append(key)
        if table == "pig_batches" and "weight_change" not in columns:
            columns.append("weight_change")
        write_csv(folder / filename, transformed[table], columns)
    write_csv(folder / "feed_summary.csv", feed_summary,
              ["batch_id", "total_feed_kg", "pipeline_run_id", "window_start", "window_end"])
    write_csv(folder / "expense_summary.csv", expense_summary,
              ["batch_id", "total_expense", "pipeline_run_id", "window_start", "window_end"])
    write_csv(folder / "farm_expense_summary.csv", farm_expense_summary,
              ["expense_type", "record_count", "total_expense", "pipeline_run_id", "window_start", "window_end"])
    write_csv(folder / "quarantine.csv", quarantine,
              ["pipeline_run_id", "table_name", "source_row", "source_id", "rule_id", "field", "reason", "record_json"])
    quality_rows: list[dict[str, str]] = []
    for table, values in profiles.items():
        for metric, count in values.items():
            if isinstance(count, list):
                quality_rows.append({
                    "table_name": table,
                    "metric": f"profile:{metric}",
                    "count": str(len(count)),
                    "details": json.dumps(count, ensure_ascii=False),
                })
            else:
                quality_rows.append({"table_name": table, "metric": f"profile:{metric}", "count": str(count), "details": ""})
    for table, values in rule_counts.items():
        for rule, count in values.items():
            quality_rows.append({"table_name": table, "metric": "rule_count", "count": str(count), "details": rule})
    write_csv(folder / "data_quality_report.csv", quality_rows,
              ["table_name", "metric", "count", "details"])
    write_json(folder / "transformation_metadata.json", metadata)


def publish_directory(temp_dir: Path, final_dir: Path) -> None:
    """Atomically replace a prior run's output after all staged files are written."""
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


def make_temp_dir(parent: Path, run_id: str) -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=f".{run_id}.", dir=parent))
