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
              ["pipeline_run_id", "table_name", "source_row", "source_id", "batch_id", "rule_id", "field", "reason", "record_json"])
    quality_rows: list[dict[str, str]] = []
    rule_guidance = {
        "C-01": ("Quarantine; correct the source ID without generating a replacement.", "critical"),
        "C-02": ("Quarantine every conflicting copy; require manual source review.", "critical"),
        "C-03": ("Quarantine all batch-code collisions; confirm the authoritative source values.", "high"),
        "C-04": ("Quarantine missing required values; preserve permitted optional NULLs.", "high"),
        "C-05": ("Quarantine invalid numeric values; correct at source and do not silently round.", "high"),
        "C-07": ("Quarantine invalid required dates/times; do not infer ambiguous values.", "high"),
        "R-01": ("Quarantine missing/orphan required relationships; preserve valid farm-level NULL expenses.", "critical"),
    }
    for table, values in profiles.items():
        denominator = int(values.get("rows_read", 0))
        for metric, value in values.items():
            if isinstance(value, dict):
                is_duplicate_ids = metric == "duplicate_id_counts"
                field = "id" if is_duplicate_ids else metric.partition(":")[2]
                for observed_value, observed_count in value.items():
                    quality_rows.append({
                        "table_name": table, "column": field,
                        "issue_type": "duplicate_primary_key" if is_duplicate_ids else "observed_category",
                        "rule_id": "C-02" if is_duplicate_ids else "C-09",
                        "metric": "duplicate_primary_key" if is_duplicate_ids else "observed_category",
                        "count": str(observed_count),
                        "details": f"{field}={observed_value}",
                        "sample_values": json.dumps([observed_value], ensure_ascii=False),
                        "affected_percent": f"{(100.0 * int(observed_count) / denominator) if denominator else 0.0:.2f}",
                        "planned_treatment": (
                            "Retain one exact copy; quarantine all rows if values conflict for this ID."
                            if is_duplicate_ids else
                            "Compare with an approved vocabulary; do not guess, normalize, or discard categories."
                        ),
                        "severity": "critical" if is_duplicate_ids else "review",
                    })
                continue
            if isinstance(value, list):
                if metric.startswith("whitespace_samples:"):
                    continue
                count = len(value)
                details = json.dumps(value, ensure_ascii=False)
                samples = details
                treatment = "Compare observed values with an approved vocabulary; no category is guessed or remapped."
                severity = "review"
                percent_text = ""
            else:
                count = int(value)
                details = ""
                is_blank_metric = metric.startswith("blank_values:")
                is_whitespace_metric = metric.startswith("whitespace_values:")
                column = metric.partition(":")[2] if is_blank_metric or is_whitespace_metric else ""
                samples = json.dumps([""]) if is_blank_metric and count else "[]"
                if is_whitespace_metric:
                    samples = json.dumps(values.get(f"whitespace_samples:{column}", []), ensure_ascii=False)
                treatment = (
                    "Trim leading/trailing text whitespace and preserve meaningful internal text."
                    if is_whitespace_metric else
                    "Retain optional NULLs; required blanks are quarantined under the applicable cleaning rule."
                    if is_blank_metric else ""
                )
                severity = "review" if count else "info"
                percent_text = f"{(100.0 * count / denominator) if denominator else 0.0:.2f}" if is_blank_metric or is_whitespace_metric or metric in {"duplicate_id_rows", "exact_duplicate_rows"} else ""
            quality_rows.append({
                "table_name": table,
                "column": column if not isinstance(value, (list, dict)) else "",
                "issue_type": "whitespace" if metric.startswith("whitespace_values:") else "blank_values" if metric.startswith("blank_values:") else metric,
                "rule_id": "C-06" if metric.startswith("whitespace_values:") else "",
                "metric": f"profile:{metric}", "count": str(count),
                "details": details, "sample_values": samples,
                "affected_percent": percent_text, "planned_treatment": treatment, "severity": severity,
            })
    issue_groups: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for item in quarantine:
        key = (item.get("table_name", ""), item.get("rule_id", ""), item.get("field", ""))
        issue_groups.setdefault(key, []).append(item)
    for (table, rule_id, field), items in issue_groups.items():
        samples: list[str] = []
        for item in items:
            try:
                record = json.loads(item.get("record_json", "{}"))
            except json.JSONDecodeError:
                record = {}
            sample = str(record.get(field, ""))
            if sample not in samples:
                samples.append(sample)
            if len(samples) == 5:
                break
        denominator = int(profiles.get(table, {}).get("rows_read", 0))
        treatment, severity = rule_guidance.get(rule_id, ("Review the row-level quarantine reason and correct the source data.", "review"))
        quality_rows.append({
            "table_name": table, "column": field, "issue_type": "quarantined_rows", "rule_id": rule_id,
            "metric": "profiled_issue", "count": str(len(items)),
            "details": f"{rule_id}:{field}", "sample_values": json.dumps(samples, ensure_ascii=False),
            "affected_percent": f"{(100.0 * len(items) / denominator) if denominator else 0.0:.2f}",
            "planned_treatment": treatment, "severity": severity,
        })
    for table, values in rule_counts.items():
        for rule, count in values.items():
            quality_rows.append({
                "table_name": table, "column": "", "issue_type": "rule_count", "rule_id": rule,
                "metric": "rule_count", "count": str(count), "details": rule,
                "sample_values": "[]", "affected_percent": "", "planned_treatment": "", "severity": "info",
            })
    write_csv(folder / "data_quality_report.csv", quality_rows,
              ["table_name", "column", "issue_type", "rule_id", "metric", "count", "details", "sample_values", "affected_percent", "planned_treatment", "severity"])
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
