"""Transformation run logs, audit metadata, and safe error summaries."""
from __future__ import annotations

import csv
import json
import os
import re
from pathlib import Path
from typing import Any

LOG_COLUMNS = (
    "transformation_run_id", "pipeline_run_id", "transformation_status",
    "handover_decision", "transformation_start", "transformation_end",
    "duration_seconds", "input_records", "accepted_records",
    "quarantined_records", "duplicate_copies_removed", "output_location", "message",
    "window_start", "window_end", "counts", "summary_rows", "rules_applied",
    "validation_results", "warnings",
)


def safe_message(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"(?i)(password|api[_-]?key|token|secret)\s*[:=]\s*[^\s,;]+", r"\1=[REDACTED]", text)
    text = re.sub(r"(://[^:/\s]+:)[^@/\s]+@", r"\1[REDACTED]@", text)
    return text[:2000]


def _csv_values(record: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    structured = {"counts", "summary_rows", "rules_applied", "validation_results", "warnings"}
    for key in LOG_COLUMNS:
        value = record.get(key, "")
        if key == "message":
            value = safe_message(value)
        elif key in structured and isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True)
        values[key] = value
    return values


def write_package_log(folder: Path, record: dict[str, Any]) -> None:
    path = folder / "transformation_log.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LOG_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerow(_csv_values(record))
        handle.flush()
        os.fsync(handle.fileno())


def _ensure_log_schema(path: Path) -> None:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames == list(LOG_COLUMNS):
            return
        rows = list(reader)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=LOG_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(_csv_values(row) for row in rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def append_operational_log(output_root: Path, record: dict[str, Any]) -> None:
    path = output_root / "logs" / "transformation" / "operational" / "transformation_log.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_log_schema(path)
    existed = path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LOG_COLUMNS, extrasaction="ignore")
        if not existed:
            writer.writeheader()
        writer.writerow(_csv_values(record))
        handle.flush()
        os.fsync(handle.fileno())


def write_audit_record(output_root: Path, record: dict[str, Any]) -> Path:
    path = output_root / "logs" / "transformation" / "audit" / f"{record['transformation_run_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    safe_record = dict(record)
    for field in ("message", "error_message"):
        if field in safe_record:
            safe_record[field] = safe_message(safe_record[field])
    temporary.write_text(json.dumps(safe_record, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    os.replace(temporary, path)
    return path


def write_central_records(output_root: Path, record: dict[str, Any]) -> None:
    safe_record = dict(record)
    safe_record["message"] = safe_message(safe_record.get("message", ""))
    append_operational_log(output_root, safe_record)
    write_audit_record(output_root, safe_record)
