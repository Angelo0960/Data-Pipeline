#!/usr/bin/env python3
"""Decide whether an extracted dataset may be handed to Transformation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

REQUIRED_TABLES = (
    "pig_batches",
    "feed_records",
    "vaccination_records",
    "expenses",
)


class HandoverInputError(ValueError):
    """Raised when handover input cannot be evaluated safely."""


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _counts_match(record_counts: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    for table in REQUIRED_TABLES:
        counts = record_counts.get(table)
        if not isinstance(counts, Mapping):
            continue
        values = {
            name: counts.get(name)
            for name in ("api_records", "extracted_records", "csv_rows")
            if counts.get(name) is not None
        }
        if len(values) >= 2 and len(set(values.values())) != 1:
            failures.append(
                f"{table}: API, extracted, and CSV record counts do not match"
            )
    return failures


def _table_failures(validation: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    for table, result in (validation.get("tables") or {}).items():
        if not isinstance(result, Mapping):
            continue
        table_result = result.get("validation_result")
        if table_result == "FAIL":
            failures.extend(str(item) for item in _as_list(result.get("failures")))
            if not result.get("failures"):
                failures.append(f"{table}: table validation failed")
    return failures


def decide_handover(
    *,
    pipeline_run_id: str,
    validation: Mapping[str, Any],
    output_files: Mapping[str, str] | None = None,
    record_counts: Mapping[str, Any] | None = None,
    extraction_status: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Return a deterministic handover decision from validation metadata."""
    if not pipeline_run_id.strip():
        raise HandoverInputError("pipeline_run_id is required")
    if not isinstance(validation, Mapping):
        raise HandoverInputError("validation must be an object")

    output_files = dict(output_files or {})
    record_counts = dict(record_counts or {})
    failures = [str(item) for item in _as_list(validation.get("failures"))]
    warnings = [str(item) for item in _as_list(validation.get("warnings"))]
    failures.extend(_table_failures(validation))
    failures.extend(_counts_match(record_counts))

    missing_outputs = sorted(set(REQUIRED_TABLES) - set(output_files))
    if missing_outputs:
        failures.append(
            "missing handover output file(s): " + ", ".join(missing_outputs)
        )

    validation_result = str(validation.get("validation_result") or validation.get("status") or "").upper()
    if validation_result == "FAIL":
        failures.append("validation result is FAIL")
    elif validation_result == "WARNING":
        warnings.append("validation result is WARNING")
    elif validation_result not in {"PASS", ""}:
        failures.append(f"unsupported validation result: {validation_result}")

    if error_message:
        failures.append(str(error_message))
    if str(extraction_status or "").upper() in {"FAILED", "FAIL"}:
        failures.append("extraction status is FAILED")

    # Preserve order while removing duplicate messages.
    failures = list(dict.fromkeys(failures))
    warnings = list(dict.fromkeys(warnings))

    if failures:
        decision = "REJECTED"
        status = "FAILED"
        result = "FAIL"
        reason = "One or more critical extraction or handover requirements failed."
    elif warnings:
        decision = "MANUAL REVIEW"
        status = "WARNING"
        result = "WARNING"
        reason = "The extraction has non-critical warnings and is held for review."
    else:
        decision = "APPROVED"
        status = "SUCCESS"
        result = "PASS"
        reason = "All critical extraction and handover requirements passed."

    return {
        "pipeline_run_id": pipeline_run_id,
        "handover_decision": decision,
        "extraction_status": status,
        "validation_result": result,
        "transformation_allowed": decision == "APPROVED",
        "watermark_update_allowed": decision == "APPROVED",
        "output_files": output_files,
        "record_counts": record_counts,
        "warnings": warnings,
        "failures": failures,
        "error_message": error_message or "",
        "decision_reason": reason,
    }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HandoverInputError(f"Could not read JSON input {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise HandoverInputError("Handover input must be a JSON object")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="JSON file containing validation and output metadata")
    parser.add_argument("--output", type=Path, help="Optional JSON output path")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        payload = _load_json(args.input)
        result = decide_handover(
            pipeline_run_id=str(payload.get("pipeline_run_id") or ""),
            validation=payload.get("validation") or {},
            output_files=payload.get("output_files"),
            record_counts=payload.get("record_counts"),
            extraction_status=payload.get("extraction_status"),
            error_message=payload.get("error_message"),
        )
    except HandoverInputError as exc:
        print(f"Handover input failed: {exc}", file=sys.stderr)
        return 2

    serialized = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized, encoding="utf-8")
    else:
        print(serialized, end="")
    return 0 if result["handover_decision"] == "APPROVED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
