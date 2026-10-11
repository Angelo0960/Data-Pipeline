#!/usr/bin/env python3
"""Extract PrepAPig farming records from Supabase into raw CSV files.

The script uses only Python's standard library. Configure the source connection
with SUPABASE_URL and SUPABASE_ANON_KEY environment variables before running.

Example:
    python extraction.py
    python extraction.py --full
    python extraction.py --output-dir data
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

TABLES = {
    "pig_batches": [
        "id",
        "batch_code",
        "pig_count",
        "breed",
        "start_weight",
        "current_weight",
        "date_acquired",
        "status",
        "created_at",
    ],
    "feed_records": [
        "id",
        "batch_id",
        "feed_type",
        "quantity_kg",
        "feeding_date",
        "feeding_time",
        "notes",
        "created_at",
    ],
    "vaccination_records": [
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
    ],
    "expenses": [
        "id",
        "batch_id",
        "expense_type",
        "amount",
        "expense_date",
        "description",
        "created_at",
    ],
}

DEFAULT_OUTPUT_DIR = Path("data")
DEFAULT_PAGE_SIZE = 1000
DEFAULT_TIMEOUT_SECONDS = 30


class ExtractionError(RuntimeError):
    """Raised when extraction cannot safely complete."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ExtractionError(f"Invalid timestamp in watermark: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def require_environment() -> tuple[str, str]:
    base_url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
    api_key = os.getenv("SUPABASE_ANON_KEY", "").strip()
    missing = []
    if not base_url:
        missing.append("SUPABASE_URL")
    if not api_key:
        missing.append("SUPABASE_ANON_KEY")
    if missing:
        raise ExtractionError(
            "Missing required environment variable(s): " + ", ".join(missing)
        )
    return base_url, api_key


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExtractionError(f"Could not read JSON file {path}: {exc}") from exc


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def build_query(table: str, window_start: datetime | None, window_end: datetime) -> str:
    params: list[tuple[str, str]] = [("select", "*")]
    if window_start is not None:
        params.append(("created_at", f"gt.{iso_timestamp(window_start)}"))
    params.append(("created_at", f"lte.{iso_timestamp(window_end)}"))
    params.append(("order", "created_at.asc"))
    return urlencode(params)


def request_json(
    url: str,
    api_key: str,
    timeout: int,
    range_start: int,
    range_end: int,
) -> list[dict[str, Any]]:
    request = Request(
        url,
        headers={
            "apikey": api_key,
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Range": f"{range_start}-{range_end}",
            "Prefer": "count=exact",
            "User-Agent": "PrepAPig-Data-Pipeline/1.0",
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise ExtractionError(f"Supabase HTTP {exc.code}: {body}") from exc
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise ExtractionError(f"Supabase request failed: {exc}") from exc

    if not isinstance(payload, list):
        raise ExtractionError("Supabase returned an unexpected response shape")
    if not all(isinstance(row, dict) for row in payload):
        raise ExtractionError("Supabase returned a non-object record")
    return payload


def extract_table(
    base_url: str,
    api_key: str,
    table: str,
    window_start: datetime | None,
    window_end: datetime,
    timeout: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    offset = 0
    while True:
        query = build_query(table, window_start, window_end)
        url = f"{base_url}/rest/v1/{table}?{query}"
        page = request_json(
            url,
            api_key,
            timeout,
            range_start=offset,
            range_end=offset + DEFAULT_PAGE_SIZE - 1,
        )
        records.extend(page)
        if len(page) < DEFAULT_PAGE_SIZE:
            break
        offset += len(page)
    return records


def validate_records(records_by_table: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    results: dict[str, Any] = {}
    failures: list[str] = []
    batch_ids = {row.get("id") for row in records_by_table["pig_batches"] if row.get("id")}

    for table, records in records_by_table.items():
        required = set(TABLES[table])
        missing_columns = sorted(required - set().union(*(row.keys() for row in records))) if records else []
        ids = [row.get("id") for row in records]
        duplicate_ids = sorted({record_id for record_id in ids if ids.count(record_id) > 1 and record_id})
        missing_ids = sum(1 for record_id in ids if not record_id)
        invalid_batch_ids = 0
        if table != "pig_batches":
            invalid_batch_ids = sum(
                1 for row in records if not row.get("batch_id") or row.get("batch_id") not in batch_ids
            )

        table_result = {
            "required_columns_present": not missing_columns,
            "missing_columns": missing_columns,
            "ids_present": missing_ids == 0,
            "duplicate_ids": duplicate_ids,
            "batch_relationships_valid": invalid_batch_ids == 0,
            "invalid_batch_relationships": invalid_batch_ids,
            "records": len(records),
        }
        results[table] = table_result
        if missing_columns:
            failures.append(f"{table}: missing columns {', '.join(missing_columns)}")
        if missing_ids:
            failures.append(f"{table}: {missing_ids} record(s) missing id")
        if duplicate_ids:
            failures.append(f"{table}: duplicate id values detected")
        if invalid_batch_ids:
            failures.append(f"{table}: invalid batch_id relationship(s)")

    batch_codes = [row.get("batch_code") for row in records_by_table["pig_batches"]]
    duplicate_codes = sorted({code for code in batch_codes if code and batch_codes.count(code) > 1})
    results["batch_code_uniqueness"] = {
        "passed": not duplicate_codes,
        "duplicates": duplicate_codes,
    }
    if duplicate_codes:
        failures.append("pig_batches: duplicate batch_code values detected")

    return {
        "status": "PASS" if not failures else "FAIL",
        "passed": not failures,
        "failures": failures,
        "tables": results,
    }


def write_csv(path: Path, records: list[dict[str, Any]], expected_columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    extra_columns = sorted({key for row in records for key in row if key not in expected_columns})
    fieldnames = expected_columns + extra_columns
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def load_watermark(path: Path) -> datetime | None:
    value = read_json(path, {}).get("last_successful_extraction_timestamp")
    return parse_timestamp(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", action="store_true", help="Ignore the saved watermark and extract all available records.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for raw data and metadata.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="Supabase request timeout in seconds.")
    return parser


def run(args: argparse.Namespace) -> int:
    started_at = utc_now()
    window_end = started_at
    run_id = f"PREPAPIG_{started_at.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    output_dir: Path = args.output_dir
    watermark_path = output_dir / "metadata" / "watermark.json"
    window_start: datetime | None = None
    table_results: dict[str, list[dict[str, Any]]] = {}
    metadata: dict[str, Any] = {
        "pipeline_run_id": run_id,
        "extraction_start": iso_timestamp(started_at),
        "extraction_end": None,
        "extraction_status": "FAILED",
        "window_start": iso_timestamp(window_start) if window_start else None,
        "window_end": iso_timestamp(window_end),
        "tables": [],
        "validation": None,
        "error_message": "",
    }

    try:
        base_url, api_key = require_environment()
        window_start = None if args.full else load_watermark(watermark_path)
        metadata["window_start"] = iso_timestamp(window_start) if window_start else None
        for table in TABLES:
            table_started = time.perf_counter()
            records = extract_table(base_url, api_key, table, window_start, window_end, args.timeout)
            table_results[table] = records
            metadata["tables"].append({
                "table_name": table,
                "records_read": len(records),
                "records_extracted": len(records),
                "records_rejected": 0,
                "extraction_duration_seconds": round(time.perf_counter() - table_started, 3),
            })

        validation = validate_records(table_results)
        metadata["validation"] = validation
        for table_log in metadata["tables"]:
            table_log["validation_result"] = (
                "PASS" if validation["tables"].get(table_log["table_name"], {}).get("required_columns_present", False)
                and validation["tables"].get(table_log["table_name"], {}).get("ids_present", False)
                and not validation["tables"].get(table_log["table_name"], {}).get("duplicate_ids")
                and validation["tables"].get(table_log["table_name"], {}).get("batch_relationships_valid", True)
                else "FAIL"
            )
        if not validation["passed"]:
            raise ExtractionError("Validation failed: " + "; ".join(validation["failures"]))

        run_raw_dir = output_dir / "raw" / run_id
        for table, records in table_results.items():
            write_csv(run_raw_dir / f"{table}.csv", records, TABLES[table])

        metadata["extraction_status"] = "SUCCESS"
        metadata["extraction_end"] = iso_timestamp(utc_now())
        metadata["extraction_duration_seconds"] = round(
            (utc_now() - started_at).total_seconds(), 3
        )
        metadata_path = output_dir / "metadata" / f"{run_id}.json"
        write_json(metadata_path, metadata)
        write_json(
            watermark_path,
            {
                "last_successful_extraction_timestamp": metadata["window_end"],
                "pipeline_run_id": run_id,
            },
        )
        print(f"Extraction succeeded: {run_id}")
        for table in TABLES:
            print(f"  {table}: {len(table_results[table])} record(s)")
        print(f"  Raw output: {run_raw_dir}")
        print(f"  Metadata: {metadata_path}")
        return 0
    except ExtractionError as exc:
        metadata["error_message"] = str(exc)
        metadata["extraction_end"] = iso_timestamp(utc_now())
        metadata["extraction_duration_seconds"] = round(
            (utc_now() - started_at).total_seconds(), 3
        )
        write_json(output_dir / "metadata" / f"{run_id}.json", metadata)
        print(f"Extraction failed: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        metadata["error_message"] = f"File-system error: {exc}"
        metadata["extraction_end"] = iso_timestamp(utc_now())
        metadata["extraction_duration_seconds"] = round(
            (utc_now() - started_at).total_seconds(), 3
        )
        write_json(output_dir / "metadata" / f"{run_id}.json", metadata)
        print(f"Extraction failed: {exc}", file=sys.stderr)
        return 1


def main() -> int:
    parser = build_parser()
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
