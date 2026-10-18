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
import logging
import os
import shlex
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
LOG_FILE_NAME = "extraction.log"
LOG_TITLE = "# PrepAPig Extraction Log"
LOG_FORMAT = "%(asctime)s %(levelname)-7s [%(run_id)s] %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

logger = logging.getLogger("extraction")


class RunIdFilter(logging.Filter):
    """Attach the active pipeline_run_id to every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = getattr(logger, "run_id", "-")
        return True


def markdown_cell(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    return text.replace("|", "\\|") or "-"


def markdown_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(markdown_cell(value) for value in row) + " |" for row in rows)
    return lines


class MarkdownRunLog:
    """Append one run's complete record to the single extraction log as markdown tables."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def write_run(
        self, metadata: dict[str, Any], mode: str, raw_output_dir: Path | None
    ) -> None:
        run_id = metadata["pipeline_run_id"]
        lines: list[str] = ["", f"## RUN {run_id}", "", "### Run Information", ""]
        lines += markdown_table(
            ["Field", "Value"],
            [
                ["pipeline_run_id", run_id],
                ["mode", mode],
                ["extraction_start", metadata["extraction_start"]],
                ["extraction_end", metadata["extraction_end"]],
                ["extraction_duration_seconds", metadata["extraction_duration_seconds"]],
                ["extraction_status", metadata["extraction_status"]],
                ["validation_result", metadata["validation_result"]],
                ["handover_decision", metadata["handover_decision"]],
                ["window_start", metadata["window_start"]],
                ["window_end", metadata["window_end"]],
                ["raw_output", raw_output_dir],
                ["error_message", metadata["error_message"]],
            ],
        )

        lines += ["", "### Table Extraction", ""]
        lines += markdown_table(
            [
                "table_name",
                "extraction_status",
                "records_read",
                "records_extracted",
                "records_rejected",
                "extraction_start",
                "extraction_end",
                "duration_seconds",
                "validation_result",
            ],
            [
                [
                    table_log["table_name"],
                    table_log["extraction_status"],
                    table_log["records_read"],
                    table_log["records_extracted"],
                    table_log["records_rejected"],
                    table_log["extraction_start"],
                    table_log["extraction_end"],
                    table_log["extraction_duration_seconds"],
                    table_log["validation_result"],
                ]
                for table_log in metadata["tables"]
            ],
        )

        lines += ["", "### Validation Findings", ""]
        validation = metadata["validation"]
        findings = [["FAIL", failure] for failure in validation["failures"]] if validation else []
        findings += [["WARNING", warning] for warning in validation["warnings"]] if validation else []
        if not findings and not metadata["error_message"]:
            findings = [["PASS", "No validation failures or warnings"]]
        elif not findings:
            findings = [
                ["FAIL", f"Extraction stopped before validation completed: {metadata['error_message']}"]
            ]
        lines += markdown_table(["Level", "Finding"], findings)
        lines += ["", "---", ""]

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            is_new = not self.path.exists()
            with self.path.open("a", encoding="utf-8") as handle:
                if is_new:
                    handle.write(f"{LOG_TITLE}\n")
                handle.write("\n".join(lines) + "\n")
        except OSError as exc:
            print(f"Could not write extraction log {self.path}: {exc}", file=sys.stderr)


def configure_logging(output_dir: Path, run_id: str, quiet: bool = False) -> Path:
    """Stream run progress lines to the console and return the single log file path."""
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers and not quiet:
        formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
        formatter.converter = time.gmtime
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.addFilter(RunIdFilter())
        logger.addHandler(console_handler)
    logger.run_id = run_id
    return output_dir / "logs" / "extraction" / "operational" / LOG_FILE_NAME


def write_audit_record(output_dir: Path, metadata: dict[str, Any]) -> None:
    """Persist the complete run metadata in restricted audit storage."""
    write_json(
        output_dir / "logs" / "extraction" / "audit" / f"{metadata['pipeline_run_id']}.json",
        metadata,
    )


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


def load_dotenv_file() -> None:
    """Load a local .env file without overwriting existing environment values."""
    candidates = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parent.parent / ".env",
        Path(__file__).resolve().parent / ".env",
    ]
    env_path = next((candidate for candidate in candidates if candidate.is_file()), None)
    if env_path is None:
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        try:
            parsed = shlex.split(raw_value.strip(), comments=True)
            os.environ[key] = parsed[0] if parsed else ""
        except ValueError as exc:
            raise ExtractionError(f"Invalid .env value for {key}: {exc}") from exc


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
        null_batch_ids = 0
        if table != "pig_batches":
            null_batch_ids = sum(1 for row in records if not row.get("batch_id"))
            invalid_batch_ids = sum(
                1
                for row in records
                if (not row.get("batch_id") and table != "expenses")
                or (row.get("batch_id") and row.get("batch_id") not in batch_ids)
            )

        table_result = {
            "required_columns_present": not missing_columns,
            "missing_columns": missing_columns,
            "ids_present": missing_ids == 0,
            "duplicate_ids": duplicate_ids,
            "batch_relationships_valid": invalid_batch_ids == 0,
            "invalid_batch_relationships": invalid_batch_ids,
            "null_batch_ids": null_batch_ids,
            "null_batch_id_policy": "allowed for farm-level expenses" if table == "expenses" else "not applicable",
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

    warnings = [
        f"expenses: {results['expenses']['null_batch_ids']} farm-level record(s) have no batch_id"
    ] if results["expenses"]["null_batch_ids"] else []

    return {
        "status": "FAIL" if failures else "WARNING" if warnings else "PASS",
        "passed": not failures,
        "failures": failures,
        "warnings": warnings,
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
    parser.add_argument("--quiet", action="store_true", help="Suppress console output. Run tables are still written to the log file.")
    return parser


def record_failure(
    run_log: MarkdownRunLog,
    output_dir: Path,
    metadata: dict[str, Any],
    started_at: datetime,
    mode: str,
    message: str,
) -> None:
    """Write the failed run's complete record to the extraction log."""
    metadata["error_message"] = message
    metadata["extraction_end"] = iso_timestamp(utc_now())
    metadata["extraction_duration_seconds"] = round(
        (utc_now() - started_at).total_seconds(), 3
    )
    logger.error("Extraction failed: %s", message)
    run_log.write_run(metadata, mode, None)
    write_audit_record(output_dir, metadata)


def run(args: argparse.Namespace) -> int:
    started_at = utc_now()
    window_end = started_at
    run_id = f"PREPAPIG_{started_at.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    output_dir: Path = args.output_dir
    log_path = configure_logging(output_dir, run_id, quiet=args.quiet)
    run_log = MarkdownRunLog(log_path)
    mode = "full" if args.full else "incremental"
    watermark_path = output_dir / "metadata" / "watermark.json"
    window_start: datetime | None = None
    table_results: dict[str, list[dict[str, Any]]] = {}
    metadata: dict[str, Any] = {
        "pipeline_run_id": run_id,
        "extraction_start": iso_timestamp(started_at),
        "extraction_end": None,
        "extraction_status": "FAILED",
        "validation_result": "FAIL",
        "handover_decision": "REJECTED",
        "window_start": iso_timestamp(window_start) if window_start else None,
        "window_end": iso_timestamp(window_end),
        "tables": [],
        "validation": None,
        "error_message": "",
    }

    logger.info(
        "Extraction started | mode=%s | window_end=%s | output_dir=%s",
        mode,
        metadata["window_end"],
        output_dir,
    )
    logger.info("Logging run tables to %s", log_path)

    try:
        base_url, api_key = require_environment()
        logger.info("Supabase source configured at %s", base_url)
        window_start = None if args.full else load_watermark(watermark_path)
        metadata["window_start"] = iso_timestamp(window_start) if window_start else None
        logger.info("Extraction window start=%s end=%s", metadata["window_start"], metadata["window_end"])
        for table in TABLES:
            table_started_at = utc_now()
            table_started = time.perf_counter()
            logger.info("Extracting table %s", table)
            table_log: dict[str, Any] = {
                "table_name": table,
                "extraction_start": iso_timestamp(table_started_at),
                "extraction_end": None,
                "records_read": None,
                "records_extracted": None,
                "records_rejected": 0,
                "extraction_duration_seconds": None,
                "extraction_status": "IN_PROGRESS",
                "validation_result": "NOT_EVALUATED",
            }
            metadata["tables"].append(table_log)
            try:
                records = extract_table(
                    base_url, api_key, table, window_start, window_end, args.timeout
                )
            except Exception:
                table_log["extraction_end"] = iso_timestamp(utc_now())
                table_log["extraction_duration_seconds"] = round(
                    time.perf_counter() - table_started, 3
                )
                table_log["extraction_status"] = "FAILED"
                table_log["validation_result"] = "FAIL"
                logger.error("Extraction failed for table %s", table)
                raise
            table_results[table] = records
            table_finished_at = utc_now()
            table_log["extraction_end"] = iso_timestamp(table_finished_at)
            table_log["records_read"] = len(records)
            table_log["records_extracted"] = len(records)
            table_log["extraction_duration_seconds"] = round(
                time.perf_counter() - table_started, 3
            )
            table_log["extraction_status"] = "SUCCESS"
            logger.info(
                "Extracted table %s | records_read=%d | duration_seconds=%.3f",
                table,
                len(records),
                table_log["extraction_duration_seconds"],
            )

        validation = validate_records(table_results)
        metadata["validation"] = validation
        metadata["validation_result"] = validation["status"]
        metadata["extraction_status"] = (
            "FAILED" if validation["failures"]
            else "WARNING" if validation["warnings"]
            else "SUCCESS"
        )
        metadata["handover_decision"] = (
            "REJECTED" if validation["failures"]
            else "MANUAL REVIEW" if validation["warnings"]
            else "APPROVED"
        )
        for failure in validation["failures"]:
            logger.error("Validation failure: %s", failure)
        for warning in validation["warnings"]:
            logger.warning("Validation warning: %s", warning)
        logger.info("Validation completed | result=%s", validation["status"])
        for table_log in metadata["tables"]:
            table_log["validation_result"] = (
                "PASS" if validation["tables"].get(table_log["table_name"], {}).get("required_columns_present", False)
                and validation["tables"].get(table_log["table_name"], {}).get("ids_present", False)
                and not validation["tables"].get(table_log["table_name"], {}).get("duplicate_ids")
                and validation["tables"].get(table_log["table_name"], {}).get("batch_relationships_valid", True)
                else "FAIL"
            )
            if (
                table_log["validation_result"] == "PASS"
                and validation["tables"].get(table_log["table_name"], {}).get("null_batch_ids", 0)
            ):
                table_log["validation_result"] = "WARNING"
            logger.info(
                "Table %s validation_result=%s",
                table_log["table_name"],
                table_log["validation_result"],
            )
        if not validation["passed"]:
            raise ExtractionError("Validation failed: " + "; ".join(validation["failures"]))

        run_raw_dir = output_dir / "raw" / run_id
        for table, records in table_results.items():
            write_csv(run_raw_dir / f"{table}.csv", records, TABLES[table])
            logger.info(
                "Wrote %d row(s) to %s", len(records), run_raw_dir / f"{table}.csv"
            )

        metadata["extraction_end"] = iso_timestamp(utc_now())
        metadata["extraction_duration_seconds"] = round(
            (utc_now() - started_at).total_seconds(), 3
        )
        if metadata["handover_decision"] == "APPROVED":
            write_json(
                watermark_path,
                {
                    "last_successful_extraction_timestamp": metadata["window_end"],
                    "pipeline_run_id": run_id,
                },
            )
            logger.info("Watermark advanced to %s", metadata["window_end"])
        else:
            logger.warning(
                "Watermark not advanced; handover decision=%s",
                metadata["handover_decision"],
            )
        logger.info(
            "Extraction finished | status=%s | duration_seconds=%.3f",
            metadata["extraction_status"],
            metadata["extraction_duration_seconds"],
        )
        run_log.write_run(metadata, mode, run_raw_dir)
        write_audit_record(output_dir, metadata)
        return 0
    except ExtractionError as exc:
        record_failure(run_log, output_dir, metadata, started_at, mode, str(exc))
        return 1
    except OSError as exc:
        record_failure(run_log, output_dir, metadata, started_at, mode, f"File-system error: {exc}")
        return 1
    except Exception as exc:
        record_failure(run_log, output_dir, metadata, started_at, mode, f"Unexpected error: {exc!r}")
        return 1


def main() -> int:
    load_dotenv_file()
    parser = build_parser()
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
