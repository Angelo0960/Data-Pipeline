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

try:
    from .handover import HandoverInputError, decide_handover
    from .validation import ValidationInputError, validate_extraction
except ImportError:  # Support running this file directly from pipeline/extraction.
    from handover import HandoverInputError, decide_handover
    from validation import ValidationInputError, validate_extraction

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
PROJECT_ROOT = Path(__file__).resolve().parents[2]
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
        findings += [["INFO", note] for note in validation.get("information", [])] if validation else []
        if not findings and not metadata["error_message"]:
            findings = [["PASS", "No validation findings"]]
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
    script_path = Path(__file__).resolve()
    candidates = [
        Path.cwd() / ".env",
        PROJECT_ROOT / ".env",
        script_path.parent.parent / ".env",
        script_path.parent / ".env",
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


def validate_records(records_by_table: dict[str, list[dict[str, Any]]], **kwargs: Any) -> dict[str, Any]:
    """Compatibility wrapper around the standalone extraction validator."""
    return validate_extraction(records_by_table, **kwargs)


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


def count_csv_rows(path: Path) -> int:
    """Count the records actually written in a CSV handover file."""
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ExtractionError(f"CSV output has no header: {path}")
        return sum(1 for _ in reader)


def record_count_payload(
    api_counts: dict[str, int],
    extracted_counts: dict[str, int],
    csv_counts: dict[str, int] | None = None,
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for table in TABLES:
        item = {
            "api_records": api_counts[table],
            "extracted_records": extracted_counts[table],
        }
        if csv_counts is not None:
            item["csv_rows"] = csv_counts[table]
        result[table] = item
    return result


def update_table_validation(metadata: dict[str, Any], validation: dict[str, Any]) -> None:
    for table_log in metadata["tables"]:
        result = validation.get("tables", {}).get(table_log["table_name"], {})
        table_log["validation_result"] = result.get("validation_result", "FAIL")
        if result.get("csv_rows_written") is not None:
            table_log["csv_rows_written"] = result["csv_rows_written"]


def apply_handover(
    metadata: dict[str, Any],
    validation: dict[str, Any],
    output_files: dict[str, str],
    record_counts: dict[str, dict[str, int]],
) -> dict[str, Any]:
    try:
        decision = decide_handover(
            pipeline_run_id=str(metadata["pipeline_run_id"]),
            validation=validation,
            output_files=output_files,
            record_counts=record_counts,
            extraction_status=validation.get("extraction_status"),
            error_message=metadata.get("error_message") or None,
        )
    except HandoverInputError as exc:
        raise ExtractionError(f"Could not decide extraction handover: {exc}") from exc
    metadata["validation"] = validation
    metadata.update(decision)
    return decision


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

        api_counts = {table: len(table_results[table]) for table in TABLES}
        validation_args = {
            "window_start": metadata["window_start"],
            "window_end": metadata["window_end"],
            "api_counts": api_counts,
            "extracted_counts": api_counts,
            "declared_columns": {table: list(columns) for table, columns in TABLES.items()},
        }
        validation = validate_records(table_results, **validation_args)
        metadata["validation"] = validation
        metadata["validation_result"] = validation["validation_result"]
        metadata["extraction_status"] = validation["extraction_status"]
        update_table_validation(metadata, validation)

        if validation["validation_result"] == "FAIL":
            decision = apply_handover(
                metadata,
                validation,
                {},
                record_count_payload(api_counts, api_counts),
            )
            for failure in decision["failures"]:
                logger.error("Validation/handover failure: %s", failure)
            raise ExtractionError("Handover rejected: " + "; ".join(decision["failures"]))

        logger.info("Validation completed | result=%s", validation["validation_result"])
        for warning in validation["warnings"]:
            logger.warning("Validation warning: %s", warning)
        for note in validation.get("information", []):
            logger.info("Validation information: %s", note)

        run_raw_dir = output_dir / "raw" / run_id
        output_files: dict[str, str] = {}
        for table, records in table_results.items():
            csv_path = run_raw_dir / f"{table}.csv"
            write_csv(csv_path, records, TABLES[table])
            output_files[table] = str(csv_path.resolve())
            logger.info("Wrote %d row(s) to %s", len(records), csv_path)

        csv_counts = {
            table: count_csv_rows(run_raw_dir / f"{table}.csv")
            for table in TABLES
        }
        validation = validate_records(
            table_results,
            **validation_args,
            csv_counts=csv_counts,
        )
        metadata["validation"] = validation
        metadata["validation_result"] = validation["validation_result"]
        metadata["extraction_status"] = validation["extraction_status"]
        update_table_validation(metadata, validation)
        decision = apply_handover(
            metadata,
            validation,
            output_files,
            record_count_payload(api_counts, api_counts, csv_counts),
        )
        for failure in decision["failures"]:
            logger.error("Handover failure: %s", failure)
        for warning in decision["warnings"]:
            logger.warning("Handover warning: %s", warning)
        logger.info(
            "Handover completed | decision=%s",
            decision["handover_decision"],
        )
        if decision["handover_decision"] == "REJECTED":
            raise ExtractionError("Handover rejected: " + "; ".join(decision["failures"]))

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
