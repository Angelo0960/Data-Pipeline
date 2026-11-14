"""Connect extraction metadata and approved handover to Transformation/Load."""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    from ..extraction.handover import REQUIRED_TABLES, decide_handover
except ImportError:
    from pipeline.extraction.handover import REQUIRED_TABLES, decide_handover


class OrchestrationError(RuntimeError):
    pass


def read_object(path: Path, label: str) -> dict[str, Any]:
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OrchestrationError(f"Cannot read {label}: {exc}") from exc
    if not isinstance(result, dict):
        raise OrchestrationError(f"{label} must be a JSON object")
    return result


def build_handover(input_dir: Path, extraction_metadata_path: Path) -> tuple[dict[str, Any], Path]:
    source = read_object(extraction_metadata_path, "extraction metadata")
    run_id = str(source.get("pipeline_run_id", "")).strip()
    tables = source.get("tables")
    if not run_id or not isinstance(tables, list):
        raise OrchestrationError("Extraction metadata must include pipeline_run_id and a tables list")
    extracted_counts = {
        str(item.get("table_name")): item.get("records_extracted")
        for item in tables if isinstance(item, dict) and item.get("table_name")
    }
    output_files: dict[str, str] = {}
    counts: dict[str, dict[str, int]] = {}
    for table in REQUIRED_TABLES:
        path = input_dir / f"{table}.csv"
        if not path.is_file():
            continue
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                import csv
                csv_count = max(sum(1 for _ in csv.reader(handle)) - 1, 0)
        except (OSError, csv.Error) as exc:
            raise OrchestrationError(f"Cannot inspect {table} handover CSV: {exc}") from exc
        count = extracted_counts.get(table, csv_count)
        if not isinstance(count, int) or count < 0:
            raise OrchestrationError(f"Extraction metadata has invalid record count for {table}")
        output_files[table] = str(path.resolve())
        counts[table] = {"api_records": count, "extracted_records": count, "csv_rows": csv_count}
    decision = decide_handover(
        pipeline_run_id=run_id,
        validation=source.get("validation") or {},
        output_files=output_files,
        record_counts=counts,
        extraction_status=source.get("extraction_status"),
        error_message=source.get("error_message") or None,
    )
    handover = {
        **source,
        **decision,
        "window_start": source.get("window_start"),
        "window_end": source.get("window_end"),
    }
    handover_dir = input_dir.parent.parent / "handover" / "transformation"
    handover_path = handover_dir / f"{run_id}.json"
    handover_dir.mkdir(parents=True, exist_ok=True)
    temporary = handover_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(handover, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(handover_path)
    return handover, handover_path


def run_from_extraction(input_dir: Path, metadata_path: Path, output_dir: Path, *, load: bool = False) -> int:
    handover, handover_path = build_handover(input_dir, metadata_path)
    if handover.get("handover_decision") != "APPROVED":
        raise OrchestrationError(
            f"Handover {handover.get('handover_decision')}; Transformation stopped. See {handover_path}"
        )
    project_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable, "-m", "pipeline.transformation.transform",
            "--input-dir", str(input_dir), "--metadata", str(handover_path),
            "--output-dir", str(output_dir),
        ],
        check=False,
        cwd=project_root,
    )
    if result.returncode != 0:
        return result.returncode
    if load:
        bundle = output_dir.resolve() / "transformed" / str(handover["pipeline_run_id"])
        load_script = Path(__file__).resolve().with_name("load.py")
        load_result = subprocess.run([sys.executable, str(load_script), str(bundle)], check=False)
        return load_result.returncode
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path, help="Extraction output folder")
    parser.add_argument("--metadata", required=True, type=Path, help="Extraction audit metadata JSON")
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument("--load", action="store_true", help="Attempt the separate load stage (currently fail-closed until configured)")
    args = parser.parse_args()
    try:
        return run_from_extraction(args.input_dir, args.metadata, args.output_dir, load=args.load)
    except (OrchestrationError, OSError, RuntimeError) as exc:
        print(f"Pipeline stopped: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
