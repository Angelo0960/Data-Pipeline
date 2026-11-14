"""Fail-closed PostgreSQL loading stage for approved transformation bundles.

The destination database/schema and idempotent write contract are project decisions
that are not yet present in repository documentation. This module refuses to
load until they are supplied rather than guessing a schema or upsert behavior.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REQUIRED_OUTPUTS = (
    "pig_batches_transformed.csv", "feed_records_transformed.csv",
    "vaccination_records_transformed.csv", "expenses_transformed.csv",
    "feed_summary.csv", "expense_summary.csv", "farm_expense_summary.csv",
)


class LoadNotConfiguredError(RuntimeError):
    pass


def inspect_approved_bundle(bundle_dir: Path) -> dict[str, Any]:
    try:
        metadata = json.loads((bundle_dir / "transformation_metadata.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LoadNotConfiguredError(f"Cannot read transformation metadata: {exc}") from exc
    if metadata.get("handover_decision") != "APPROVED" or metadata.get("transformation_status") != "PASS":
        raise LoadNotConfiguredError("Only PASS/APPROVED transformation bundles may be loaded")
    missing = [name for name in REQUIRED_OUTPUTS if not (bundle_dir / name).is_file()]
    if missing:
        raise LoadNotConfiguredError("Bundle is incomplete: " + ", ".join(missing))
    return metadata


def load_approved_bundle(bundle_dir: Path) -> dict[str, Any]:
    """Validate the input boundary; refuse until DB/schema/upsert decisions exist."""
    metadata = inspect_approved_bundle(bundle_dir)
    raise LoadNotConfiguredError(
        "PostgreSQL loading is intentionally disabled: approve a destination schema, "
        "connection configuration, and rerun-safe load strategy before enabling it "
        f"(transformation_run_id={metadata.get('transformation_run_id', 'unknown')})."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle_dir", type=Path, help="Validated transformation run folder")
    args = parser.parse_args()
    try:
        load_approved_bundle(args.bundle_dir)
    except LoadNotConfiguredError as exc:
        print(f"Load stage not run: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
