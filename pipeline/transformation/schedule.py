"""Daily-scheduler entrypoint and schedule configuration validation.

The project has not yet approved a scheduler runtime or run time. This module
provides a one-shot command intended for an external Cron trigger; it does not
start a hidden loop or invent a schedule.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    from .orchestrate import run_from_extraction
except ImportError:
    from pipeline.transformation.orchestrate import run_from_extraction


@dataclass(frozen=True)
class ScheduleSettings:
    daily_time: str
    timezone: str


def schedule_settings() -> ScheduleSettings:
    daily_time = os.getenv("PREPAPIG_DAILY_TIME", "").strip()
    timezone_name = os.getenv("PREPAPIG_TIMEZONE", "").strip()
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", daily_time):
        raise ValueError("Set PREPAPIG_DAILY_TIME as HH:MM after the project approves the run time")
    if not timezone_name:
        raise ValueError("Set PREPAPIG_TIMEZONE after the project approves the schedule timezone")
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("PREPAPIG_TIMEZONE is not a recognized IANA timezone") from exc
    return ScheduleSettings(daily_time=daily_time, timezone=timezone_name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path, help="Extraction output folder")
    parser.add_argument("--metadata", required=True, type=Path, help="Extraction audit metadata JSON")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parents[2] / "pipeline" / "data")
    parser.add_argument("--validate-config-only", action="store_true", help="Validate approved schedule settings without starting stages")
    args = parser.parse_args()
    try:
        settings = schedule_settings()
        if args.validate_config_only:
            print(f"Schedule configuration valid: daily={settings.daily_time}, timezone={settings.timezone}")
            return 0
        result = run_from_extraction(args.input_dir, args.metadata, args.output_dir)
        return int(result)
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"Scheduled pipeline run stopped: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
