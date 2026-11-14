"""Compatibility CLI for the separate PostgreSQL load stage."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.load import (  # noqa: E402
    REQUIRED_OUTPUTS,
    LoadError,
    inspect_approved_bundle,
    load_approved_bundle,
    main,
    migrate_database,
)

LoadNotConfiguredError = LoadError


if __name__ == "__main__":
    raise SystemExit(main())
