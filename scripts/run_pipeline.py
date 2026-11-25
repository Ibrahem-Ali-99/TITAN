"""Run a pipeline module from a clean checkout.

`python -m titan_warehouse` finds the module only when `PYTHONPATH` already
contains the pipeline source directories. This script adds those directories,
loads `.env` from the repository root, and then runs the module.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

from scripts.envfile import load_env_file

ROOT = Path(__file__).resolve().parents[1]
SOURCES = (
    ROOT / "pipelines" / "warehouse" / "src",
    ROOT / "pipelines" / "quality" / "src",
    ROOT / "pipelines" / "ml" / "src",
    ROOT / "pipelines" / "dagster" / "src",
)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: run_pipeline.py <module> [args...]")
    sys.path[:0] = [str(path) for path in SOURCES]
    load_env_file(ROOT / ".env")
    module = sys.argv[1]
    sys.argv = sys.argv[1:]
    runpy.run_module(module, run_name="__main__")


if __name__ == "__main__":
    main()
