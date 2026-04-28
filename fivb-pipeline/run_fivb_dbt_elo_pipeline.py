#!/usr/bin/env python3
"""
FIVB transforms pipeline: Elo table init -> dbt -> Elo compute.

Intended for:
- standalone reruns when raw data is already loaded
- post-ingestion chaining from the VIS raw ingestion workflow
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from etl.pipeline_env import normalize_database_url_for_pipeline

ROOT = Path(__file__).resolve().parent


def _dbt_argv(*args: str) -> list[str]:
    """dbt-core 1.8+ has no python -m dbt; use the console script next to this Python."""
    w = shutil.which("dbt")
    if w:
        return [w, *args]
    bin_dir = Path(sys.executable).resolve().parent
    for name in ("dbt", "dbt.exe"):
        p = bin_dir / name
        if p.is_file():
            return [str(p), *args]
    raise SystemExit(
        "dbt CLI not found. After `pip install -r requirements-cron.txt`, dbt should sit "
        "next to python or on PATH."
    )


def main() -> None:
    normalize_database_url_for_pipeline()
    os.chdir(ROOT)
    os.environ["DBT_PROFILES_DIR"] = str(ROOT / ".dbt")

    print("Step 1/3: Elo tables init…")
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "elo_compute.py"), "--init-only"],
        cwd=ROOT,
        check=True,
    )

    print("Step 2/3: dbt run…")
    subprocess.run(_dbt_argv("deps"), cwd=ROOT, check=True)
    subprocess.run(_dbt_argv("run"), cwd=ROOT, check=True)

    print("Step 3/3: Elo compute…")
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "elo_compute.py")],
        cwd=ROOT,
        check=True,
    )
    print("FIVB dbt/Elo pipeline complete.")


if __name__ == "__main__":
    main()
