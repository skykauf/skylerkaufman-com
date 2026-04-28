#!/usr/bin/env python3
"""
FIVB VIS raw ingestion only.

Intended for GitHub Actions and manual runs when you want fresh raw VIS data
without immediately running dbt/Elo.
"""
from __future__ import annotations

import os
from pathlib import Path

from etl.pipeline_env import normalize_database_url_for_pipeline

ROOT = Path(__file__).resolve().parent


def main() -> None:
    normalize_database_url_for_pipeline()
    os.chdir(ROOT)

    # Import after DATABASE_URL normalization so DB consumers use the final URL.
    from etl.load_raw import IngestionLimits, run_full_ingestion

    print("Step 1/1: VIS raw ingestion…")
    run_full_ingestion(limits=IngestionLimits.from_env())
    print("FIVB VIS raw ingestion complete.")


if __name__ == "__main__":
    main()
