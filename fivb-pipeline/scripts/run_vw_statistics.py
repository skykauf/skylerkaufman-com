#!/usr/bin/env python3
"""
Ingest Volleyball World BPT player statistics only (sitemap HTML → raw.raw_vw_player_tournament_stats).

Used by .github/workflows/fivb-vw-statistics.yml and can be run locally with DATABASE_URL set.

  cd fivb-pipeline && export DATABASE_URL='postgresql://...' && python scripts/run_vw_statistics.py
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> None:
    os.chdir(ROOT)
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    from etl.pipeline_env import normalize_database_url_for_pipeline
    from etl.vw_statistics import run_vw_statistics_ingestion

    normalize_database_url_for_pipeline()
    run_vw_statistics_ingestion()
    # Final stats are logged once at INFO by etl.vw_statistics (avoids duplicating the same line).


if __name__ == "__main__":
    main()
