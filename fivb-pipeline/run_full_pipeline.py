#!/usr/bin/env python3
"""
Run full FIVB pipeline: VIS ETL -> Elo table init -> dbt -> Elo compute.

Intended for GitHub Actions against Supabase Postgres. Set DATABASE_URL to a
postgres URL (e.g. Supabase session or direct port 5432). SSL is forced for
*.supabase.co hosts when sslmode is missing.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

from etl.libpq_url import normalize_postgres_scheme, strip_unknown_libpq_query_params

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


def _normalize_database_url() -> None:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is required")
    url = normalize_postgres_scheme(url.strip())
    # Drop pooler/dashboard query keys libpq/psycopg2 reject (e.g. non-standard ?foo=…)
    url = strip_unknown_libpq_query_params(url)

    # dbt profile reads PGHOST / DB_PASSWORD / etc.
    raw = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    parsed = urlparse(raw)
    if not parsed.hostname:
        raise SystemExit("Could not parse host from DATABASE_URL")

    os.environ.setdefault("PGHOST", parsed.hostname)
    os.environ.setdefault("PGPORT", str(parsed.port or 5432))
    os.environ.setdefault("DB_USER", unquote(parsed.username or ""))
    os.environ.setdefault("DB_PASSWORD", unquote(parsed.password or ""))
    path = (parsed.path or "").lstrip("/")
    dbname = path.split("/")[0] if path else "postgres"
    os.environ.setdefault("DB_NAME", unquote(dbname))

    # SQLAlchemy + psycopg2
    if url.startswith("postgresql://") and "+psycopg2" not in url:
        sa_url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        sa_url = url
    if "sslmode=" not in sa_url and "supabase.co" in sa_url:
        sep = "&" if "?" in sa_url else "?"
        sa_url = f"{sa_url}{sep}sslmode=require"
    os.environ["DATABASE_URL"] = sa_url


def main() -> None:
    _normalize_database_url()
    os.chdir(ROOT)
    os.environ["DBT_PROFILES_DIR"] = str(ROOT / ".dbt")

    # Import after DATABASE_URL is set for dotenv + get_engine consumers.
    from etl.load_raw import IngestionLimits, run_full_ingestion

    print("Step 1/4: VIS raw ingestion…")
    run_full_ingestion(limits=IngestionLimits.from_env())

    print("Step 2/4: Elo tables init…")
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "elo_compute.py"), "--init-only"],
        cwd=ROOT,
        check=True,
    )

    print("Step 3/4: dbt run…")
    subprocess.run(_dbt_argv("deps"), cwd=ROOT, check=True)
    subprocess.run(_dbt_argv("run"), cwd=ROOT, check=True)

    print("Step 4/4: Elo compute…")
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "elo_compute.py")],
        cwd=ROOT,
        check=True,
    )
    print("Pipeline complete.")


if __name__ == "__main__":
    main()
