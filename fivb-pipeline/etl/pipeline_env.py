"""Shared DATABASE_URL normalization for pipeline entrypoints (Supabase SSL, dbt env)."""

from __future__ import annotations

import os
from urllib.parse import unquote, urlparse

from .libpq_url import normalize_postgres_scheme, strip_unknown_libpq_query_params


def normalize_database_url_for_pipeline() -> None:
    """
    Read DATABASE_URL from the environment, normalize for SQLAlchemy/psycopg2,
    set PGHOST/DB_* for dbt, and append sslmode=require for *.supabase.co when missing.
    Raises SystemExit if DATABASE_URL is unset or invalid.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is required")
    url = normalize_postgres_scheme(url.strip())
    url = strip_unknown_libpq_query_params(url)

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

    if url.startswith("postgresql://") and "+psycopg2" not in url:
        sa_url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        sa_url = url
    if "sslmode=" not in sa_url and "supabase.co" in sa_url:
        sep = "&" if "?" in sa_url else "?"
        sa_url = f"{sa_url}{sep}sslmode=require"
    os.environ["DATABASE_URL"] = sa_url
