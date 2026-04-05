"""
Normalize Postgres URLs for libpq / psycopg2.

Cloud dashboards sometimes append query keys that SQLAlchemy passes through to
psycopg2, which then rejects them (e.g. invalid connection option "supa").
"""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

# https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS
_LIBPQ_URI_QUERY_KEYS = frozenset(
    {
        "host",
        "hostaddr",
        "port",
        "user",
        "password",
        "dbname",
        "passfile",
        "connect_timeout",
        "client_encoding",
        "options",
        "application_name",
        "fallback_application_name",
        "keepalives",
        "keepalives_idle",
        "target_session_attrs",
        "sslmode",
        "sslcompression",
        "sslcert",
        "sslkey",
        "sslrootcert",
        "sslcrl",
        "requirepeer",
        "ssl_min_protocol_version",
        "ssl_max_protocol_version",
        "gssencmode",
        "krbsrvname",
        "gsslib",
        "channel_binding",
        "service",
    }
)


def normalize_postgres_scheme(url: str) -> str:
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


def strip_unknown_libpq_query_params(url: str) -> str:
    """
    Drop URI query parameters that libpq does not define. Keeps order of allowed keys.
    Handles postgresql:// and postgresql+psycopg2://.
    """
    had_psycopg2_driver = url.startswith("postgresql+psycopg2://")
    u = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    parsed = urlparse(u)
    if not parsed.query:
        return url

    kept = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() in _LIBPQ_URI_QUERY_KEYS
    ]
    new_query = urlencode(kept)
    rebuilt = urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )
    if had_psycopg2_driver:
        rebuilt = rebuilt.replace("postgresql://", "postgresql+psycopg2://", 1)
    return rebuilt
