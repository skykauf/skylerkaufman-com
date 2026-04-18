"""
Ingest Beach Pro Tour player statistics from en.volleyballworld.com HTML pages.

Discovers pages via the public sitemap, fetches each statistics subpage
(e.g. .../statistics/men/best-attackers/), and parses tables with class
vbw-tournament-player-statistic-table / vbw-stats-scorers.

This is independent of the FIVB VIS API; VW uses Volleyball World player ids
(data-player-no on each row). Store metrics as jsonb for varying column sets
per stat category.
"""

from __future__ import annotations

import logging
import os
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from sqlalchemy.engine import Engine
from tqdm import tqdm

from .db import bulk_upsert, ensure_raw_tables, get_engine

logger = logging.getLogger(__name__)

DEFAULT_SITEMAP = "https://en.volleyballworld.com/sitemap.xml"
USER_AGENT = (
    "skylerkaufman-fivb-pipeline/1.0 (+https://github.com/skylerkaufman/skylerkaufman-com; "
    "vw-stats-ingest)"
)

_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def _int_env(key: str, default: int | None) -> int | None:
    v = os.environ.get(key, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def canonical_stat_url(url: str) -> str:
    """Normalize scheme and trailing slash for stable PKs."""
    p = urlparse(url.strip())
    netloc = (p.netloc or "en.volleyballworld.com").lower()
    path = (p.path or "").rstrip("/") + "/"
    return urlunparse(("https", netloc, path, "", "", ""))


def is_beach_bpt_player_stats_url(url: str) -> bool:
    """True for BPT tournament player stat subpages (not /statistics/ index only)."""
    u = url.strip().lower().rstrip("/")
    if "/beachvolleyball/competitions/" not in u:
        return False
    if "beach-pro-tour" not in u:
        return False
    if "/statistics/men/" not in u and "/statistics/women/" not in u:
        return False
    slug = u.split("/")[-1]
    return slug.startswith("best-")


def event_key_from_stat_url(url: str) -> str:
    """Path prefix up to /statistics (tournament hub identity)."""
    p = urlparse(url.strip())
    path = p.path.rstrip("/")
    idx = path.find("/statistics")
    if idx == -1:
        return path
    return path[:idx]


def fetch_sitemap_stat_urls(
    sitemap_url: str = DEFAULT_SITEMAP,
    session: requests.Session | None = None,
) -> list[str]:
    """Return sorted unique canonical URLs for BPT player statistics pages."""
    sess = session or requests.Session()
    headers = {"User-Agent": USER_AGENT, "Accept": "application/xml,text/xml,*/*"}
    r = sess.get(sitemap_url, headers=headers, timeout=120)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    out: set[str] = set()
    for loc in root.findall(".//sm:loc", _NS):
        if loc.text:
            url = loc.text.strip()
            if is_beach_bpt_player_stats_url(url):
                out.add(canonical_stat_url(url))
    return sorted(out)


def _cell_semantic_key(classes: list[str], *, header: bool) -> str | None:
    skip = {"vbw-o-table__header", "vbw-o-table__cell"}
    for c in classes:
        if c in skip:
            continue
        return c
    return None


def parse_vw_player_stats_html(html: str) -> list[dict[str, Any]]:
    """
    Parse player statistics rows from a VW statistics HTML document.
    Returns list of dicts with keys: vw_player_id, display_rank, player_name,
    federation, metrics (dict of column_key -> text).
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.vbw-tournament-player-statistic-table")
    if table is None:
        table = soup.select_one("table.vbw-stats-scorers")
    if table is None:
        return []

    header_tr = table.select_one("thead tr.vbw-o-table__header-group") or table.select_one(
        "thead tr"
    )
    if not header_tr:
        return []
    col_keys: list[str | None] = []
    for th in header_tr.find_all("th"):
        k = _cell_semantic_key(th.get("class") or [], header=True)
        col_keys.append(k)

    rows_out: list[dict[str, Any]] = []
    for tr in table.select("tbody tr[data-player-no]"):
        try:
            pid = int(tr.get("data-player-no", "0"))
        except (TypeError, ValueError):
            continue
        if not pid:
            continue
        tds = tr.find_all("td", recursive=False)
        metrics: dict[str, str] = {}
        display_rank: int | None = None
        player_name: str | None = None
        federation: str | None = None
        for i, td in enumerate(tds):
            key = col_keys[i] if i < len(col_keys) else None
            if not key:
                continue
            text = " ".join(td.stripped_strings)
            text = text.strip() if text else ""
            metrics[key] = text
            if key == "rank":
                try:
                    display_rank = int(text.replace("=", "").strip())
                except ValueError:
                    display_rank = None
            elif key == "playername":
                link = td.find("a")
                player_name = link.get_text(strip=True) if link else text
            elif key == "federation":
                federation = text
        rows_out.append(
            {
                "vw_player_id": pid,
                "display_rank": display_rank,
                "player_name": player_name,
                "federation": federation,
                "metrics": metrics,
            }
        )
    return rows_out


def _http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,*/*"})
    return s


def fetch_and_parse_stat_page(session: requests.Session, url: str) -> tuple[str, list[dict[str, Any]]]:
    """GET url, return (final_url, parsed rows). Empty rows on 404/parse miss."""
    try:
        r = session.get(url, timeout=90, allow_redirects=True)
        if r.status_code == 404:
            logger.warning("VW stats 404: %s", url)
            return canonical_stat_url(r.url), []
        r.raise_for_status()
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "html" not in ctype and "text" not in ctype:
            logger.warning("VW stats unexpected content-type for %s: %s", url, ctype)
        rows = parse_vw_player_stats_html(r.text)
        return canonical_stat_url(r.url), rows
    except requests.RequestException as e:
        logger.warning("VW stats fetch failed %s: %s", url, e)
        return canonical_stat_url(url), []


def run_vw_statistics_ingestion(
    engine: Engine | None = None,
    *,
    sitemap_url: str | None = None,
    max_urls: int | None = None,
    max_workers: int = 6,
) -> dict[str, int]:
    """
    Fetch all BPT player statistics URLs from the sitemap, parse HTML, upsert into
    raw.raw_vw_player_tournament_stats.

    Env:
      ETL_VW_STATS_SITEMAP — override sitemap URL
      ETL_VW_STATS_MAX_URLS — cap number of stat pages (for testing)
      ETL_VW_STATS_WORKERS — parallel fetch workers (default 6)
    """
    engine = engine or get_engine()
    ensure_raw_tables(engine)

    sm = sitemap_url or os.environ.get("ETL_VW_STATS_SITEMAP", DEFAULT_SITEMAP).strip()
    cap = max_urls if max_urls is not None else _int_env("ETL_VW_STATS_MAX_URLS", None)
    workers = max(1, int(os.environ.get("ETL_VW_STATS_WORKERS", str(max_workers))))

    session = _http_session()
    urls = fetch_sitemap_stat_urls(sm, session=session)
    if cap is not None and cap > 0:
        urls = urls[:cap]

    rows_buffer: list[dict[str, Any]] = []
    stats = {"urls": 0, "rows": 0, "empty": 0, "errors": 0}

    def flush() -> None:
        nonlocal rows_buffer
        if rows_buffer:
            bulk_upsert(
                engine,
                "raw.raw_vw_player_tournament_stats",
                rows_buffer,
                ("stat_url", "vw_player_id"),
            )
            stats["rows"] += len(rows_buffer)
            rows_buffer = []

    # Optional throttle (default 0 when parallelizing; set e.g. 0.05 if remote limits)
    delay = float(os.environ.get("ETL_VW_STATS_REQUEST_DELAY", "0" if workers > 1 else "0.1"))

    def worker(u: str) -> tuple[str, list[dict[str, Any]]]:
        if delay > 0:
            time.sleep(delay)
        return fetch_and_parse_stat_page(session, u)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(worker, u): u for u in urls}
        for fut in tqdm(as_completed(futures), total=len(urls), desc="VW stats pages", unit="page"):
            u = futures[fut]
            try:
                final_url, parsed = fut.result()
            except Exception as e:
                logger.warning("VW stats worker error for %s: %s", u, e)
                stats["errors"] += 1
                continue
            stats["urls"] += 1
            ek = event_key_from_stat_url(final_url)
            if not parsed:
                stats["empty"] += 1
            for pr in parsed:
                rows_buffer.append(
                    {
                        "event_key": ek,
                        "stat_url": final_url,
                        "vw_player_id": pr["vw_player_id"],
                        "display_rank": pr.get("display_rank"),
                        "player_name": pr.get("player_name"),
                        "federation": pr.get("federation"),
                        "metrics": pr.get("metrics") or {},
                    }
                )
            if len(rows_buffer) >= 500:
                flush()
    flush()
    return stats
