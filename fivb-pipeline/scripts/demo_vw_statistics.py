#!/usr/bin/env python3
"""
Smoke-test Volleyball World BPT statistics parsing without a database.

  cd fivb-pipeline && pip install -r requirements-cron.txt
  python3 scripts/demo_vw_statistics.py
  python3 scripts/demo_vw_statistics.py --live --max-urls 8

Uses the public sitemap to list stat pages, then parses HTML for each URL.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(description="Demo VW stats HTML parsing")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Fetch sitemap and sample pages from en.volleyballworld.com",
    )
    parser.add_argument("--max-urls", type=int, default=12, help="Cap URLs when --live")
    args = parser.parse_args()

    from etl.vw_statistics import (
        fetch_and_parse_stat_page,
        fetch_sitemap_stat_urls,
        parse_vw_player_stats_html,
        _http_session,
    )

    sample = """
<table class="vbw-o-table vbw-tournament-player-statistic-table vbw-stats-scorers">
<thead><tr class="vbw-o-table__header-group">
<th class="vbw-o-table__header rank"><span>Rank</span></th>
<th class="vbw-o-table__header playername"><span>Player</span></th>
<th class="vbw-o-table__header federation"><span>Team</span></th>
<th class="vbw-o-table__header attacks"><span>Points</span></th>
</tr></thead>
<tbody>
<tr class="vbw-o-table__row" data-player-no="12345">
<td class="vbw-o-table__cell rank">1</td>
<td class="vbw-o-table__cell playername"><a href="/p/12345">Test Player</a></td>
<td class="vbw-o-table__cell federation">USA</td>
<td class="vbw-o-table__cell attacks">42</td>
</tr>
</tbody>
</table>
"""
    rows = parse_vw_player_stats_html(sample)
    assert len(rows) == 1, rows
    assert rows[0]["vw_player_id"] == 12345
    assert rows[0]["metrics"].get("attacks") == "42"
    print("fixture HTML: OK (1 row)")

    if not args.live:
        print("Pass --live to fetch sitemap + real pages.")
        return

    session = _http_session()
    all_urls = fetch_sitemap_stat_urls(session=session)
    urls = all_urls[: max(1, args.max_urls)]
    print(f"sitemap: parsing first {len(urls)} stat URLs (of {len(all_urls)} total)")
    ok = 0
    empty = 0
    for u in urls:
        final, parsed = fetch_and_parse_stat_page(session, u)
        if parsed:
            ok += 1
            print(f"  OK {len(parsed):4} rows  {final}")
        else:
            empty += 1
            print(f"  -- 0 rows  {final}")
    print(f"summary: pages_with_rows={ok} empty={empty} tried={len(urls)}")


if __name__ == "__main__":
    main()
