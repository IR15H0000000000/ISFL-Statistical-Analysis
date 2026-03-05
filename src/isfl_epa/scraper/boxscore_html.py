"""Scraper for S1-26 (2016 engine) HTML boxscore pages.

Fetches individual boxscore HTML pages and extracts team stats,
normalizing to the same dict keys used by the S27+ JSON boxscore
scraper so cross-validation logic is format-agnostic.
"""

import requests
from bs4 import BeautifulSoup

from isfl_epa.config import League, get_boxscore_html_url
from isfl_epa.scraper.cache import get_cached, save_to_cache
from isfl_epa.scraper.pbp_html import fetch_game_ids

# Stat labels in the Team Stats table → output keys
_STAT_MAP = {
    "Passing": ("aPassing", "hPassing"),
    "Rushing": ("aRushing", "hRushing"),
    "Rushing Attempts": ("aRushes", "hRushes"),
}


def _parse_boxscore_html(html: str, game_id: int) -> dict | None:
    """Parse an HTML boxscore page into a normalized dict."""
    soup = BeautifulSoup(html, "html.parser")

    # Find the Team Stats table — its first row has a single cell with exactly "Team Stats"
    team_stats_table = None
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if first_row:
            first_cell = first_row.find(["td", "th"])
            if first_cell and first_cell.get_text(strip=True) == "Team Stats":
                team_stats_table = table
                break

    if not team_stats_table:
        return None

    result: dict = {"id": game_id}
    rows = team_stats_table.find_all("tr")

    for row in rows:
        cells = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cells) < 3:
            continue

        label = cells[0]

        if label in _STAT_MAP:
            away_key, home_key = _STAT_MAP[label]
            result[away_key] = int(cells[1])
            result[home_key] = int(cells[2])
        elif label == "Comp/Att":
            for i, side in enumerate(["a", "h"], start=1):
                parts = cells[i].split("/")
                result[f"{side}Comp"] = int(parts[0])
                result[f"{side}Att"] = int(parts[1])

    return result


def fetch_boxscore_html(
    league: League,
    season: int,
    game_id: int,
    *,
    force_refresh: bool = False,
) -> dict | None:
    """Fetch and parse an HTML boxscore for a single game (S1-26)."""
    if not force_refresh:
        cached = get_cached(league, season, "boxscore_html", game_id)
        if cached is not None:
            return cached

    url = get_boxscore_html_url(league, season, game_id)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    result = _parse_boxscore_html(resp.text, game_id)
    if result:
        save_to_cache(league, season, "boxscore_html", game_id, result)
    return result


def fetch_all_season_boxscores_html(
    league: League, season: int, *, force_refresh: bool = False
) -> list[dict]:
    """Fetch all boxscore data for an HTML-era season."""
    game_ids = fetch_game_ids(league, season, force_refresh=force_refresh)
    all_boxes = []
    for gid in game_ids:
        box = fetch_boxscore_html(league, season, gid, force_refresh=force_refresh)
        if box:
            all_boxes.append(box)
    return all_boxes
