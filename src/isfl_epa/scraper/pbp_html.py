"""Scraper for S1-26 (2016 engine) HTML play-by-play pages.

Each game is a separate HTML page with a <table class=Grid> containing
one <tr> per play. This module fetches the HTML, parses it with
BeautifulSoup, and normalizes the output to the same dict format used
by the S27+ JSON scraper so the downstream parser is engine-agnostic.
"""

import re
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from isfl_epa.config import League, SCRAPER_MAX_WORKERS, get_game_results_url, get_pbp_html_url
from isfl_epa.logging_config import get_logger
from isfl_epa.scraper.cache import get_cached, save_to_cache
from isfl_epa.scraper.http import get_session

logger = get_logger("scraper.pbp_html")

_QUARTER_MAP = {
    "First Quarter": "Q1",
    "Second Quarter": "Q2",
    "Third Quarter": "Q3",
    "Fourth Quarter": "Q4",
    "Overtime": "OT",
}

_LOGO_RE = re.compile(r"(\d+)_s\.png")
_GAME_ID_RE = re.compile(r"(?:Logs|Boxscores)/(\d+)\.html")
_CSS_MAP = {"f": "", "c": "c", "d": "d", "e": "e"}
_ACTION_START_RE = re.compile(r"^(Kickoff|Punt |Pass |Rush |Free Kick|Onsides? Kick)")


def _extract_team_id(td) -> int | None:
    img = td.find("img")
    if img and img.get("src"):
        m = _LOGO_RE.search(img["src"])
        if m:
            return int(m.group(1))
    return None


def _extract_css(td) -> str:
    for cls in td.get("class", []):
        if cls in _CSS_MAP:
            return _CSS_MAP[cls]
    return ""


def _is_continuation_row(row_data: dict) -> bool:
    """Check if a row is a continuation of the previous play.

    Continuation rows have empty down/distance and empty field position
    — e.g. kickoff return narratives split across multiple rows.
    Rows that start with a primary action keyword (Kickoff, Punt, etc.)
    are independent plays even if they lack down/distance info.
    """
    if row_data["t"] or row_data["o"]:
        return False
    if _ACTION_START_RE.match(row_data["m"]):
        return False
    return True


def _parse_html(html: str, game_id: int) -> dict:
    """Parse an HTML PBP page into the normalized Format A dict."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="Grid")
    if not table:
        logger.warning("No PBP table found in HTML for game %d", game_id)
        return {"id": game_id, "Q1": [], "Q2": [], "Q3": [], "Q4": [], "OT": []}

    quarters: dict[str, list[dict]] = {
        "Q1": [], "Q2": [], "Q3": [], "Q4": [], "OT": [],
    }
    current_quarter = "Q1"

    rows = table.find_all("tr")
    for row in rows:
        # Check for quarter header
        th = row.find("th")
        if th:
            text = th.get_text(strip=True)
            for label, qkey in _QUARTER_MAP.items():
                if label in text:
                    current_quarter = qkey
                    # Add quarter marker play
                    quarters[current_quarter].append({
                        "c": "15:00",
                        "t": "---",
                        "o": "--",
                        "m": text,
                        "css": "",
                        "s": None,
                    })
                    break
            continue

        tds = row.find_all("td")
        if len(tds) < 5:
            continue

        team_id = _extract_team_id(tds[0])
        clock = tds[1].get_text(strip=True)
        down_dist = tds[2].get_text(strip=True)
        field_pos = tds[3].get_text(strip=True)
        description = tds[4].get_text(strip=True)
        css = _extract_css(tds[4])

        row_data = {
            "c": clock,
            "t": down_dist,
            "o": field_pos,
            "m": description,
            "css": css,
            "s": None,
        }
        if team_id is not None:
            row_data["id"] = team_id

        # Merge continuation rows into the previous play
        plays = quarters[current_quarter]
        if plays and _is_continuation_row(row_data):
            prev = plays[-1]
            prev["m"] = prev["m"] + "<br/>" + description
            # Promote css if the continuation has a more specific class
            if css and not prev["css"]:
                prev["css"] = css
        else:
            plays.append(row_data)

    return {"id": game_id, **quarters}


def fetch_game_html(
    league: League,
    season: int,
    game_id: int,
    *,
    force_refresh: bool = False,
) -> dict:
    """Fetch and parse an HTML PBP page for a single game (S1-26).

    Returns a dict in the same shape as the S27+ JSON format:
    {"id": game_id, "Q1": [...], "Q2": [...], ...}
    """
    if not force_refresh:
        cached = get_cached(league, season, "pbp_html", game_id)
        if cached is not None:
            logger.debug("Cache hit: %s S%d pbp_html game %d", league.value, season, game_id)
            return cached

    url = get_pbp_html_url(league, season, game_id)
    logger.debug("Fetching %s S%d pbp_html game %d", league.value, season, game_id)
    resp = get_session().get(url, timeout=30)
    resp.raise_for_status()

    game = _parse_html(resp.text, game_id)
    save_to_cache(league, season, "pbp_html", game_id, game)
    return game


def fetch_game_ids(
    league: League, season: int, *, force_refresh: bool = False
) -> list[int]:
    """Discover all game IDs for an HTML-era season from GameResults.html."""
    if not force_refresh:
        cached = get_cached(league, season, "game_ids", 0)
        if cached is not None:
            return cached

    url = get_game_results_url(league, season)
    resp = get_session().get(url, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    ids = set()
    for a in soup.find_all("a", href=True):
        m = _GAME_ID_RE.search(a["href"])
        if m:
            ids.add(int(m.group(1)))

    result = sorted(ids)
    save_to_cache(league, season, "game_ids", 0, result)
    return result


def fetch_all_season_pbp_html(
    league: League, season: int, *, force_refresh: bool = False
) -> list[dict]:
    """Fetch all PBP data for an HTML-era season (concurrent)."""
    game_ids = fetch_game_ids(league, season, force_refresh=force_refresh)
    with ThreadPoolExecutor(max_workers=SCRAPER_MAX_WORKERS) as pool:
        futures = {
            pool.submit(fetch_game_html, league, season, gid, force_refresh=force_refresh): gid
            for gid in game_ids
        }
        # Collect in original game_id order
        results = {}
        for f, gid in futures.items():
            results[gid] = f.result()
    return [results[gid] for gid in game_ids]
