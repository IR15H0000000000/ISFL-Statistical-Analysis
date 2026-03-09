"""Scraper for game type classification (preseason / regular / playoff).

Parses GameResults.html and PlayoffResults.html from the ISFL Index to build
a mapping of game_id -> game_type for each season.

S1-26 (HTML era): game links follow ``Logs/N.html`` or ``Boxscores/N.html``
S27+  (JSON era): game links follow ``Boxscores/Boxscore.html?id=N``
Playoff games live on a separate ``PlayoffResults.html`` page for all eras.
"""

import re

from bs4 import BeautifulSoup

from isfl_epa.config import League, get_game_results_url, get_season_prefix, BASE_URL
from isfl_epa.logging_config import get_logger
from isfl_epa.scraper.cache import get_cached, save_to_cache
from isfl_epa.scraper.http import get_session

logger = get_logger("scraper.game_results")

# Matches both old-style (Logs/123.html) and new-style (Boxscore.html?id=123)
_GAME_ID_RE_OLD = re.compile(r"(?:Logs|Boxscores)/(\d+)\.html")
_GAME_ID_RE_NEW = re.compile(r"Boxscore\.html\?id=(\d+)")
_PRESEASON_RE = re.compile(r"Pre-Season\s+Week", re.IGNORECASE)
_REGULAR_RE = re.compile(r"^Week\s+\d+", re.IGNORECASE)


def _extract_game_id(href: str) -> int | None:
    """Extract game ID from either link format."""
    m = _GAME_ID_RE_OLD.search(href)
    if m:
        return int(m.group(1))
    m = _GAME_ID_RE_NEW.search(href)
    if m:
        return int(m.group(1))
    return None


def _parse_game_results_page(html: str) -> dict[int, str]:
    """Parse GameResults.html, returning game_id -> 'preseason' or 'regular'."""
    soup = BeautifulSoup(html, "html.parser")
    current_type = "regular"  # default if no header found
    mapping: dict[int, str] = {}

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if tds:
            text = tds[0].get_text(strip=True)
            if _PRESEASON_RE.search(text):
                current_type = "preseason"
            elif _REGULAR_RE.match(text):
                current_type = "regular"

        for a in tr.find_all("a", href=True):
            gid = _extract_game_id(a["href"])
            if gid is not None:
                mapping[gid] = current_type

    return mapping


def _parse_playoff_results_page(html: str) -> set[int]:
    """Parse PlayoffResults.html, returning set of playoff game IDs."""
    soup = BeautifulSoup(html, "html.parser")
    ids: set[int] = set()
    for a in soup.find_all("a", href=True):
        gid = _extract_game_id(a["href"])
        if gid is not None:
            ids.add(gid)
    return ids


def _get_playoff_results_url(league: League, season: int) -> str:
    prefix = get_season_prefix(league, season)
    return f"{BASE_URL}/{prefix}/PlayoffResults.html"


def fetch_game_type_mapping(
    league: League,
    season: int,
    *,
    force_refresh: bool = False,
) -> dict[int, str]:
    """Build a game_id -> game_type mapping for a season.

    Returns a dict mapping each game_id to one of:
    ``"preseason"``, ``"regular"``, or ``"playoff"``.
    """
    if not force_refresh:
        cached = get_cached(league, season, "game_types", 0)
        if cached is not None:
            logger.debug("Cache hit: %s S%d game_types", league.value, season)
            return cached

    session = get_session()
    mapping: dict[int, str] = {}

    # 1. Parse GameResults.html (preseason + regular season)
    url = get_game_results_url(league, season)
    logger.debug("Fetching game results: %s S%d", league.value, season)
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        mapping.update(_parse_game_results_page(resp.text))
    except Exception:
        logger.warning("Failed to fetch GameResults.html for %s S%d", league.value, season)

    # 2. Parse PlayoffResults.html (playoff games)
    playoff_url = _get_playoff_results_url(league, season)
    try:
        resp = session.get(playoff_url, timeout=30)
        resp.raise_for_status()
        playoff_ids = _parse_playoff_results_page(resp.text)
        for gid in playoff_ids:
            mapping[gid] = "playoff"
        logger.debug(
            "%s S%d game types: %d preseason, %d regular, %d playoff",
            league.value, season,
            sum(1 for v in mapping.values() if v == "preseason"),
            sum(1 for v in mapping.values() if v == "regular"),
            len(playoff_ids),
        )
    except Exception:
        logger.warning("Failed to fetch PlayoffResults.html for %s S%d", league.value, season)

    save_to_cache(league, season, "game_types", 0, mapping)
    return mapping
