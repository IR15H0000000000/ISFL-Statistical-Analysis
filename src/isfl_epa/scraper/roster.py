"""Scraper for team roster pages from index.sim-football.com.

Fetches roster HTML pages and extracts player positions, overall ratings,
and index player IDs. Positions T/C/G are normalized to OL.
"""

import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from isfl_epa.config import League, SCRAPER_MAX_WORKERS, get_roster_url
from isfl_epa.logging_config import get_logger
from isfl_epa.scraper.cache import get_cached, save_to_cache
from isfl_epa.scraper.http import get_session

logger = get_logger("scraper.roster")

# Positions to normalize to OL
_OL_POSITIONS = {"T", "C", "G"}

# Known team full-name -> abbreviation mapping.
# Covers all ISFL and DSFL teams across seasons.
# The primary team_id -> abbreviation mapping comes from get_team_id_to_abbr()
# in database.py; this is used as a fallback when DB data isn't available.
TEAM_ABBR_MAP: dict[str, str] = {
    # ISFL teams (current S59)
    "Baltimore Hawks": "BAL",
    "Yellowknife Wraiths": "YKW",
    "Colorado Yeti": "COL",
    "Arizona Outlaws": "AZ",
    "Orange County Otters": "OCO",
    "San Jose SaberCats": "SJS",
    "Cape Town Crash": "CTC",
    "New Orleans Secondline": "NOLA",
    "Osaka Kaiju": "OSK",
    "Austin Copperheads": "AUS",
    "Sarasota Sailfish": "SAR",
    "Honolulu Hahalua": "HON",
    "Black Forest Brood": "BFB",
    "New York Silverbacks": "NYS",
    # Historical ISFL teams
    "Berlin Fire Salamanders": "BER",
    "Chicago Butchers": "CHI",
    "Philadelphia Liberty": "PHI",
    "Norfolk Seawolves": "NOR",
    "Portland Pythons": "POR",
    "London Royals": "LDN",
    "Minnesota Grey Ducks": "MIN",
    # DSFL teams
    "Portland Pythons": "POR",
    "Kansas City Coyotes": "KC",
    "Norfolk Seawolves": "NOR",
    "London Royals": "LDN",
    "Tijuana Luchadores": "TIJ",
    "Minnesota Grey Ducks": "MIN",
    "Dallas Birddogs": "DAL",
    "Bondi Beach Buccaneers": "BBB",
}


def _extract_team_name(soup: BeautifulSoup) -> str | None:
    """Extract team name from the page heading (e.g., 'Baltimore Hawks (9-4-0)')."""
    # Look for the first heading or bold text with team name pattern
    for tag in soup.find_all(["h1", "h2", "h3", "b"]):
        text = tag.get_text(strip=True)
        # Match "Team Name (W-L-T)" pattern
        match = re.match(r"^(.+?)\s*\(\d+-\d+-\d+\)$", text)
        if match:
            return match.group(1).strip()
    return None


def _parse_roster_html(html: str) -> tuple[list[dict], str | None]:
    """Parse a team roster HTML page into a list of player dicts.

    Returns:
        Tuple of (players list, team_name or None).
    """
    soup = BeautifulSoup(html, "html.parser")

    # Extract team name from page heading
    team_name = _extract_team_name(soup)

    # Find the "Active Roster" table — first row text is "Active Roster"
    roster_table = None
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if first_row:
            text = first_row.get_text(strip=True)
            if text == "Active Roster":
                roster_table = table
                break

    if not roster_table:
        return [], team_name

    rows = roster_table.find_all("tr")
    if len(rows) < 3:
        return [], team_name

    # Row 0 is "Active Roster" title, Row 1 has column headers
    header_cells = [c.get_text(strip=True) for c in rows[1].find_all(["td", "th"])]
    col_map = {name: i for i, name in enumerate(header_cells)}

    pos_idx = col_map.get("Pos")
    ovr_idx = col_map.get("Ovr")
    name_idx = col_map.get("Player", 0)

    if pos_idx is None:
        return [], team_name

    players = []
    for row in rows[2:]:
        cells = row.find_all("td")
        if len(cells) <= max(pos_idx, name_idx):
            continue

        # Extract player name and index_player_id from link
        name_cell = cells[name_idx]
        link = name_cell.find("a")
        name = name_cell.get_text(strip=True)
        if not name:
            continue

        index_player_id = None
        if link and link.get("href"):
            parsed = urlparse(link["href"])
            qs = parse_qs(parsed.query)
            if "id" in qs:
                try:
                    index_player_id = int(qs["id"][0])
                except (ValueError, IndexError):
                    pass

        position = cells[pos_idx].get_text(strip=True)
        # Normalize OL positions
        if position in _OL_POSITIONS:
            position = "OL"

        overall = None
        if ovr_idx is not None and ovr_idx < len(cells):
            try:
                overall = int(cells[ovr_idx].get_text(strip=True))
            except ValueError:
                pass

        players.append({
            "name": name,
            "index_player_id": index_player_id,
            "position": position,
            "overall": overall,
        })

    return players, team_name


_SUFFIX_WORDS = {"jr", "sr", "ii", "iii", "iv", "v"}


def _to_ascii(name: str) -> str:
    """Strip non-ASCII characters for matching purposes."""
    return name.encode("ascii", "ignore").decode("ascii")


def _clean_last_name(name: str) -> str:
    """Remove suffixes like Jr., III, etc. from a last name."""
    parts = name.split()
    while len(parts) > 1 and parts[-1].lower().rstrip(".") in _SUFFIX_WORDS:
        parts.pop()
    return " ".join(parts)


def _parse_name(name: str) -> tuple[str, str]:
    """Parse a name in either 'Last, F.' or 'First Last' format.

    Handles suffixes like 'Jr.', 'III', and parenthetical tags like '(C)', '(R)', '(BOT)'.
    Returns (last_name_lower, first_initial_lower).
    """
    # Remove parenthetical tags like (C), (R), (BOT) and special chars
    cleaned = re.sub(r"\s*\([^)]*\)", "", name).strip()
    cleaned = re.sub(r"[™®©]", "", cleaned)
    cleaned = _to_ascii(cleaned)
    # Remove trailing dots from initials like "F."
    cleaned = cleaned.rstrip(".")

    if "," in cleaned:
        parts = cleaned.split(",", 1)
        last = _clean_last_name(parts[0].strip())
        first_part = parts[1].strip()
        first_initial = first_part[0].lower() if first_part else ""
        return last.lower(), first_initial

    # "First Last" or "First Last Jr." format
    parts = cleaned.split()
    if not parts:
        return "", ""
    # Strip suffixes from the end
    while len(parts) > 1 and parts[-1].lower().rstrip(".") in _SUFFIX_WORDS:
        parts.pop()
    last = parts[-1] if parts else ""
    first_initial = parts[0][0].lower() if parts[0] else ""
    return last.lower(), first_initial


def _parse_name_with_suffix(name: str) -> tuple[str, str]:
    """Like _parse_name but preserves suffixes in the last name.

    'McDummy Jr., W.' -> ('mcdummy jr', 'w')
    'de la Rosa, M.' -> ('de la rosa', 'm')
    Used for higher-fidelity matching before falling back to _parse_name.
    """
    # Remove parenthetical tags like (C), (R), (BOT) and special chars
    cleaned = re.sub(r"\s*\([^)]*\)", "", name).strip()
    cleaned = re.sub(r"[™®©]", "", cleaned)
    cleaned = _to_ascii(cleaned)
    # Remove trailing dots
    cleaned = cleaned.rstrip(".")

    if "," in cleaned:
        parts = cleaned.split(",", 1)
        last = parts[0].strip().rstrip(".")
        first_part = parts[1].strip()
        first_initial = first_part[0].lower() if first_part else ""
        return last.lower(), first_initial

    # "First Last" format
    parts = cleaned.split()
    if not parts:
        return "", ""
    last = " ".join(parts[1:]) if len(parts) > 1 else parts[0]
    first_initial = parts[0][0].lower() if parts[0] else ""
    return last.lower().rstrip("."), first_initial


def match_roster_to_players(
    roster_entries: list[dict],
    player_names: list[dict],
) -> list[dict]:
    """Match roster entries to player_names records by name.

    Uses two-pass matching:
    1. Exact match with suffixes preserved (e.g., "mcdummy jr" + "w")
    2. Relaxed match without suffixes (e.g., "mcdummy" + "w")

    Args:
        roster_entries: list of dicts with 'name', 'position', etc.
        player_names: list of dicts with 'player_id', 'name' (full name), 'team'

    Returns:
        roster_entries updated with 'player_id' field (None if no match).
    """
    # Build both exact and relaxed lookups
    exact_lookup: dict[tuple[str, str], int] = {}
    relaxed_lookup: dict[tuple[str, str], int] = {}
    for pn in player_names:
        exact_key = _parse_name_with_suffix(pn["name"])
        relaxed_key = _parse_name(pn["name"])
        if exact_key[0]:
            exact_lookup[exact_key] = pn["player_id"]
        if relaxed_key[0]:
            relaxed_lookup[relaxed_key] = pn["player_id"]

    for entry in roster_entries:
        # Try exact match first (with suffixes)
        exact_key = _parse_name_with_suffix(entry["name"])
        pid = exact_lookup.get(exact_key)
        if pid is None:
            # Fall back to relaxed match (without suffixes)
            relaxed_key = _parse_name(entry["name"])
            pid = relaxed_lookup.get(relaxed_key)
        entry["player_id"] = pid

    return roster_entries


def fetch_team_roster(
    league: League,
    season: int,
    team_id: int,
    *,
    force_refresh: bool = False,
) -> tuple[list[dict], str | None]:
    """Fetch and parse a single team's roster page.

    Returns:
        Tuple of (players list, team_name or None).
    """
    if not force_refresh:
        cached = get_cached(league, season, "roster", team_id)
        if cached is not None:
            return cached, None

    url = get_roster_url(league, season, team_id)
    try:
        resp = get_session().get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Failed to fetch roster for %s S%d team %d: %s", league.value, season, team_id, e)
        return [], None

    players, team_name = _parse_roster_html(resp.text)
    if players:
        save_to_cache(league, season, "roster", team_id, players)
    return players, team_name


def fetch_season_rosters(
    league: League,
    season: int,
    *,
    max_teams: int = 16,
    force_refresh: bool = False,
) -> tuple[list[dict], dict[int, str]]:
    """Fetch rosters for all teams in a season (concurrent).

    Fetches all team IDs 1..max_teams concurrently, then filters results.

    Returns:
        Tuple of (combined player list with 'team_id', team_id -> team_name map).
    """
    team_ids = list(range(1, max_teams + 1))

    with ThreadPoolExecutor(max_workers=SCRAPER_MAX_WORKERS) as pool:
        futures = {
            pool.submit(fetch_team_roster, league, season, tid, force_refresh=force_refresh): tid
            for tid in team_ids
        }
        results = {}
        for f, tid in futures.items():
            results[tid] = f.result()

    all_players = []
    team_names: dict[int, str] = {}

    for team_id in team_ids:
        roster, team_name = results[team_id]
        if not roster:
            continue
        if team_name:
            team_names[team_id] = team_name
        for entry in roster:
            entry["team_id"] = team_id
        all_players.extend(roster)

    return all_players, team_names
