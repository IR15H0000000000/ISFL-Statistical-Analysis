"""Scraper for team roster pages from index.sim-football.com.

Fetches roster HTML pages and extracts player positions, overall ratings,
and index player IDs. Positions T/C/G are normalized to OL.
"""

import re
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

from isfl_epa.config import League, get_roster_url
from isfl_epa.scraper.cache import get_cached, save_to_cache

# Positions to normalize to OL
_OL_POSITIONS = {"T", "C", "G"}


def _parse_roster_html(html: str) -> list[dict]:
    """Parse a team roster HTML page into a list of player dicts."""
    soup = BeautifulSoup(html, "html.parser")

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
        return []

    rows = roster_table.find_all("tr")
    if len(rows) < 3:
        return []

    # Row 0 is "Active Roster" title, Row 1 has column headers
    header_cells = [c.get_text(strip=True) for c in rows[1].find_all(["td", "th"])]
    col_map = {name: i for i, name in enumerate(header_cells)}

    pos_idx = col_map.get("Pos")
    ovr_idx = col_map.get("Ovr")
    name_idx = col_map.get("Player", 0)

    if pos_idx is None:
        return []

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

    return players


_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}


def _clean_last_name(name: str) -> str:
    """Remove suffixes like Jr., III, etc. from a last name."""
    parts = name.split()
    while len(parts) > 1 and parts[-1].lower().rstrip(".") in {"jr", "sr", "ii", "iii", "iv", "v"}:
        parts.pop()
    return " ".join(parts)


def _parse_name(name: str) -> tuple[str, str]:
    """Parse a name in either 'Last, F.' or 'First Last' format.

    Handles suffixes like 'Jr.', 'III', and parenthetical tags like '(C)', '(R)', '(BOT)'.
    Returns (last_name_lower, first_initial_lower).
    """
    # Remove parenthetical tags like (C), (R), (BOT)
    cleaned = re.sub(r"\s*\([^)]*\)", "", name).strip()
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
    while len(parts) > 1 and parts[-1].lower().rstrip(".") in {"jr", "sr", "ii", "iii", "iv", "v"}:
        parts.pop()
    last = parts[-1] if parts else ""
    first_initial = parts[0][0].lower() if parts[0] else ""
    return last.lower(), first_initial


def match_roster_to_players(
    roster_entries: list[dict],
    player_names: list[dict],
) -> list[dict]:
    """Match roster entries to player_names records by last name + first initial.

    Args:
        roster_entries: list of dicts with 'name', 'position', etc.
        player_names: list of dicts with 'player_id', 'name' (full name), 'team'

    Returns:
        roster_entries updated with 'player_id' field (None if no match).
    """
    # Build lookup: (last_name_lower, first_initial_lower) -> player_id
    name_lookup: dict[tuple[str, str], int] = {}
    for pn in player_names:
        key = _parse_name(pn["name"])
        if key[0]:
            name_lookup[key] = pn["player_id"]

    for entry in roster_entries:
        key = _parse_name(entry["name"])
        entry["player_id"] = name_lookup.get(key)

    return roster_entries


def fetch_team_roster(
    league: League,
    season: int,
    team_id: int,
    *,
    force_refresh: bool = False,
) -> list[dict]:
    """Fetch and parse a single team's roster page."""
    cache_key = f"roster_{team_id}"
    if not force_refresh:
        cached = get_cached(league, season, "roster", team_id)
        if cached is not None:
            return cached

    url = get_roster_url(league, season, team_id)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.HTTPError:
        return []

    players = _parse_roster_html(resp.text)
    if players:
        save_to_cache(league, season, "roster", team_id, players)
    return players


def fetch_season_rosters(
    league: League,
    season: int,
    *,
    max_teams: int = 16,
    force_refresh: bool = False,
) -> list[dict]:
    """Fetch rosters for all teams in a season.

    Iterates team IDs 1..max_teams, stopping after consecutive 404s.
    Returns combined list with 'team_id' added to each entry.
    """
    all_players = []
    consecutive_empty = 0

    for team_id in range(1, max_teams + 1):
        roster = fetch_team_roster(
            league, season, team_id, force_refresh=force_refresh
        )
        if not roster:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            continue

        consecutive_empty = 0
        for entry in roster:
            entry["team_id"] = team_id
        all_players.extend(roster)

    return all_players
