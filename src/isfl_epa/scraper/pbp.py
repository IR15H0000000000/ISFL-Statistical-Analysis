import json

import requests
from lzstring import LZString

from isfl_epa.config import (
    League,
    PBP_FILES_PER_SEASON,
    get_pbp_file_num,
    get_pbp_url,
)
from isfl_epa.scraper.cache import get_cached, save_to_cache

_lz = LZString()


def fetch_pbp_file(
    league: League, season: int, file_num: int, *, force_refresh: bool = False
) -> list[dict]:
    """Fetch and decompress a single pbpData file. Returns list of game objects."""
    if not force_refresh:
        cached = get_cached(league, season, "pbp", file_num)
        if cached is not None:
            return cached

    url = get_pbp_url(league, season, file_num)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    # Response may be served as ISO-8859-1 but is actually UTF-8 with BOM
    text = resp.content.decode("utf-8-sig")
    decompressed = _lz.decompressFromEncodedURIComponent(text)
    if not decompressed:
        return []

    data = json.loads(decompressed)
    save_to_cache(league, season, "pbp", file_num, data)
    return data


def fetch_game(
    league: League, season: int, game_id: int, *, force_refresh: bool = False
) -> dict | None:
    """Fetch a single game's PBP data by game ID."""
    file_num = get_pbp_file_num(game_id)
    games = fetch_pbp_file(league, season, file_num, force_refresh=force_refresh)
    for game in games:
        if game.get("id") == game_id:
            return game
    return None


def fetch_all_season_pbp(
    league: League, season: int, *, force_refresh: bool = False
) -> list[dict]:
    """Fetch all PBP data for an entire season (all 10 files)."""
    all_games = []
    for file_num in range(1, PBP_FILES_PER_SEASON + 1):
        games = fetch_pbp_file(league, season, file_num, force_refresh=force_refresh)
        all_games.extend(games)
    return all_games
