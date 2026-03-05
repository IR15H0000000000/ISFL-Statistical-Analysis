import json

import requests
from lzstring import LZString

from isfl_epa.config import (
    BOXSCORE_FILES_PER_SEASON,
    League,
    get_boxscore_file_num,
    get_boxscore_url,
)
from isfl_epa.scraper.cache import get_cached, save_to_cache

_lz = LZString()


def fetch_boxscore_file(
    league: League, season: int, file_num: int, *, force_refresh: bool = False
) -> list[dict]:
    """Fetch and decompress a single boxscoreData file."""
    if not force_refresh:
        cached = get_cached(league, season, "boxscore", file_num)
        if cached is not None:
            return cached

    url = get_boxscore_url(league, season, file_num)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    text = resp.content.decode("utf-8-sig")
    decompressed = _lz.decompressFromEncodedURIComponent(text)
    if not decompressed:
        return []

    data = json.loads(decompressed)
    save_to_cache(league, season, "boxscore", file_num, data)
    return data


def fetch_boxscore(
    league: League, season: int, game_id: int, *, force_refresh: bool = False
) -> dict | None:
    """Fetch a single game's boxscore by game ID."""
    file_num = get_boxscore_file_num(game_id)
    games = fetch_boxscore_file(league, season, file_num, force_refresh=force_refresh)
    for game in games:
        if game.get("id") == game_id:
            return game
    return None


def fetch_all_season_boxscores(
    league: League, season: int, *, force_refresh: bool = False
) -> list[dict]:
    """Fetch all boxscore data for an entire season."""
    all_games = []
    for file_num in range(1, BOXSCORE_FILES_PER_SEASON + 1):
        games = fetch_boxscore_file(
            league, season, file_num, force_refresh=force_refresh
        )
        all_games.extend(games)
    return all_games
