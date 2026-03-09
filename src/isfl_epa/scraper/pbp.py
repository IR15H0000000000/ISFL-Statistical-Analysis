import json
from concurrent.futures import ThreadPoolExecutor

from lzstring import LZString

from isfl_epa.config import (
    League,
    PBP_FILES_PER_SEASON,
    SCRAPER_MAX_WORKERS,
    get_pbp_file_num,
    get_pbp_url,
)
from isfl_epa.scraper.cache import get_cached, save_to_cache
from isfl_epa.scraper.http import get_session

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
    resp = get_session().get(url, timeout=30)
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
    """Fetch all PBP data for an entire season (all 10 files, concurrent)."""
    all_games = []
    with ThreadPoolExecutor(max_workers=SCRAPER_MAX_WORKERS) as pool:
        futures = {
            pool.submit(fetch_pbp_file, league, season, fn, force_refresh=force_refresh): fn
            for fn in range(1, PBP_FILES_PER_SEASON + 1)
        }
        # Collect in file_num order for deterministic output
        results = {fn: f.result() for f, fn in sorted(
            ((f, fn) for f, fn in futures.items()), key=lambda x: x[1]
        )}
    for fn in range(1, PBP_FILES_PER_SEASON + 1):
        all_games.extend(results[fn])
    return all_games
