import json
from concurrent.futures import ThreadPoolExecutor

from lzstring import LZString

from isfl_epa.config import (
    BOXSCORE_FILES_PER_SEASON,
    League,
    SCRAPER_MAX_WORKERS,
    get_boxscore_file_num,
    get_boxscore_url,
)
from isfl_epa.logging_config import get_logger
from isfl_epa.scraper.cache import get_cached, save_to_cache
from isfl_epa.scraper.http import get_session

_lz = LZString()
logger = get_logger("scraper.boxscore")


def fetch_boxscore_file(
    league: League, season: int, file_num: int, *, force_refresh: bool = False
) -> list[dict]:
    """Fetch and decompress a single boxscoreData file."""
    if not force_refresh:
        cached = get_cached(league, season, "boxscore", file_num)
        if cached is not None:
            logger.debug("Cache hit: %s S%d boxscore file %d", league.value, season, file_num)
            return cached

    url = get_boxscore_url(league, season, file_num)
    logger.debug("Fetching %s S%d boxscore file %d from %s", league.value, season, file_num, url)
    resp = get_session().get(url, timeout=30)
    resp.raise_for_status()

    text = resp.content.decode("utf-8-sig")
    decompressed = _lz.decompressFromEncodedURIComponent(text)
    if not decompressed:
        logger.warning(
            "Decompression returned empty for %s S%d boxscore file %d",
            league.value, season, file_num,
        )
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
    league: League,
    season: int,
    *,
    force_refresh: bool = False,
    refresh_last: int = 0,
) -> list[dict]:
    """Fetch all boxscore data for an entire season (concurrent).

    Args:
        league: League to fetch.
        season: Season number.
        force_refresh: Re-download all files even if cached.
        refresh_last: Only re-download the last N files (where new games
            typically appear). Earlier files serve from cache. Ignored if
            ``force_refresh`` is True.
    """
    if refresh_last > 0:
        logger.info(
            "Refreshing last %d boxscore file(s) for %s S%d",
            refresh_last, league.value, season,
        )

    all_games = []
    with ThreadPoolExecutor(max_workers=SCRAPER_MAX_WORKERS) as pool:
        futures = {}
        for fn in range(1, BOXSCORE_FILES_PER_SEASON + 1):
            per_file_refresh = force_refresh or (
                refresh_last > 0 and fn > BOXSCORE_FILES_PER_SEASON - refresh_last
            )
            futures[pool.submit(
                fetch_boxscore_file, league, season, fn, force_refresh=per_file_refresh
            )] = fn
        results = {fn: f.result() for f, fn in sorted(
            ((f, fn) for f, fn in futures.items()), key=lambda x: x[1]
        )}
    for fn in range(1, BOXSCORE_FILES_PER_SEASON + 1):
        all_games.extend(results[fn])
    return all_games
