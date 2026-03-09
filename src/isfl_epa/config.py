import os
from enum import Enum


class EngineVersion(str, Enum):
    DDSPF_2016 = "2016"
    DDSPF_2022 = "2022"


class League(str, Enum):
    ISFL = "ISFL"
    DSFL = "DSFL"


BASE_URL = "https://index.sim-football.com"

# Season ranges per league (full index range)
ISFL_SEASONS = range(1, 60)
DSFL_SEASONS = range(3, 60)

# S27+ (2022 engine): PBP in compressed JSON files (pbpData1-10.txt)
# S1-26 (2016 engine): PBP in individual HTML pages per game
ISFL_PBP_JSON_SEASONS = range(27, 60)
DSFL_PBP_JSON_SEASONS = range(27, 60)

# S1-23 used NSFL prefix, S24+ used ISFL prefix
NSFL_SEASONS = range(1, 24)

# Engine cutoff: S1-26 = 2016 engine, S27+ = 2022 engine
ENGINE_CUTOFF_SEASON = 27

PBP_FILES_PER_SEASON = 10
BOXSCORE_FILES_PER_SEASON = 10

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DEFAULT_DATABASE_URL = "postgresql+psycopg://isfl:isfl@localhost:5432/isfl"


def get_database_url() -> str:
    """Return the database URL from the environment or fall back to default."""
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


# ---------------------------------------------------------------------------
# EPA model hyperparameters
# ---------------------------------------------------------------------------

EPA_MODEL_MAX_ITER = 200
EPA_MODEL_MAX_DEPTH = 6
EPA_MODEL_LEARNING_RATE = 0.1
EPA_MODEL_MIN_SAMPLES_LEAF = 100

# Feature engineering
SCORE_CLIP = 28
DISTANCE_CLIP = 30

# ---------------------------------------------------------------------------
# EPA stat thresholds (minimum plays for leaderboard display)
# ---------------------------------------------------------------------------

MIN_DROPBACKS = 100
MIN_RUSH_ATTEMPTS = 50
MIN_TARGETS = 30

# ---------------------------------------------------------------------------
# Training splits
# ---------------------------------------------------------------------------

TRAIN_SEASONS_2016 = list(range(1, 24))
TEST_SEASONS_2016 = list(range(24, 27))
TEST_SEASON_2022_STEP = 5  # every Nth season is test

# ---------------------------------------------------------------------------
# Scraper concurrency
# ---------------------------------------------------------------------------

SCRAPER_MAX_WORKERS = 5  # concurrent HTTP requests


def get_engine(season: int) -> EngineVersion:
    if season < ENGINE_CUTOFF_SEASON:
        return EngineVersion.DDSPF_2016
    return EngineVersion.DDSPF_2022


def get_season_prefix(league: League, season: int) -> str:
    if league == League.ISFL and season in NSFL_SEASONS:
        return f"NSFLS{season:02d}"
    return f"{league.value}S{season}"


def get_pbp_url(league: League, season: int, file_num: int) -> str:
    prefix = get_season_prefix(league, season)
    return f"{BASE_URL}/{prefix}/Logs/pbpData{file_num}.txt"


def get_boxscore_url(league: League, season: int, file_num: int) -> str:
    prefix = get_season_prefix(league, season)
    return f"{BASE_URL}/{prefix}/Boxscores/boxscoreData{file_num}.txt"


def get_game_results_url(league: League, season: int) -> str:
    prefix = get_season_prefix(league, season)
    return f"{BASE_URL}/{prefix}/GameResults.html"


def get_pbp_file_num(game_id: int) -> int:
    """Determine which pbpData file contains a given game ID."""
    return (game_id % PBP_FILES_PER_SEASON) + 1


def get_boxscore_file_num(game_id: int) -> int:
    """Determine which boxscoreData file contains a given game ID."""
    return (game_id % BOXSCORE_FILES_PER_SEASON) + 1


def get_roster_url(league: League, season: int, team_id: int) -> str:
    """URL for a team roster page."""
    prefix = get_season_prefix(league, season)
    return f"{BASE_URL}/{prefix}/Teams/{team_id}.html"


def get_pbp_html_url(league: League, season: int, game_id: int) -> str:
    """URL for individual HTML PBP page (S1-26, 2016 engine)."""
    prefix = get_season_prefix(league, season)
    return f"{BASE_URL}/{prefix}/Logs/{game_id}.html"


def get_boxscore_html_url(league: League, season: int, game_id: int) -> str:
    """URL for individual HTML boxscore page (S1-26, 2016 engine)."""
    prefix = get_season_prefix(league, season)
    return f"{BASE_URL}/{prefix}/Boxscores/{game_id}.html"
