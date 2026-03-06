# ISFL EPA - Play-by-Play Analyzer & EPA Calculator

Analyzes play-by-play data from the [ISFL](https://index.sim-football.com/) (International Simulation Football League) to extract player/team stats and calculate Expected Points Added (EPA).

## Overview

The ISFL uses the DDSPF simulation engine across two eras:

- **Seasons 27–59** (DDSPF 2022 engine): PBP via LZString-compressed JSON files
- **Seasons 1–26** (DDSPF 2016 engine): PBP via individual HTML game pages

This project:

1. **Scrapes & caches** PBP and boxscore data from the index (both formats)
2. **Parses** natural-language play descriptions into structured data
3. **Aggregates** player and team statistics
4. **Calculates EPA** (Expected Points Added) per play using a model trained on historical sim data

## Project Status

| Phase | Status | Description |
|-------|--------|-------------|
| 1. Data Extraction | Done | Scraper, decompression, caching for both engine eras |
| 2. Play Parsing | Done | Regex-based parser — ~100% parse rate, cross-validated against boxscores |
| 3. Stats Aggregation | Done | Player/team stats, player registry, PostgreSQL + Parquet storage, FastAPI |
| 4. EPA Model | Not started | Expected points model + EPA/play |

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync              # install dependencies
uv sync --all-extras # include dev dependencies (jupyter, pytest, etc.)
```

### Docker (for PostgreSQL + API)

```bash
docker compose up -d    # start PostgreSQL + FastAPI
docker compose down     # stop
```

The API runs at `http://localhost:8000` with auto-generated docs at `/docs`.

## Usage

### CLI

```bash
# Download and cache PBP + boxscore data for a season
uv run isfl-epa scrape --league ISFL --season 50

# Dump raw play-by-play JSON for a specific game
uv run isfl-epa explore --league ISFL --season 50 --game-id 9630

# Parse a season and load into PostgreSQL + Parquet
uv run isfl-epa build --league ISFL --season 50

# View season stats
uv run isfl-epa stats --season 50 --stat passing --top 10
uv run isfl-epa stats --season 50 --stat rushing --top 10
uv run isfl-epa stats --season 50 --stat team

# Export stats to file
uv run isfl-epa stats --season 50 --stat team --output data/processed/team_s50.csv

# Look up a player
uv run isfl-epa player --name "Patterson, J."
uv run isfl-epa player --id 42
```

### Python API

```python
from isfl_epa.config import League
from isfl_epa.scraper.pbp import fetch_game, fetch_all_season_pbp
from isfl_epa.scraper.boxscore import fetch_boxscore
from isfl_epa.parser.play_parser import parse_play, parse_game

# Fetch a single game's PBP data
raw = fetch_game(League.ISFL, season=50, game_id=9630)

# Fetch all games for a season
games = fetch_all_season_pbp(League.ISFL, season=50)

# Fetch a boxscore
boxscore = fetch_boxscore(League.ISFL, season=50, game_id=9630)

# Parse a game into structured plays
game = parse_game(raw, season=50, league="ISFL")
for play in game.plays:
    print(f"{play.play_type.value}: {play.yards_gained} yds")
```

## Data Source

Data is scraped from the [ISFL Index](https://index.sim-football.com/).

### Data Availability

| Seasons | Engine | PBP Format | Boxscore |
|---------|--------|------------|----------|
| 27–59 | DDSPF 2022 | LZString-compressed JSON (`pbpData{1-10}.txt`) | JSON (`boxscoreData{1-10}.txt`) |
| 1–26 | DDSPF 2016 | Individual HTML pages (`/Logs/{game_id}.html`) | HTML pages |

For S27+ JSON, a game's file is determined by `(game_id % 10) + 1`. For S1–26, each game has its own HTML page.

### PBP Play Fields

Each play object contains:

| Field | Description | Example |
|-------|-------------|---------|
| `id` | Team ID (possession) | `6` |
| `s` | Current score | `NYS 3 - SJS 0` |
| `c` | Game clock | `14:49` |
| `t` | Down and distance | `2nd and 7` |
| `o` | Field position | `SJS - 25` |
| `m` | Play description | `Pass by Patterson, J., complete to...` |
| `css` | Display styling | `c`, `d`, `e`, or empty |

## Project Structure

```
src/isfl_epa/
  config.py          # URLs, season ranges, engine version mapping
  cli.py             # Typer CLI (scrape, explore, build, stats, player)
  scraper/
    pbp.py           # Fetch + decompress S27+ JSON PBP data
    pbp_html.py      # Fetch + parse S1-26 HTML PBP pages
    boxscore.py      # Fetch + decompress S27+ JSON boxscore data
    boxscore_html.py # Fetch + parse S1-26 HTML boxscore pages
    cache.py         # Local JSON file cache
  parser/
    schema.py        # Pydantic models (PlayType, ParsedPlay, Game)
    play_parser.py   # Regex pipeline: structured fields + play descriptions
  stats/
    models.py        # Pydantic stat-line models (passing, rushing, etc.)
    aggregation.py   # Game -> player/team stat aggregation
  players/
    registry.py      # Cross-season player identity linking
    overrides.yaml   # Manual name correction overrides
  storage/
    database.py      # PostgreSQL schema, load, query (SQLAlchemy Core)
    parquet.py       # Parquet read/write for bulk analysis
  api/
    app.py           # FastAPI application
    routes/          # API endpoints (plays, stats, players)
  epa/               # (Phase 4) Expected points model + EPA calculation
tests/
  test_parser.py              # 46 unit tests covering all play types and edge cases
  cross_validate_test.py      # 16 tests cross-validating parsed stats vs boxscores
  stats_test.py               # 29 unit tests for stats aggregation
  stats_cross_validate_test.py # 8 tests validating stats vs boxscores
  registry_test.py            # 14 tests for player registry
  storage_test.py             # Parquet + PostgreSQL round-trip tests
  api_test.py                 # 11 FastAPI endpoint tests
notebooks/
  01_data_exploration.ipynb  # Raw data exploration
data/
  raw/               # Cached decompressed JSON (gitignored)
  processed/         # Parquet files (gitignored)
  models/            # Trained EP model artifacts (gitignored)
```

## Parser Details

The play parser (`parser/play_parser.py`) handles both engine formats by normalizing S1–26 HTML data into the same dict shape as S27+ JSON. It uses a two-stage regex pipeline:

1. **Structured field parsers** — extract down/distance, field position, and score from `t`, `o`, `s` fields
2. **Description parser** — match the primary action (pass, rush, sack, kick, etc.) then apply overlay checks (touchdown, fumble, interception, first down, PAT, etc.)

Supported play types: pass (complete/incomplete/intercepted/throwaway), rush, sack, punt (including blocked), kickoff (touchback/return/onside), field goal (including blocked), kneel, spike, penalty (nullifying/standalone), timeout, quarter markers.

### Cross-Validation

Parser output is validated against boxscore data across 8 seasons (S25, S26, S27, S28, S35, S45, S50, S59) covering both engine eras and ~1,100 games.

| Metric | S27+ (JSON) | S1–26 (HTML) |
|--------|-------------|--------------|
| Parse rate | ~100% | ~100% |
| rush / rush_yd | 95–100% | 92–96% |
| pass_yd / comp | 97–100% | 93–95% |
| att | 93–97% | 62–69% |

**Boxscore accounting conventions** discovered through analysis:
- Spikes count as pass attempts; sacks do not
- Kneels count as rush attempts in S28+ but not S27 (each kneel = -2 rush yards)
- 2-point conversion plays are excluded from passing and rushing totals

**Remaining mismatches** are caused by penalty-nullified plays. When a play is nullified, the PBP replaces the original description with the penalty text (e.g., `"Play nullified by X Penalty on Y: Holding"`). The original action (pass/rush) and its yardage are lost from the PBP data but may still be counted in the boxscore. This affects:
- **att (all seasons):** ~3–8% of games have boxscore att 1 higher than PBP, from a nullified pass attempt whose `"Pass "` prefix was not preserved
- **pass_yd (rare):** 1–3 games per season where a long completion was nullified, creating a 10–60 yard gap
- **HTML att (S25/S26):** Higher mismatch rate because the 2016 engine uses standalone penalty entries rather than `"Play nullified by"` format, making it impossible to associate penalties with the original play

These are inherent data limitations of the PBP format, not parser bugs.

## API Endpoints

When running via Docker Compose, the FastAPI server provides:

| Endpoint | Description |
|----------|-------------|
| `GET /plays/?season=&game_id=&play_type=&player_id=` | Query plays with filters |
| `GET /plays/{game_id}` | All plays for a game |
| `GET /stats/passing?season=&top=` | Season passing leaders |
| `GET /stats/rushing?season=&top=` | Season rushing leaders |
| `GET /stats/receiving?season=&top=` | Season receiving leaders |
| `GET /stats/defensive?season=&top=` | Season defensive leaders |
| `GET /stats/team?season=` | Team season stats |
| `GET /stats/player/{id}/game-log?category=` | Player game log |
| `GET /players/?name=&season=` | Search players |
| `GET /players/{id}` | Player profile with career stats |
| `GET /players/{id}/plays` | All plays involving a player |

Full OpenAPI docs at `http://localhost:8000/docs`.

## Roadmap

### Phase 4: EPA Model
- Train an Expected Points model on historical ISFL data (separate from NFL — different sim distributions)
- Calculate EPA per play: `EPA = EP_after - EP_before`
- Aggregate EPA/play per team and player (passer EPA/dropback, rusher EPA/carry, etc.)
