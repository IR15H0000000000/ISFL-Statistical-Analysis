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
| 2. Play Parsing | Done | Regex-based parser — 99.98% parse rate, boxscore-validated |
| 3. Stats Aggregation | Not started | Player and team stat calculations |
| 4. EPA Model | Not started | Expected points model + EPA/play |

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync              # install dependencies
uv sync --all-extras # include dev dependencies (jupyter, pytest, etc.)
```

## Usage

### CLI

```bash
# Download and cache PBP + boxscore data for a season
uv run isfl-epa scrape --league ISFL --season 50

# Dump raw play-by-play JSON for a specific game
uv run isfl-epa explore --league ISFL --season 50 --game-id 9630
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
  cli.py             # Typer CLI (scrape, explore)
  scraper/
    pbp.py           # Fetch + decompress S27+ JSON PBP data
    pbp_html.py      # Fetch + parse S1-26 HTML PBP pages
    boxscore.py      # Fetch + decompress boxscore data
    cache.py         # Local JSON file cache
  parser/
    schema.py        # Pydantic models (PlayType, ParsedPlay, Game)
    play_parser.py   # Regex pipeline: structured fields + play descriptions
  stats/             # (Phase 3) Player and team stat aggregation
  epa/               # (Phase 4) Expected points model + EPA calculation
tests/
  test_parser.py     # 43 tests covering all play types and edge cases
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

Supported play types: pass (complete/incomplete/intercepted/throwaway), rush, sack, punt, kickoff (touchback/return/onside), field goal, kneel, spike, penalty (nullifying/standalone), timeout, quarter markers.

**Validation:** 99.98% parse rate on S50 (24,631/24,635 plays). Pass yards, completions, attempts, rush yards, and rush attempts all match boxscore data exactly.

## Roadmap

### Phase 3: Stats Aggregation
- Per-player stats: passing, rushing, receiving, defensive
- Per-team stats: total offense/defense, turnover diff, 3rd down rate
- Cross-validation against boxscore data

### Phase 4: EPA Model
- Train an Expected Points model on historical ISFL data (separate from NFL — different sim distributions)
- Calculate EPA per play: `EPA = EP_after - EP_before`
- Aggregate EPA/play per team and player (passer EPA/dropback, rusher EPA/carry, etc.)
