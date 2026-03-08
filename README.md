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
| 4. EPA Model | Done | Expected points model, EPA/play, per-player/team aggregation, API |

## Parsed Data Summary

The database contains fully parsed play-by-play data across **59 ISFL seasons**, covering both engine eras:

| | Count |
|--|------:|
| Seasons | 59 |
| Games | 6,256 |
| Plays | 1,233,292 |
| Unique Players | 11,120 |

### Play Type Breakdown

| Play Type | Count | Pct |
|-----------|------:|----:|
| pass | 521,467 | 42.3% |
| rush | 364,093 | 29.5% |
| kickoff | 93,398 | 7.6% |
| punt | 76,706 | 6.2% |
| penalty | 50,076 | 4.1% |
| sack | 45,810 | 3.7% |
| quarter_marker | 33,588 | 2.7% |
| field_goal | 27,933 | 2.3% |
| timeout | 9,973 | 0.8% |
| spike | 5,306 | 0.4% |
| kneel | 4,895 | 0.4% |

### Per-Season Summary

| Season | Games | Plays | Pass | Rush | TDs | INTs |
|-------:|------:|------:|-----:|-----:|----:|-----:|
| 1 | 58 | 11,013 | 3,861 | 3,383 | 204 | 113 |
| 2 | 75 | 14,565 | 5,425 | 4,215 | 296 | 173 |
| 3 | 75 | 14,617 | 5,512 | 4,227 | 365 | 192 |
| 4 | 75 | 14,587 | 5,888 | 3,978 | 369 | 182 |
| 5 | 76 | 14,463 | 6,272 | 3,644 | 443 | 181 |
| 6 | 75 | 14,568 | 6,034 | 3,891 | 463 | 227 |
| 7 | 75 | 14,641 | 6,264 | 3,645 | 417 | 145 |
| 8 | 75 | 14,437 | 6,304 | 3,758 | 455 | 141 |
| 9 | 75 | 14,461 | 6,124 | 3,939 | 435 | 165 |
| 10 | 75 | 14,552 | 6,064 | 4,079 | 448 | 139 |
| 11 | 75 | 14,586 | 6,171 | 4,109 | 468 | 164 |
| 12 | 75 | 14,317 | 5,904 | 4,223 | 462 | 151 |
| 13 | 74 | 13,969 | 5,913 | 3,917 | 467 | 152 |
| 14 | 75 | 14,214 | 5,985 | 4,038 | 453 | 110 |
| 15 | 75 | 14,208 | 5,773 | 4,271 | 437 | 131 |
| 16 | 92 | 17,572 | 6,802 | 5,592 | 529 | 193 |
| 17 | 92 | 17,457 | 7,111 | 5,289 | 512 | 151 |
| 18 | 92 | 16,960 | 6,954 | 5,017 | 515 | 166 |
| 19 | 92 | 17,167 | 6,668 | 5,477 | 499 | 157 |
| 20 | 91 | 17,258 | 6,702 | 5,490 | 501 | 151 |
| 21 | 92 | 17,300 | 6,589 | 5,730 | 504 | 105 |
| 22 | 109 | 20,359 | 7,264 | 7,164 | 497 | 143 |
| 23 | 127 | 23,209 | 8,365 | 8,098 | 593 | 191 |
| 24 | 127 | 23,260 | 8,707 | 7,893 | 622 | 211 |
| 25 | 147 | 27,811 | 10,665 | 9,016 | 771 | 271 |
| 26 | 147 | 27,424 | 10,703 | 8,929 | 823 | 236 |
| 27 | 147 | 24,179 | 10,981 | 6,384 | 793 | 241 |
| 28 | 147 | 24,406 | 11,118 | 6,699 | 786 | 244 |
| 29 | 147 | 24,354 | 11,282 | 6,469 | 853 | 249 |
| 30 | 147 | 24,316 | 11,284 | 6,429 | 851 | 232 |
| 31 | 147 | 24,190 | 11,116 | 6,525 | 839 | 261 |
| 32 | 147 | 24,427 | 10,903 | 6,950 | 856 | 245 |
| 33 | 147 | 24,219 | 11,036 | 6,661 | 884 | 222 |
| 34 | 147 | 24,139 | 10,719 | 6,715 | 885 | 284 |
| 35 | 147 | 24,146 | 10,382 | 7,240 | 866 | 247 |
| 36 | 147 | 23,989 | 10,228 | 7,318 | 820 | 250 |
| 37 | 147 | 24,271 | 10,340 | 7,343 | 825 | 268 |
| 38 | 147 | 24,367 | 10,797 | 6,920 | 852 | 254 |
| 39 | 147 | 24,329 | 10,684 | 6,991 | 853 | 270 |
| 40 | 147 | 24,499 | 11,107 | 6,650 | 853 | 242 |
| 41 | 147 | 24,335 | 11,015 | 6,662 | 770 | 256 |
| 42 | 147 | 24,286 | 10,861 | 6,741 | 827 | 253 |
| 43 | 147 | 24,280 | 10,964 | 6,696 | 826 | 237 |
| 44 | 147 | 24,158 | 11,065 | 6,560 | 810 | 250 |
| 45 | 147 | 24,311 | 11,223 | 6,329 | 840 | 240 |
| 46 | 147 | 24,305 | 10,954 | 6,633 | 789 | 236 |
| 47 | 147 | 24,518 | 11,105 | 6,654 | 805 | 272 |
| 48 | 147 | 24,259 | 10,712 | 6,932 | 853 | 253 |
| 49 | 147 | 24,426 | 10,193 | 7,687 | 764 | 223 |
| 50 | 147 | 24,635 | 10,657 | 7,373 | 836 | 258 |
| 51 | 147 | 24,438 | 10,234 | 7,544 | 817 | 260 |
| 52 | 147 | 24,291 | 10,219 | 7,594 | 794 | 215 |
| 53 | 147 | 23,951 | 9,573 | 7,935 | 777 | 190 |
| 54 | 146 | 23,911 | 9,870 | 7,597 | 786 | 212 |
| 55 | 146 | 23,968 | 9,850 | 7,775 | 795 | 230 |
| 56 | 146 | 23,904 | 9,867 | 7,595 | 790 | 207 |
| 57 | 147 | 23,952 | 9,797 | 7,599 | 781 | 225 |
| 58 | 147 | 24,099 | 10,120 | 7,628 | 788 | 242 |
| 59 | 112 | 18,459 | 7,187 | 6,253 | 617 | 153 |

Run `uv run isfl-epa summary` to regenerate this from the live database, or `--season 50` for a single season.

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

# View parsed data summary
uv run isfl-epa summary
uv run isfl-epa summary --season 50

# Train era-specific EP models (drive-outcome regression)
uv run isfl-epa train-ep --era both

# Train with a specific model type (hgb_reg, hgb, or logistic)
uv run isfl-epa train-ep --era both --model-type hgb_reg

# Compute EPA for a season
uv run isfl-epa compute-epa --season 50

# View EPA leaders
uv run isfl-epa epa-stats --season 50 --stat passing --top 10
uv run isfl-epa epa-stats --season 50 --stat team

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
  epa/
    score_reconstruct.py # Reconstruct S1-26 per-play scores from scoring events
    dataset.py         # Drive-outcome labeling, feature matrix for EP model
    model.py           # EPModel: train, predict, save/load (HGB regressor + classifier)
    calculator.py      # EPA computation via next-play lookahead (drive + half-score modes)
    models.py          # Pydantic models (PlayEPA, PlayerEPASeason, TeamEPASeason)
tests/
  test_parser.py              # 46 unit tests covering all play types and edge cases
  cross_validate_test.py      # 16 tests cross-validating parsed stats vs boxscores
  stats_test.py               # 29 unit tests for stats aggregation
  stats_cross_validate_test.py # 8 tests validating stats vs boxscores
  registry_test.py            # 14 tests for player registry
  storage_test.py             # Parquet + PostgreSQL round-trip tests
  api_test.py                 # 11 FastAPI endpoint tests
  epa_test.py                 # 44 tests for EPA pipeline (drive labeling, model, calculator)
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
| `GET /epa/passing-leaders?season=&top=&min_dropbacks=` | EPA passing leaders |
| `GET /epa/rushing-leaders?season=&top=&min_attempts=` | EPA rushing leaders |
| `GET /epa/receiving-leaders?season=&top=&min_targets=` | EPA receiving leaders |
| `GET /epa/defensive-leaders?season=&top=&min_plays=` | EPA defensive leaders |
| `GET /epa/team?season=` | Team EPA rankings |
| `GET /epa/team-dashboard?season=&side=` | Team dashboard (offensive/defensive) |
| `GET /epa/player/{id}?season=` | Player EPA profile |
| `GET /epa/game/{game_id}` | Play-level EPA for a game |
| `GET /epa/leaderboard?category=&season=&mode=` | Player leaderboard with EPA + traditional stats |
| `GET /epa/seasons` | Seasons with EPA data |
| `GET /epa/teams?season=` | Teams, optionally filtered by season |
| `GET /epa/positions` | Distinct player positions |

Full OpenAPI docs at `http://localhost:8000/docs`.

## EPA Model

The EPA (Expected Points Added) model quantifies the value of every play beyond traditional counting stats.

### Methodology

1. **Drive-outcome labeling**: Each play is labeled with the actual points scored on its current drive. Drive boundaries are detected by possession changes, half changes, and scoring events. Point values use actual outcomes:
   - TD + PAT good: **+7** | TD + PAT miss: **+6**
   - Defensive TD (pick-6, fumble-return TD) + PAT good: **-7** | + PAT miss: **-6**
   - Field goal: **+3** | Safety: **-2**
   - All other drive endings (punt, turnover, end of half, missed FG, turnover on downs): **0**

2. **EP model**: A `HistGradientBoostingRegressor` directly predicts expected points from game state. Trained on drive-start plays only for better calibration at drive boundaries. Two era-specific models handle engine differences:
   - **S1–26 model** (2016 engine) → `data/models/ep_model_2016.joblib`
   - **S27–59 model** (2022 engine) → `data/models/ep_model_2022.joblib`

3. **Features** (7 per era): down, distance, yardline_100, score_differential, half_seconds_remaining, is_home, is_overtime

4. **EPA calculation**: Uses next-play lookahead: `EPA = EP_after - EP_before`.
   - **Scrimmage plays only**: pass, rush, sack, field_goal (kickoffs, punts, penalties, kneels, spikes are excluded)
   - **Scoring plays**: TD = ±(6 + pat_bonus), FG = +3, safety = -2
   - **Possession change**: EP_after = 0 (drive ended without scoring)
   - **Same drive**: EP_after = next play's EP_before (ensures EPA telescopes correctly: total drive EPA = drive_outcome - EP_first_play)
   - **Defensive TDs**: pick-6 and fumble-return TDs produce negative EP_after with actual PAT results

5. **Score reconstruction**: S1-26 HTML-era games lack per-play scores. These are reconstructed from scoring events (TDs, FGs, safeties, PATs) detected during parsing.

### Training

- **Train set**: S1-23 + S27-56 (both engine eras, drive-start plays only)
- **Test set**: S24-26 + S57-59 (holdout from each era)
- Era-specific models avoid the need for an `engine_era` feature
- Mean EPA per game ≈ -0.04 (near zero, confirming proper calibration)
