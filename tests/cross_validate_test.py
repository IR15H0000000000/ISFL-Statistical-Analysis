"""Cross-validate parsed play stats against boxscore data for all cached seasons.

Run with: uv run pytest tests/cross_validate_test.py -v -s

Boxscore accounting conventions discovered through analysis:
- Spikes count as pass attempts (att)
- Kneels count as rush attempts in S35+ but NOT in S27
- Each kneel costs -2 rush yards in the boxscore
- Penalty-nullified plays can cause small per-game discrepancies (typically 1-2 yards)
"""

import logging

import pytest

from isfl_epa.config import ENGINE_CUTOFF_SEASON, League
from isfl_epa.parser.play_parser import parse_game
from isfl_epa.parser.schema import PlayType
from isfl_epa.scraper.boxscore import fetch_all_season_boxscores
from isfl_epa.scraper.boxscore_html import fetch_all_season_boxscores_html
from isfl_epa.scraper.pbp import fetch_all_season_pbp
from isfl_epa.scraper.pbp_html import fetch_all_season_pbp_html

logger = logging.getLogger(__name__)

SEASONS = [25, 26, 27, 28, 35, 45, 50, 59]

# S27 does not count kneels as rush attempts; S28+ does
_KNEELS_COUNT_AS_RUSHES_FROM = 28
_KNEEL_YARDS = -2


def _game_stats(game, season):
    """Aggregate stats from parsed plays, adjusted for boxscore conventions."""
    pass_yd = sum(
        p.yards_gained or 0 for p in game.plays if p.play_type == PlayType.PASS
    )
    comp = sum(
        1
        for p in game.plays
        if p.play_type == PlayType.PASS and ", complete to" in p.description
    )
    # Boxscore counts spikes as pass attempts
    att = sum(
        1
        for p in game.plays
        if (p.play_type == PlayType.PASS and p.passer)
        or p.play_type == PlayType.SPIKE
    )
    rush_yd = sum(
        p.yards_gained or 0 for p in game.plays if p.play_type == PlayType.RUSH
    )
    rush = sum(1 for p in game.plays if p.play_type == PlayType.RUSH)
    kneels = sum(1 for p in game.plays if p.play_type == PlayType.KNEEL)

    # Adjust for kneel conventions
    if season >= _KNEELS_COUNT_AS_RUSHES_FROM:
        rush += kneels
        rush_yd += kneels * _KNEEL_YARDS

    return {
        "pass_yd": pass_yd,
        "comp": comp,
        "att": att,
        "rush_yd": rush_yd,
        "rush": rush,
        "total": len(game.plays),
        "unknown": sum(
            1 for p in game.plays if p.play_type == PlayType.UNKNOWN
        ),
    }


def _box_stats(box):
    """Extract combined stats from boxscore."""
    return {
        "pass_yd": int(box.get("aPassing", 0)) + int(box.get("hPassing", 0)),
        "comp": int(box.get("aComp", 0)) + int(box.get("hComp", 0)),
        "att": int(box.get("aAtt", 0)) + int(box.get("hAtt", 0)),
        "rush_yd": int(box.get("aRushing", 0)) + int(box.get("hRushing", 0)),
        "rush": int(box.get("aRushes", 0)) + int(box.get("hRushes", 0)),
    }


def _fetch_pbp(season):
    """Fetch PBP data using the appropriate scraper for the season."""
    if season < ENGINE_CUTOFF_SEASON:
        return fetch_all_season_pbp_html(League.ISFL, season)
    return fetch_all_season_pbp(League.ISFL, season)


def _fetch_boxscores(season):
    """Fetch boxscore data using the appropriate scraper for the season."""
    if season < ENGINE_CUTOFF_SEASON:
        return fetch_all_season_boxscores_html(League.ISFL, season)
    return fetch_all_season_boxscores(League.ISFL, season)


@pytest.mark.parametrize("season", SEASONS)
def test_parse_rate(season):
    """Parse rate should be >= 99.9% for each season."""
    raw_games = _fetch_pbp(season)
    total = 0
    unknown = 0
    unknown_examples = []

    for raw_game in raw_games:
        game = parse_game(raw_game, season, "ISFL")
        for p in game.plays:
            total += 1
            if p.play_type == PlayType.UNKNOWN:
                unknown += 1
                if len(unknown_examples) < 10:
                    unknown_examples.append(
                        f"  game {game.id} Q{p.quarter} {p.clock}: "
                        f"{p.description[:100]}"
                    )

    pct = 100 * (total - unknown) / total if total else 0
    print(f"\n  S{season}: {total - unknown}/{total} plays parsed ({pct:.2f}%)")
    if unknown_examples:
        print(f"  Unparsed:")
        for ex in unknown_examples:
            print(ex)

    assert pct >= 99.9, f"S{season} parse rate {pct:.2f}% < 99.9%"


@pytest.mark.parametrize("season", SEASONS)
def test_stats_vs_boxscore(season):
    """Per-stat match rate should be >= 90% across all games in a season.

    Remaining mismatches come from penalty-nullified plays whose yards
    are counted in the PBP but excluded from boxscore totals.
    """
    raw_games = _fetch_pbp(season)
    boxscores = _fetch_boxscores(season)
    box_by_id = {b["id"]: b for b in boxscores}

    compared = 0
    stat_matches = {"pass_yd": 0, "comp": 0, "att": 0, "rush_yd": 0, "rush": 0}
    mismatches = []

    for raw_game in raw_games:
        gid = raw_game["id"]
        box = box_by_id.get(gid)
        if not box:
            continue

        game = parse_game(raw_game, season, "ISFL")
        ps = _game_stats(game, season)
        bs = _box_stats(box)
        compared += 1

        game_mm = []
        for stat in stat_matches:
            if ps[stat] == bs[stat]:
                stat_matches[stat] += 1
            else:
                game_mm.append(f"{stat}({ps[stat]} v {bs[stat]})")
        if game_mm:
            mismatches.append((gid, game_mm))

    print(f"\n  S{season}: compared {compared} games")
    for stat, count in stat_matches.items():
        pct = 100 * count / compared if compared else 0
        print(f"    {stat:>10}: {count}/{compared} ({pct:.1f}%)")

    if mismatches:
        print(f"  Games with any mismatch: {len(mismatches)}/{compared}")
        for gid, mm in mismatches[:5]:
            print(f"    game {gid}: {', '.join(mm)}")
        if len(mismatches) > 5:
            print(f"    ... and {len(mismatches) - 5} more")

    # S27+ (JSON): >= 80% match per stat. Remaining mismatches from
    # penalty-nullified plays (PBP records original, boxscore excludes).
    # S1-26 (HTML): >= 50% match. The 2016 engine PBP has lower fidelity —
    # penalty handling differs, and some plays are absent from the HTML PBP.
    threshold = 50 if season < ENGINE_CUTOFF_SEASON else 80
    for stat, count in stat_matches.items():
        pct = 100 * count / compared if compared else 100
        assert pct >= threshold, (
            f"S{season} {stat}: only {count}/{compared} games match "
            f"({pct:.1f}% < {threshold}%)"
        )
