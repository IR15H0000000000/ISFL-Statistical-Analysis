"""Cross-validate stats aggregation against boxscore data.

Uses the same seasons and thresholds as cross_validate_test.py but validates
through the real aggregation module rather than ad-hoc counting.
"""

import pytest

from isfl_epa.config import ENGINE_CUTOFF_SEASON, League
from isfl_epa.parser.play_parser import parse_game
from isfl_epa.scraper.boxscore import fetch_all_season_boxscores
from isfl_epa.scraper.boxscore_html import fetch_all_season_boxscores_html
from isfl_epa.scraper.pbp import fetch_all_season_pbp
from isfl_epa.scraper.pbp_html import fetch_all_season_pbp_html
from isfl_epa.stats.aggregation import game_team_stats

SEASONS = [25, 26, 27, 28, 35, 45, 50, 59]


def _fetch_pbp(season):
    if season < ENGINE_CUTOFF_SEASON:
        return fetch_all_season_pbp_html(League.ISFL, season)
    return fetch_all_season_pbp(League.ISFL, season)


def _fetch_boxscores(season):
    if season < ENGINE_CUTOFF_SEASON:
        return fetch_all_season_boxscores_html(League.ISFL, season)
    return fetch_all_season_boxscores(League.ISFL, season)


def _box_stats(box):
    """Extract combined (away + home) stats from boxscore."""
    return {
        "pass_yards": int(box.get("aPassing", 0)) + int(box.get("hPassing", 0)),
        "pass_comp": int(box.get("aComp", 0)) + int(box.get("hComp", 0)),
        "pass_att": int(box.get("aAtt", 0)) + int(box.get("hAtt", 0)),
        "rush_yards": int(box.get("aRushing", 0)) + int(box.get("hRushing", 0)),
        "rush_att": int(box.get("aRushes", 0)) + int(box.get("hRushes", 0)),
    }


def _agg_stats(team_games):
    """Combine home + away TeamGame rows into combined game stats."""
    return {
        "pass_yards": sum(t.pass_yards for t in team_games),
        "pass_comp": sum(t.pass_comp for t in team_games),
        "pass_att": sum(t.pass_att for t in team_games),
        "rush_yards": sum(t.rush_yards for t in team_games),
        "rush_att": sum(t.rush_att for t in team_games),
    }


@pytest.mark.parametrize("season", SEASONS)
def test_team_stats_vs_boxscore(season):
    """Per-stat match rate should meet thresholds when comparing aggregation vs boxscore."""
    raw_games = _fetch_pbp(season)
    boxscores = _fetch_boxscores(season)
    box_by_id = {b["id"]: b for b in boxscores}

    compared = 0
    stat_keys = ["pass_yards", "pass_comp", "pass_att", "rush_yards", "rush_att"]
    stat_matches = {k: 0 for k in stat_keys}
    mismatches = []

    for raw_game in raw_games:
        gid = raw_game["id"]
        box = box_by_id.get(gid)
        if not box:
            continue

        game = parse_game(raw_game, season, "ISFL")
        team_games = game_team_stats(game)
        if not team_games:
            continue

        agg = _agg_stats(team_games)
        bs = _box_stats(box)
        compared += 1

        game_mm = []
        for stat in stat_keys:
            if agg[stat] == bs[stat]:
                stat_matches[stat] += 1
            else:
                game_mm.append(f"{stat}({agg[stat]} v {bs[stat]})")
        if game_mm:
            mismatches.append((gid, game_mm))

    print(f"\n  S{season}: compared {compared} games")
    for stat, count in stat_matches.items():
        pct = 100 * count / compared if compared else 0
        print(f"    {stat:>12}: {count}/{compared} ({pct:.1f}%)")

    if mismatches:
        print(f"  Games with any mismatch: {len(mismatches)}/{compared}")
        for gid, mm in mismatches[:5]:
            print(f"    game {gid}: {', '.join(mm)}")
        if len(mismatches) > 5:
            print(f"    ... and {len(mismatches) - 5} more")

    threshold = 60 if season < ENGINE_CUTOFF_SEASON else 92
    for stat, count in stat_matches.items():
        pct = 100 * count / compared if compared else 100
        assert pct >= threshold, (
            f"S{season} {stat}: only {count}/{compared} games match "
            f"({pct:.1f}% < {threshold}%)"
        )
