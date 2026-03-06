"""Aggregate parsed plays into player and team stat lines.

Accounting conventions match the boxscore validation in cross_validate_test.py:
- 2pt conversions excluded from all stats
- Spikes count as team-level pass att only (no passer field)
- Kneels count as team-level rush att/yards for S28+ only (each = -2 yards)
- Sacks: passer gets sacks_taken, sacker gets defensive credit. Not a pass attempt.
- Completions: ", complete to" in description
"""

from collections import defaultdict

import pandas as pd

from isfl_epa.config import ENGINE_CUTOFF_SEASON
from isfl_epa.parser.schema import Game, ParsedPlay, PlayType
from isfl_epa.stats.models import (
    PlayerDefensive,
    PlayerPassing,
    PlayerReceiving,
    PlayerRushing,
    TeamGame,
)

# S28+ counts kneels as rush attempts; S27 does not
_KNEELS_COUNT_AS_RUSHES_FROM = 28
_KNEEL_YARDS = -2


def _is_two_point(p: ParsedPlay) -> bool:
    return "2 point" in p.description or "conversion" in p.description


def _team_abbr(game: Game, team_id: int | None) -> str | None:
    """Map a possession_team_id to the 3-char team abbreviation."""
    if team_id is None:
        return None
    if team_id == game.home_team_id:
        return game.home_team
    if team_id == game.away_team_id:
        return game.away_team
    return None


def _opponent(game: Game, team: str | None) -> str | None:
    if team == game.home_team:
        return game.away_team
    if team == game.away_team:
        return game.home_team
    return None


# ---------------------------------------------------------------------------
# Per-game player stats
# ---------------------------------------------------------------------------


def game_player_passing(game: Game, registry=None) -> list[PlayerPassing]:
    """Aggregate per-passer passing stats for a single game."""
    stats: dict[str, dict] = defaultdict(lambda: {
        "comp": 0, "att": 0, "yards": 0, "td": 0,
        "interceptions": 0, "sacks": 0, "sack_yards": 0,
    })

    for p in game.plays:
        if _is_two_point(p):
            continue

        if p.play_type == PlayType.PASS and p.passer:
            s = stats[p.passer]
            s["att"] += 1
            if ", complete to" in p.description:
                s["comp"] += 1
                s["yards"] += p.yards_gained or 0
                if p.touchdown:
                    s["td"] += 1
            if p.interception:
                s["interceptions"] += 1

        elif p.play_type == PlayType.SACK and p.passer:
            s = stats[p.passer]
            s["sacks"] += 1
            s["sack_yards"] += p.yards_gained or 0

    result = []
    for player, s in stats.items():
        team = _passer_team(game, player)
        pid = _resolve_player_id(registry, player, game.season, team)
        result.append(PlayerPassing(
            player_id=pid, player=player, team=team,
            game_id=game.id, season=game.season, **s,
        ))
    return result


def game_player_rushing(game: Game, registry=None) -> list[PlayerRushing]:
    """Aggregate per-rusher rushing stats for a single game."""
    stats: dict[str, dict] = defaultdict(lambda: {
        "att": 0, "yards": 0, "td": 0, "fumbles": 0,
    })

    for p in game.plays:
        if _is_two_point(p):
            continue
        if p.play_type == PlayType.RUSH and p.rusher:
            s = stats[p.rusher]
            s["att"] += 1
            s["yards"] += p.yards_gained or 0
            if p.touchdown:
                s["td"] += 1
            if p.fumble_lost:
                s["fumbles"] += 1

    result = []
    for player, s in stats.items():
        team = _rusher_team(game, player)
        pid = _resolve_player_id(registry, player, game.season, team)
        result.append(PlayerRushing(
            player_id=pid, player=player, team=team,
            game_id=game.id, season=game.season, **s,
        ))
    return result


def game_player_receiving(game: Game, registry=None) -> list[PlayerReceiving]:
    """Aggregate per-receiver receiving stats for a single game."""
    stats: dict[str, dict] = defaultdict(lambda: {
        "receptions": 0, "yards": 0, "td": 0, "fumbles": 0,
    })

    for p in game.plays:
        if _is_two_point(p):
            continue
        if p.play_type == PlayType.PASS and p.receiver and ", complete to" in p.description:
            s = stats[p.receiver]
            s["receptions"] += 1
            s["yards"] += p.yards_gained or 0
            if p.touchdown:
                s["td"] += 1
            if p.fumble_lost:
                s["fumbles"] += 1

    result = []
    for player, s in stats.items():
        team = _receiver_team(game, player)
        pid = _resolve_player_id(registry, player, game.season, team)
        result.append(PlayerReceiving(
            player_id=pid, player=player, team=team,
            game_id=game.id, season=game.season, **s,
        ))
    return result


def game_player_defensive(game: Game, registry=None) -> list[PlayerDefensive]:
    """Aggregate per-defender defensive stats for a single game."""
    stats: dict[str, dict] = defaultdict(lambda: {
        "tackles": 0, "sacks": 0.0, "interceptions": 0, "fumble_recoveries": 0,
    })

    for p in game.plays:
        if p.tackler:
            stats[p.tackler]["tackles"] += 1
        if p.sacker:
            stats[p.sacker]["sacks"] += 1.0
        if p.interceptor:
            stats[p.interceptor]["interceptions"] += 1
        if p.fumble_recoverer:
            stats[p.fumble_recoverer]["fumble_recoveries"] += 1

    result = []
    for player, s in stats.items():
        # Defensive players are on the non-possession team, but we don't
        # easily know that per-play. Use None for now; the registry can
        # resolve team from other appearances.
        pid = _resolve_player_id(registry, player, game.season, None)
        result.append(PlayerDefensive(
            player_id=pid, player=player, team=None,
            game_id=game.id, season=game.season, **s,
        ))
    return result


# ---------------------------------------------------------------------------
# Per-game team stats
# ---------------------------------------------------------------------------


def game_team_stats(game: Game) -> list[TeamGame]:
    """Produce two TeamGame rows (home and away) for a single game."""
    home = game.home_team
    away = game.away_team
    if not home or not away:
        return []

    home_id = game.home_team_id
    away_id = game.away_team_id

    # Initialize accumulators for each team
    t: dict[str, dict] = {}
    for team, opp, is_home in [(home, away, True), (away, home, False)]:
        t[team] = {
            "opponent": opp, "is_home": is_home,
            "pass_comp": 0, "pass_att": 0, "pass_yards": 0, "pass_td": 0,
            "rush_att": 0, "rush_yards": 0, "rush_td": 0,
            "first_downs": 0, "interceptions_thrown": 0, "fumbles_lost": 0,
            "third_down_att": 0, "third_down_conv": 0,
            "sacks_taken": 0, "sacks_made": 0,
        }

    for p in game.plays:
        if _is_two_point(p):
            continue

        team = _team_abbr(game, p.possession_team_id)
        if team not in t:
            continue
        opp = t[team]["opponent"]
        s = t[team]

        if p.play_type == PlayType.PASS:
            if p.passer:
                s["pass_att"] += 1
            if ", complete to" in p.description:
                s["pass_comp"] += 1
                s["pass_yards"] += p.yards_gained or 0
                if p.touchdown:
                    s["pass_td"] += 1
            if p.interception:
                s["interceptions_thrown"] += 1

        elif p.play_type == PlayType.SPIKE:
            s["pass_att"] += 1

        elif p.play_type == PlayType.RUSH:
            s["rush_att"] += 1
            s["rush_yards"] += p.yards_gained or 0
            if p.touchdown:
                s["rush_td"] += 1

        elif p.play_type == PlayType.SACK:
            s["sacks_taken"] += 1
            if opp in t:
                t[opp]["sacks_made"] += 1

        if p.fumble_lost:
            s["fumbles_lost"] += 1
        if p.first_down:
            s["first_downs"] += 1

        # Third down tracking
        if p.down == 3 and p.play_type in (
            PlayType.PASS, PlayType.RUSH, PlayType.SACK,
        ):
            s["third_down_att"] += 1
            if p.first_down or p.touchdown:
                s["third_down_conv"] += 1

    # Kneel adjustments
    if game.season >= _KNEELS_COUNT_AS_RUSHES_FROM:
        for p in game.plays:
            if p.play_type == PlayType.KNEEL:
                team = _team_abbr(game, p.possession_team_id)
                if team in t:
                    t[team]["rush_att"] += 1
                    t[team]["rush_yards"] += _KNEEL_YARDS

    # Extract final score from last play with score data
    points = _extract_final_score(game)

    result = []
    for team_abbr, s in t.items():
        pf = points.get(team_abbr, 0)
        pa = points.get(s["opponent"], 0)
        total_yards = s["pass_yards"] + s["rush_yards"]
        turnovers = s["interceptions_thrown"] + s["fumbles_lost"]
        opp = s.pop("opponent")
        is_home = s.pop("is_home")
        result.append(TeamGame(
            game_id=game.id, season=game.season,
            team=team_abbr, opponent=opp, is_home=is_home,
            points_for=pf, points_against=pa,
            total_yards=total_yards, turnovers=turnovers,
            **s,
        ))
    return result


# ---------------------------------------------------------------------------
# Season-level aggregation
# ---------------------------------------------------------------------------


def season_player_stats(
    games: list[Game],
    category: str,
    registry=None,
    totals: bool = True,
) -> pd.DataFrame:
    """Aggregate player stats across a list of games.

    Args:
        games: Parsed games for the season.
        category: One of "passing", "rushing", "receiving", "defensive".
        registry: Optional PlayerRegistry for ID resolution.
        totals: If True, group by player and sum. If False, return game logs.

    Returns:
        DataFrame with one row per player (totals) or per player-game (logs).
    """
    func_map = {
        "passing": game_player_passing,
        "rushing": game_player_rushing,
        "receiving": game_player_receiving,
        "defensive": game_player_defensive,
    }
    func = func_map[category]
    rows = []
    for game in games:
        rows.extend(s.model_dump() for s in func(game, registry))

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if not totals:
        return df

    # Group by player (and team) for season totals
    group_cols = ["player_id", "player", "team"]
    sum_cols = [c for c in df.columns if c not in group_cols + ["game_id", "season"]]
    season = games[0].season if games else 0

    agg = df.groupby(group_cols, dropna=False)[sum_cols].sum().reset_index()
    agg["season"] = season
    agg["game_id"] = 0  # sentinel for season totals
    return agg.sort_values(sum_cols[1] if len(sum_cols) > 1 else sum_cols[0], ascending=False)


def season_team_stats(games: list[Game]) -> pd.DataFrame:
    """Aggregate team stats across a list of games into a DataFrame."""
    rows = []
    for game in games:
        rows.extend(s.model_dump() for s in game_team_stats(game))

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_final_score(game: Game) -> dict[str, int]:
    """Get final score from the last play with score data."""
    for p in reversed(game.plays):
        if p.score_away is not None and p.score_home is not None:
            return {
                game.away_team: p.score_away,
                game.home_team: p.score_home,
            }
    return {}


def _passer_team(game: Game, passer: str) -> str | None:
    """Find team for a passer by looking at possession on their plays."""
    for p in game.plays:
        if p.passer == passer and p.possession_team_id is not None:
            return _team_abbr(game, p.possession_team_id)
    return None


def _rusher_team(game: Game, rusher: str) -> str | None:
    for p in game.plays:
        if p.rusher == rusher and p.possession_team_id is not None:
            return _team_abbr(game, p.possession_team_id)
    return None


def _receiver_team(game: Game, receiver: str) -> str | None:
    for p in game.plays:
        if p.receiver == receiver and p.possession_team_id is not None:
            return _team_abbr(game, p.possession_team_id)
    return None


def _resolve_player_id(registry, name: str, season: int, team: str | None) -> int | None:
    if registry is None:
        return None
    return registry.get_or_create(name, season, team)
