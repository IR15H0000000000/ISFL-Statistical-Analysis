"""EPA query endpoints."""

import time

from fastapi import APIRouter, Query, Request
from sqlalchemy import Integer, and_, case, cast, desc, distinct, exists, func, literal_column, or_, select, Float

# Server-side cache: (season, side, game_type) -> (timestamp, data)
_dashboard_cache: dict[tuple, tuple[float, list[dict]]] = {}
_CACHE_TTL = 300  # 5 minutes

# Visualization cache: (endpoint, season_min, season_max, game_type) -> (timestamp, data)
_viz_cache: dict[tuple, tuple[float, object]] = {}
_VIZ_CACHE_TTL = 3600  # 1 hour (data only changes on rebuild)


def _viz_cached(key: tuple, compute_fn):
    """Return cached result or compute and cache it."""
    cached = _viz_cache.get(key)
    if cached and time.time() - cached[0] < _VIZ_CACHE_TTL:
        return cached[1]
    result = compute_fn()
    _viz_cache[key] = (time.time(), result)
    return result


def invalidate_viz_cache():
    """Clear all viz cache entries. Call after data rebuild/EPA recompute."""
    _viz_cache.clear()
    _dashboard_cache.clear()

from isfl_epa.storage.database import (
    games_table,
    get_team_id_to_abbr,
    get_team_id_to_abbr_multi,
    get_team_id_to_all_abbrs,
    play_epa_table,
    player_game_defensive_table,
    player_game_passing_table,
    player_game_receiving_table,
    player_game_rushing_table,
    player_positions_table,
    player_season_epa_table,
    plays_table,
    team_games_table,
    team_season_epa_table,
)

router = APIRouter()


def _league_avg_epa(conn, season: int, game_type: str, play_types: list[str]) -> float | None:
    """League-average EPA per play for the given play types in a season."""
    stmt = (
        select(func.avg(play_epa_table.c.epa))
        .join(plays_table, plays_table.c.id == play_epa_table.c.play_id)
        .where(plays_table.c.season == season)
        .where(plays_table.c.game_type == game_type)
        .where(plays_table.c.play_type.in_(play_types))
        .where(play_epa_table.c.epa.isnot(None))
    )
    result = conn.execute(stmt).scalar()
    return float(result) if result is not None else None


def _league_avg_team_epa(conn, season: int, game_type: str, side: str) -> float | None:
    """League-average EPA/play across all teams for the given side."""
    t = team_season_epa_table
    stmt = select(
        func.sum(t.c.total_epa) / func.nullif(func.sum(t.c.plays), 0)
    ).where(t.c.season == season).where(t.c.game_type == game_type).where(t.c.side == side)
    result = conn.execute(stmt).scalar()
    return float(result) if result is not None else None


def _add_epa_plus(rows: list[dict], avg: float | None, epa_per_key: str) -> None:
    """Mutate rows in-place to add epa_plus column."""
    for row in rows:
        epa_per = row.get(epa_per_key)
        row["epa_plus"] = round((epa_per - avg) * 100, 1) if (epa_per is not None and avg is not None) else None


def _league_avg_epa_by_position(conn, season: int, game_type: str, epa_col: str, plays_col: str) -> dict[str, float]:
    """Weighted avg EPA/play by position for a season.
    Returns dict {position: avg_epa_per_play}."""
    epa_t = player_season_epa_table
    pp = player_positions_table
    stmt = (
        select(
            pp.c.position,
            (func.sum(epa_t.c[epa_col]) / func.nullif(func.sum(epa_t.c[plays_col]), 0)).label("avg_epa"),
        )
        .join(pp, (epa_t.c.player_id == pp.c.player_id) & (epa_t.c.season == pp.c.season))
        .where(epa_t.c.season == season)
        .where(epa_t.c.game_type == game_type)
        .where(epa_t.c[plays_col] > 0)
        .group_by(pp.c.position)
    )
    return {row.position: float(row.avg_epa) for row in conn.execute(stmt) if row.avg_epa is not None}


def _add_epa_plus_by_position(rows: list[dict], avg_by_pos: dict[str, float], epa_per_key: str, fallback_avg: float | None = None) -> None:
    """Mutate rows in-place: epa_plus uses position-specific baseline."""
    for row in rows:
        epa_per = row.get(epa_per_key)
        pos = row.get("position", "")
        avg = avg_by_pos.get(pos, fallback_avg)
        row["epa_plus"] = round((epa_per - avg) * 100, 1) if (epa_per is not None and avg is not None) else None


@router.get("/passing-leaders")
def epa_passing_leaders(
    request: Request,
    season: int = Query(...),
    top: int = Query(default=20, le=100),
    min_dropbacks: int = Query(default=100),
    game_type: str = Query(default="regular"),
):
    """Top passers by EPA/dropback."""
    t = player_season_epa_table
    engine = request.app.state.engine
    stmt = (
        select(t)
        .where(t.c.season == season)
        .where(t.c.game_type == game_type)
        .where(t.c.dropbacks >= min_dropbacks)
        .order_by(desc(t.c.epa_per_dropback))
        .limit(top)
    )
    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(stmt)]
        avg = _league_avg_epa(conn, season, game_type, ["pass", "sack"])
        _add_epa_plus(rows, avg, "epa_per_dropback")
        return rows


@router.get("/rushing-leaders")
def epa_rushing_leaders(
    request: Request,
    season: int = Query(...),
    top: int = Query(default=20, le=100),
    min_attempts: int = Query(default=50),
    game_type: str = Query(default="regular"),
):
    """Top rushers by EPA/rush."""
    t = player_season_epa_table
    engine = request.app.state.engine
    stmt = (
        select(t)
        .where(t.c.season == season)
        .where(t.c.game_type == game_type)
        .where(t.c.rush_attempts >= min_attempts)
        .order_by(desc(t.c.epa_per_rush))
        .limit(top)
    )
    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(stmt)]
        avg = _league_avg_epa(conn, season, game_type, ["rush"])
        _add_epa_plus(rows, avg, "epa_per_rush")
        return rows


@router.get("/receiving-leaders")
def epa_receiving_leaders(
    request: Request,
    season: int = Query(...),
    top: int = Query(default=20, le=100),
    min_targets: int = Query(default=30),
    game_type: str = Query(default="regular"),
):
    """Top receivers by EPA/target."""
    t = player_season_epa_table
    engine = request.app.state.engine
    stmt = (
        select(t)
        .where(t.c.season == season)
        .where(t.c.game_type == game_type)
        .where(t.c.targets >= min_targets)
        .order_by(desc(t.c.epa_per_target))
        .limit(top)
    )
    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(stmt)]
        avg = _league_avg_epa(conn, season, game_type, ["pass"])
        _add_epa_plus(rows, avg, "epa_per_target")
        return rows


@router.get("/defensive-leaders")
def epa_defensive_leaders(
    request: Request,
    season: int = Query(...),
    top: int = Query(default=20, le=100),
    min_plays: int = Query(default=50),
    game_type: str = Query(default="regular"),
):
    """Top defenders by EPA/play (lower = better)."""
    t = player_season_epa_table
    engine = request.app.state.engine
    stmt = (
        select(t)
        .where(t.c.season == season)
        .where(t.c.game_type == game_type)
        .where(t.c.def_plays >= min_plays)
        .order_by(t.c.epa_per_def_play)
        .limit(top)
    )
    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(stmt)]
        avg = _league_avg_epa(conn, season, game_type, ["pass", "rush", "sack"])
        _add_epa_plus(rows, avg, "epa_per_def_play")
        return rows


@router.get("/team")
def epa_team_stats(
    request: Request,
    season: int = Query(...),
    game_type: str = Query(default="regular"),
):
    """Team EPA rankings."""
    t = team_season_epa_table
    engine = request.app.state.engine
    stmt = (
        select(t)
        .where(t.c.season == season)
        .where(t.c.game_type == game_type)
        .order_by(desc(t.c.epa_per_play))
    )
    with engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(stmt)]


@router.get("/player/{player_id}")
def player_epa_profile(
    request: Request,
    player_id: int,
    season: int | None = None,
    game_type: str = Query(default="regular"),
):
    """Player EPA summary, optionally filtered by season."""
    t = player_season_epa_table
    engine = request.app.state.engine
    stmt = select(t).where(t.c.player_id == player_id).where(t.c.game_type == game_type)
    if season is not None:
        stmt = stmt.where(t.c.season == season)
    stmt = stmt.order_by(t.c.season)
    with engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(stmt)]


@router.get("/game/{game_id}")
def game_epa(
    request: Request,
    game_id: int,
):
    """All play-level EPA for a game."""
    engine = request.app.state.engine
    stmt = (
        select(
            plays_table.c.id,
            plays_table.c.play_index,
            plays_table.c.quarter,
            plays_table.c.clock,
            plays_table.c.play_type,
            plays_table.c.description,
            plays_table.c.possession_team_id,
            plays_table.c.down,
            plays_table.c.distance,
            plays_table.c.yard_line,
            plays_table.c.yard_line_team,
            plays_table.c.yards_gained,
            plays_table.c.home_team,
            plays_table.c.away_team,
            plays_table.c.touchdown,
            plays_table.c.interception,
            plays_table.c.fumble_lost,
            plays_table.c.safety,
            plays_table.c.first_down,
            plays_table.c.fg_good,
            plays_table.c.passer,
            plays_table.c.rusher,
            plays_table.c.receiver,
            plays_table.c.penalty,
            plays_table.c.penalty_team,
            plays_table.c.penalty_type,
            play_epa_table.c.ep_before,
            play_epa_table.c.ep_after,
            play_epa_table.c.epa,
        )
        .join(play_epa_table, plays_table.c.id == play_epa_table.c.play_id, isouter=True)
        .where(plays_table.c.game_id == game_id)
        .order_by(plays_table.c.play_index)
    )
    with engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(stmt)]


@router.get("/games")
def list_games(
    request: Request,
    season: int = Query(...),
    game_type: str = Query(default="regular"),
):
    """List games for a season with team names and scores."""
    engine = request.app.state.engine
    home = (
        select(
            team_games_table.c.game_id,
            team_games_table.c.team.label("home_team"),
            team_games_table.c.points_for.label("home_score"),
        )
        .where(
            and_(
                team_games_table.c.season == season,
                team_games_table.c.game_type == game_type,
                team_games_table.c.is_home == True,  # noqa: E712
            )
        )
        .subquery("home")
    )
    away = (
        select(
            team_games_table.c.game_id,
            team_games_table.c.team.label("away_team"),
            team_games_table.c.points_for.label("away_score"),
        )
        .where(
            and_(
                team_games_table.c.season == season,
                team_games_table.c.game_type == game_type,
                team_games_table.c.is_home == False,  # noqa: E712
            )
        )
        .subquery("away")
    )
    stmt = (
        select(
            home.c.game_id,
            home.c.home_team,
            away.c.away_team,
            home.c.home_score,
            away.c.away_score,
        )
        .join(away, home.c.game_id == away.c.game_id)
        .order_by(home.c.game_id)
    )
    with engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(stmt)]


@router.get("/seasons")
def available_seasons(
    request: Request,
    game_type: str = Query(default="regular"),
):
    """List seasons that have EPA data (regular) or games (playoff), descending."""
    engine = request.app.state.engine
    if game_type == "playoff":
        stmt = (
            select(games_table.c.season)
            .where(games_table.c.game_type == "playoff")
            .distinct()
            .order_by(desc(games_table.c.season))
        )
    else:
        stmt = (
            select(team_season_epa_table.c.season)
            .where(team_season_epa_table.c.game_type == game_type)
            .distinct()
            .order_by(desc(team_season_epa_table.c.season))
        )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(stmt)]


def _success_check():
    """SQL expression for EPA-based success rate.

    A play is successful if EPA > 0.
    Requires play_epa table to be joined in the query.
    """
    return play_epa_table.c.epa > 0


@router.get("/team-dashboard")
def team_dashboard(
    request: Request,
    season: int = Query(...),
    side: str = Query(default="offensive"),
    game_type: str = Query(default="regular"),
):
    """Combined team stats for the dashboard visualization."""
    cache_key = (season, side, game_type)
    cached = _dashboard_cache.get(cache_key)
    if cached:
        ts, data = cached
        if time.time() - ts < _CACHE_TTL:
            return data

    engine = request.app.state.engine
    if side == "defensive":
        result = _defensive_dashboard(engine, season, game_type)
    else:
        result = _offensive_dashboard(engine, season, game_type)

    _dashboard_cache[cache_key] = (time.time(), result)
    return result


def _read_team_epa(conn, season: int, game_type: str = "regular", side: str = "offensive") -> dict[str, dict]:
    """Read precomputed team EPA from team_season_epa table.

    Returns {team: {total_epa, pass_epa, rush_epa, plays, epa_per_play, success_rate}}.
    """
    t = team_season_epa_table
    stmt = (
        select(t)
        .where(t.c.season == season)
        .where(t.c.game_type == game_type)
        .where(t.c.side == side)
    )
    return {
        row.team: {
            "total_epa": row.total_epa or 0,
            "pass_epa": row.pass_epa or 0,
            "rush_epa": row.rush_epa or 0,
            "plays": row.plays or 0,
            "epa_per_play": row.epa_per_play or 0,
            "success_rate": row.success_rate or 0,
        }
        for row in conn.execute(stmt)
    }


def _offensive_dashboard(engine, season: int, game_type: str = "regular") -> list[dict]:
    """Build offensive team dashboard data."""
    with engine.connect() as conn:
        # 1. Precomputed EPA + success rate from team_season_epa
        epa_rows = _read_team_epa(conn, season, game_type, side="offensive")
        def_epa_rows = _read_team_epa(conn, season, game_type, side="defensive")
        league_avg_off = _league_avg_team_epa(conn, season, game_type, "offensive")
        league_avg_def = _league_avg_team_epa(conn, season, game_type, "defensive")

        # 2. Traditional stats from team_games
        tg = team_games_table
        trad_stmt = (
            select(
                tg.c.team,
                func.sum(tg.c.pass_comp).label("pass_comp"),
                func.sum(tg.c.pass_att).label("pass_att"),
                func.sum(tg.c.pass_yards).label("pass_yards"),
                func.sum(tg.c.pass_td).label("pass_td"),
                func.sum(tg.c.rush_att).label("rush_att"),
                func.sum(tg.c.rush_yards).label("rush_yards"),
                func.sum(tg.c.rush_td).label("rush_td"),
                func.sum(tg.c.sacks_taken).label("sacks_taken"),
                func.sum(tg.c.interceptions_thrown).label("ints_thrown"),
                func.sum(tg.c.fumbles_lost).label("fumbles_lost"),
                func.sum(tg.c.fumble_recoveries).label("fumble_recoveries"),
                func.sum(tg.c.points_for).label("points_for"),
                func.sum(case((tg.c.points_for > tg.c.points_against, 1), else_=0)).label("wins"),
                func.sum(case((tg.c.points_for < tg.c.points_against, 1), else_=0)).label("losses"),
                func.sum(case((tg.c.points_for == tg.c.points_against, 1), else_=0)).label("ties"),
            )
            .where(tg.c.season == season)
            .where(tg.c.game_type == game_type)
            .group_by(tg.c.team)
        )
        trad_rows = {row.team: row._mapping for row in conn.execute(trad_stmt)}

        # 3. Opponent fumbles_lost grouped by opponent = this team's defensive recoveries
        opp_fl_stmt = (
            select(
                tg.c.opponent.label("team"),
                func.sum(tg.c.fumbles_lost).label("opp_fumbles_lost"),
            )
            .where(tg.c.season == season)
            .where(tg.c.game_type == game_type)
            .group_by(tg.c.opponent)
        )
        opp_fl = {row.team: row.opp_fumbles_lost or 0 for row in conn.execute(opp_fl_stmt)}

        # Merge everything
        results = []
        all_teams = set(epa_rows.keys()) | set(trad_rows.keys())
        for team in sorted(all_teams):
            epa = epa_rows.get(team, {})
            trad = trad_rows.get(team, {})
            pass_att = trad.get("pass_att") or 0
            rush_att = trad.get("rush_att") or 0
            sacks_taken = trad.get("sacks_taken") or 0
            ints_thrown = trad.get("ints_thrown") or 0
            dropbacks = pass_att + sacks_taken
            wins = trad.get("wins") or 0
            losses = trad.get("losses") or 0
            ties = trad.get("ties") or 0
            fl = trad.get("fumbles_lost") or 0
            mixed_fr = trad.get("fumble_recoveries") or 0
            def_fr = opp_fl.get(team, 0)
            off_fr = max(mixed_fr - def_fr, 0)
            fumbles_total = fl + off_fr

            off_epa_per = epa.get("epa_per_play", 0)
            off_epa_plus = round((off_epa_per - league_avg_off) * 100, 1) if league_avg_off is not None else None
            def_epa_per = def_epa_rows.get(team, {}).get("epa_per_play")
            def_epa_plus = round((def_epa_per - league_avg_def) * 100, 1) if (def_epa_per is not None and league_avg_def is not None) else None
            net_epa_plus = round(off_epa_plus - def_epa_plus, 1) if (off_epa_plus is not None and def_epa_plus is not None) else None

            results.append({
                "team": team,
                "record": f"{wins}-{losses}" + (f"-{ties}" if ties else ""),
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "points_for": trad.get("points_for") or 0,
                "epa_per_play": round(off_epa_per, 3),
                "total_epa": round(epa.get("total_epa", 0), 2),
                "off_epa_plus": off_epa_plus,
                "net_epa_plus": net_epa_plus,
                "success_pct": round(epa.get("success_rate", 0) * 100, 1),
                "epa_per_pass": round(epa.get("pass_epa", 0) / dropbacks, 3) if dropbacks else 0,
                "epa_per_rush": round(epa.get("rush_epa", 0) / rush_att, 3) if rush_att else 0,
                "pass_yards": trad.get("pass_yards") or 0,
                "comp_pct": round((trad.get("pass_comp") or 0) / pass_att * 100, 1) if pass_att else 0,
                "pass_td": trad.get("pass_td") or 0,
                "rush_yards": trad.get("rush_yards") or 0,
                "rush_td": trad.get("rush_td") or 0,
                "sack_pct": round(sacks_taken / dropbacks * 100, 1) if dropbacks else 0,
                "int_pct": round(ints_thrown / pass_att * 100, 1) if pass_att else 0,
                "fumbles_lost": fl,
                "off_fumble_recoveries": off_fr,
                "fumbles_total": fumbles_total,
                "fumble_lost_pct": round(fl / fumbles_total * 100, 1) if fumbles_total else 0,
            })

        results.sort(key=lambda r: r["epa_per_play"], reverse=True)
        for i, r in enumerate(results):
            r["rank"] = i + 1
        return results


def _defensive_dashboard(engine, season: int, game_type: str = "regular") -> list[dict]:
    """Build defensive team dashboard data."""
    with engine.connect() as conn:
        # 1. Precomputed defensive EPA + success rate
        def_epa = _read_team_epa(conn, season, game_type, side="defensive")
        league_avg_def = _league_avg_team_epa(conn, season, game_type, "defensive")
        league_avg_off = _league_avg_team_epa(conn, season, game_type, "offensive")
        off_epa = _read_team_epa(conn, season, game_type, side="offensive")

        # 2. Defensive traditional stats: what opponents did against each team
        tg = team_games_table
        def_trad_stmt = (
            select(
                tg.c.opponent.label("team"),
                func.sum(tg.c.pass_comp).label("pass_comp"),
                func.sum(tg.c.pass_att).label("pass_att"),
                func.sum(tg.c.pass_yards).label("pass_yards"),
                func.sum(tg.c.pass_td).label("pass_td"),
                func.sum(tg.c.rush_att).label("rush_att"),
                func.sum(tg.c.rush_yards).label("rush_yards"),
                func.sum(tg.c.rush_td).label("rush_td"),
                func.sum(tg.c.sacks_taken).label("sacks_taken"),
                func.sum(tg.c.interceptions_thrown).label("ints_thrown"),
                func.sum(tg.c.fumbles_lost).label("opp_fumbles_lost"),
            )
            .where(tg.c.season == season)
            .where(tg.c.game_type == game_type)
            .group_by(tg.c.opponent)
        )
        trad_rows = {row.team: row._mapping for row in conn.execute(def_trad_stmt)}

        # 3. Defensive sacks_made, points_against, record from the team's own perspective
        own_def_stmt = (
            select(
                tg.c.team,
                func.sum(tg.c.sacks_made).label("sacks_made"),
                func.sum(tg.c.forced_fumbles).label("forced_fumbles"),
                func.sum(tg.c.points_against).label("points_against"),
                func.sum(case((tg.c.points_for > tg.c.points_against, 1), else_=0)).label("wins"),
                func.sum(case((tg.c.points_for < tg.c.points_against, 1), else_=0)).label("losses"),
                func.sum(case((tg.c.points_for == tg.c.points_against, 1), else_=0)).label("ties"),
            )
            .where(tg.c.season == season)
            .where(tg.c.game_type == game_type)
            .group_by(tg.c.team)
        )
        own_def = {row.team: row._mapping for row in conn.execute(own_def_stmt)}

        results = []
        all_teams = set(def_epa.keys()) | set(trad_rows.keys())
        for team in sorted(all_teams):
            epa_data = def_epa.get(team, {})
            trad = trad_rows.get(team, {})
            own = own_def.get(team, {})
            opp_pass_att = trad.get("pass_att") or 0
            opp_rush_att = trad.get("rush_att") or 0
            opp_sacks_taken = trad.get("sacks_taken") or 0
            opp_ints = trad.get("ints_thrown") or 0
            opp_dropbacks = opp_pass_att + opp_sacks_taken
            sacks_made = own.get("sacks_made") or 0

            wins = own.get("wins") or 0
            losses = own.get("losses") or 0
            ties = own.get("ties") or 0
            ff = own.get("forced_fumbles") or 0
            def_fr = trad.get("opp_fumbles_lost") or 0

            def_epa_per = epa_data.get("epa_per_play", 0)
            def_epa_plus = round((def_epa_per - league_avg_def) * 100, 1) if league_avg_def is not None else None
            team_off_epa_per = off_epa.get(team, {}).get("epa_per_play")
            off_epa_plus = round((team_off_epa_per - league_avg_off) * 100, 1) if (team_off_epa_per is not None and league_avg_off is not None) else None
            net_epa_plus = round(off_epa_plus - def_epa_plus, 1) if (off_epa_plus is not None and def_epa_plus is not None) else None

            results.append({
                "team": team,
                "record": f"{wins}-{losses}" + (f"-{ties}" if ties else ""),
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "points_against": own.get("points_against") or 0,
                "epa_per_play": round(def_epa_per, 3),
                "total_epa": round(epa_data.get("total_epa", 0), 2),
                "def_epa_plus": def_epa_plus,
                "net_epa_plus": net_epa_plus,
                "success_pct": round(epa_data.get("success_rate", 0) * 100, 1),
                "epa_per_pass": round(epa_data.get("pass_epa", 0) / opp_dropbacks, 3) if opp_dropbacks else 0,
                "epa_per_rush": round(epa_data.get("rush_epa", 0) / opp_rush_att, 3) if opp_rush_att else 0,
                "pass_yards": trad.get("pass_yards") or 0,
                "comp_pct": round((trad.get("pass_comp") or 0) / opp_pass_att * 100, 1) if opp_pass_att else 0,
                "pass_td": trad.get("pass_td") or 0,
                "rush_yards": trad.get("rush_yards") or 0,
                "rush_td": trad.get("rush_td") or 0,
                "sack_pct": round(sacks_made / opp_dropbacks * 100, 1) if opp_dropbacks else 0,
                "int_pct": round(opp_ints / opp_pass_att * 100, 1) if opp_pass_att else 0,
                "forced_fumbles": ff,
                "def_fumble_recoveries": def_fr,
                "fumble_recovery_pct": round(def_fr / ff * 100, 1) if ff else 0,
            })

        # For defense, lower EPA is better — sort ascending
        results.sort(key=lambda r: r["epa_per_play"])
        for i, r in enumerate(results):
            r["rank"] = i + 1
        return results


def _resolve_possession_team(conn, season: int):
    """Return a dict mapping (game_id, possession_team_id) → team_name.

    Uses intersection-based team_id → abbreviation mapping from
    get_team_id_to_abbr, then expands to per-game mapping.
    """
    engine = conn.engine
    ptid_to_team = get_team_id_to_abbr(engine, season)

    # Build per-game mapping from the global ptid → team map
    p = plays_table
    game_ptids_stmt = (
        select(p.c.game_id, p.c.possession_team_id)
        .where(p.c.season == season)
        .where(p.c.possession_team_id.isnot(None))
        .distinct()
    )
    mapping = {}
    for row in conn.execute(game_ptids_stmt):
        team = ptid_to_team.get(int(row.possession_team_id))
        if team:
            mapping[(row.game_id, row.possession_team_id)] = team

    return mapping


def _compute_success_rate(conn, season: int, offensive: bool = True, game_type: str = "regular") -> dict[str, float]:
    """Compute EPA-based success rate per team.

    A play is successful if EPA > 0.
    If offensive=True, groups by possession team (how successful is the offense).
    If offensive=False, groups by defending team (how successful are opponents).
    """
    mapping = _resolve_possession_team(conn, season)
    if not mapping:
        return {}

    p = plays_table
    e = play_epa_table
    scrimmage_types = ["pass", "rush", "sack"]

    # Fetch all scrimmage plays with EPA values
    stmt = (
        select(
            p.c.game_id,
            p.c.possession_team_id,
            e.c.epa,
        )
        .select_from(p.join(e, p.c.id == e.c.play_id))
        .where(p.c.season == season)
        .where(p.c.game_type == game_type)
        .where(p.c.play_type.in_(scrimmage_types))
        .where(e.c.epa.isnot(None))
    )

    # Compute success per team
    team_total = {}
    team_success = {}
    for row in conn.execute(stmt):
        poss_team = mapping.get((row.game_id, row.possession_team_id))
        if not poss_team:
            continue

        # Determine the defending team for defensive view
        if not offensive:
            game_teams = [
                t for (gid, tid), t in mapping.items()
                if gid == row.game_id and t != poss_team
            ]
            team = game_teams[0] if game_teams else None
            if not team:
                continue
        else:
            team = poss_team

        team_total[team] = team_total.get(team, 0) + 1
        if row.epa > 0:
            team_success[team] = team_success.get(team, 0) + 1

    return {
        team: team_success.get(team, 0) / total
        for team, total in team_total.items()
        if total > 0
    }


_OFFENSIVE_POSITIONS_ORDER = ["QB", "RB", "FB", "WR", "TE", "OL"]
_DEFENSIVE_POSITIONS_ORDER = ["DE", "DT", "LB", "CB", "FS", "SS"]
_OFFENSIVE_POSITIONS = set(_OFFENSIVE_POSITIONS_ORDER)
_DEFENSIVE_POSITIONS = set(_DEFENSIVE_POSITIONS_ORDER)


@router.get("/positions")
def available_positions(
    request: Request,
    side: str | None = Query(default=None),
):
    """List distinct positions, optionally filtered by side (offensive/defensive)."""
    engine = request.app.state.engine
    pp = player_positions_table
    stmt = select(pp.c.position).distinct()
    with engine.connect() as conn:
        db_positions = {row[0] for row in conn.execute(stmt)}
    if side == "offensive":
        return [p for p in _OFFENSIVE_POSITIONS_ORDER if p in db_positions]
    if side == "defensive":
        return [p for p in _DEFENSIVE_POSITIONS_ORDER if p in db_positions]
    # No side filter: offensive order then defensive order then any remaining
    known = _OFFENSIVE_POSITIONS_ORDER + _DEFENSIVE_POSITIONS_ORDER
    result = [p for p in known if p in db_positions]
    result += sorted(db_positions - set(known))
    return result


@router.get("/teams")
def available_teams(
    request: Request,
    season: int | None = Query(default=None),
    game_type: str = Query(default="regular"),
):
    """List distinct teams, optionally filtered by season."""
    engine = request.app.state.engine
    tg = team_games_table
    stmt = select(tg.c.team).distinct().order_by(tg.c.team)
    stmt = stmt.where(tg.c.game_type == game_type)
    if season is not None:
        stmt = stmt.where(tg.c.season == season)
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(stmt)]


@router.get("/leaderboard")
def player_leaderboard(
    request: Request,
    category: str = Query(default="passing"),
    season: int = Query(default=None),
    season_min: int = Query(default=None),
    season_max: int = Query(default=None),
    mode: str = Query(default="season"),
    position: str = Query(default=None),
    team: str = Query(default=None),
    min_plays: int = Query(default=None),
    limit: int = Query(default=50, le=200),
    game_type: str = Query(default="regular"),
):
    """Player leaderboard with EPA + traditional stats.

    Modes:
    - season: single season (requires `season`)
    - career: aggregate across season range into one row per player
    - seasons: individual season rows across season range
    """
    engine = request.app.state.engine

    # Determine season filter
    if mode == "season":
        if season is None:
            return []
        season_list = [season]
    else:
        s_min = season_min or 1
        s_max = season_max or 999
        season_list = None  # means range filter
        season_range = (s_min, s_max)

    with engine.connect() as conn:
        if category == "passing":
            rows = _leaderboard_passing(conn, mode, season_list if mode == "season" else None,
                                        season_range if mode != "season" else None,
                                        position, team, min_plays or 100, limit, game_type)
            if mode == "season" and season is not None:
                avg_by_pos = _league_avg_epa_by_position(conn, season, game_type, "pass_epa", "dropbacks")
                fallback = _league_avg_epa(conn, season, game_type, ["pass", "sack"])
                _add_epa_plus_by_position(rows, avg_by_pos, "epa_per_dropback", fallback)
            return rows
        elif category == "rushing":
            rows = _leaderboard_rushing(conn, mode, season_list if mode == "season" else None,
                                        season_range if mode != "season" else None,
                                        position, team, min_plays or 50, limit, game_type)
            if mode == "season" and season is not None:
                avg_by_pos = _league_avg_epa_by_position(conn, season, game_type, "rush_epa", "rush_attempts")
                fallback = _league_avg_epa(conn, season, game_type, ["rush"])
                _add_epa_plus_by_position(rows, avg_by_pos, "epa_per_rush", fallback)
            return rows
        elif category == "receiving":
            rows = _leaderboard_receiving(conn, mode, season_list if mode == "season" else None,
                                          season_range if mode != "season" else None,
                                          position, team, min_plays or 30, limit, game_type)
            if mode == "season" and season is not None:
                avg_by_pos = _league_avg_epa_by_position(conn, season, game_type, "recv_epa", "targets")
                fallback = _league_avg_epa(conn, season, game_type, ["pass"])
                _add_epa_plus_by_position(rows, avg_by_pos, "epa_per_target", fallback)
            return rows
        elif category == "defense":
            rows = _leaderboard_defense(conn, mode, season_list if mode == "season" else None,
                                        season_range if mode != "season" else None,
                                        position, team, min_plays or 50, limit, game_type)
            if mode == "season" and season is not None:
                avg_by_pos = _league_avg_epa_by_position(conn, season, game_type, "def_epa", "def_plays")
                fallback = _league_avg_epa(conn, season, game_type, ["pass", "rush", "sack"])
                _add_epa_plus_by_position(rows, avg_by_pos, "epa_per_def_play", fallback)
            return rows
        return []


def _season_filter(table, season_list, season_range):
    """Build a WHERE clause for season filtering."""
    if season_list:
        return table.c.season.in_(season_list)
    if season_range:
        return table.c.season.between(season_range[0], season_range[1])
    return True


def _apply_position_filter(stmt, pp, position, epa_table, season_list, season_range):
    """Left join player_positions and optionally filter by position."""
    stmt = stmt.outerjoin(
        pp,
        (epa_table.c.player_id == pp.c.player_id)
        & (_season_filter(pp, season_list, season_range))
    )
    if position:
        stmt = stmt.where(pp.c.position == position)
    return stmt


def _leaderboard_passing(conn, mode, season_list, season_range, position, team, min_plays, limit, game_type="regular"):
    epa_t = player_season_epa_table
    pg = player_game_passing_table
    pp = player_positions_table

    sf_epa = _season_filter(epa_t, season_list, season_range)
    sf_pg = _season_filter(pg, season_list, season_range)
    gt_epa = epa_t.c.game_type == game_type
    gt_pg = pg.c.game_type == game_type

    if mode == "career":
        # Aggregate traditional stats per player
        trad_sub = (
            select(
                pg.c.player_id,
                func.count(func.distinct(pg.c.game_id)).label("games"),
                func.sum(pg.c.comp).label("comp"),
                func.sum(pg.c.att).label("att"),
                func.sum(pg.c.yards).label("yards"),
                func.sum(pg.c.td).label("td"),
                func.sum(pg.c.interceptions).label("interceptions"),
                func.sum(pg.c.sacks).label("sacks"),
            )
            .where(sf_pg)
            .where(gt_pg)
            .where(pg.c.player_id.isnot(None))
            .group_by(pg.c.player_id)
        ).subquery("trad")

        # Aggregate EPA per player
        epa_sub = (
            select(
                epa_t.c.player_id,
                func.max(epa_t.c.player).label("player"),
                func.sum(epa_t.c.pass_epa).label("pass_epa"),
                func.sum(epa_t.c.dropbacks).label("dropbacks"),
            )
            .where(sf_epa)
            .where(gt_epa)
            .where(epa_t.c.dropbacks > 0)
            .where(epa_t.c.player_id.isnot(None))
            .group_by(epa_t.c.player_id)
        ).subquery("epa")

        # Most recent team
        team_sub = (
            select(
                epa_t.c.player_id,
                func.max(epa_t.c.team).label("team"),
            )
            .where(sf_epa)
            .where(gt_epa)
            .where(epa_t.c.dropbacks > 0)
            .group_by(epa_t.c.player_id)
        ).subquery("team_info")

        # Most common position
        pos_sub = (
            select(
                pp.c.player_id,
                pp.c.position,
                func.count().label("cnt"),
            )
            .where(_season_filter(pp, season_list, season_range))
            .group_by(pp.c.player_id, pp.c.position)
        ).subquery("pos_cnt")

        pos_ranked = (
            select(
                pos_sub.c.player_id,
                pos_sub.c.position,
            )
            .distinct(pos_sub.c.player_id)
            .order_by(pos_sub.c.player_id, desc(pos_sub.c.cnt))
        ).subquery("pos_best")

        stmt = (
            select(
                epa_sub.c.player_id,
                epa_sub.c.player,
                team_sub.c.team,
                pos_ranked.c.position,
                trad_sub.c.games,
                trad_sub.c.comp,
                trad_sub.c.att,
                trad_sub.c.yards,
                trad_sub.c.td,
                trad_sub.c.interceptions,
                trad_sub.c.sacks,
                epa_sub.c.pass_epa,
                epa_sub.c.dropbacks,
            )
            .outerjoin(trad_sub, epa_sub.c.player_id == trad_sub.c.player_id)
            .outerjoin(team_sub, epa_sub.c.player_id == team_sub.c.player_id)
            .outerjoin(pos_ranked, epa_sub.c.player_id == pos_ranked.c.player_id)
            .where(epa_sub.c.dropbacks >= min_plays)
        )

        if position:
            stmt = stmt.where(pos_ranked.c.position == position)
        if team:
            stmt = stmt.where(team_sub.c.team == team)

        stmt = stmt.order_by(desc(epa_sub.c.pass_epa / epa_sub.c.dropbacks)).limit(limit)

        rows = conn.execute(stmt).fetchall()
        return [_format_passing_row(r) for r in rows]

    else:
        # season or seasons mode: one row per player-season
        stmt = (
            select(
                epa_t.c.player_id,
                epa_t.c.player,
                epa_t.c.team,
                epa_t.c.season,
                pp.c.position,
                func.sum(pg.c.comp).label("comp"),
                func.sum(pg.c.att).label("att"),
                func.sum(pg.c.yards).label("yards"),
                func.sum(pg.c.td).label("td"),
                func.sum(pg.c.interceptions).label("interceptions"),
                func.sum(pg.c.sacks).label("sacks"),
                func.count(func.distinct(pg.c.game_id)).label("games"),
                epa_t.c.pass_epa,
                epa_t.c.dropbacks,
            )
            .outerjoin(pg, (epa_t.c.player_id == pg.c.player_id) & (epa_t.c.season == pg.c.season) & (pg.c.game_type == game_type))
            .outerjoin(pp, (epa_t.c.player_id == pp.c.player_id) & (epa_t.c.season == pp.c.season))
            .where(sf_epa)
            .where(gt_epa)
            .where(epa_t.c.dropbacks >= min_plays)
            .group_by(
                epa_t.c.player_id, epa_t.c.player, epa_t.c.team, epa_t.c.season,
                pp.c.position, epa_t.c.pass_epa, epa_t.c.dropbacks,
            )
        )

        if position:
            stmt = stmt.where(pp.c.position == position)
        if team:
            stmt = stmt.where(epa_t.c.team == team)

        stmt = stmt.order_by(desc(epa_t.c.pass_epa / epa_t.c.dropbacks)).limit(limit)

        rows = conn.execute(stmt).fetchall()
        results = []
        for r in rows:
            d = _format_passing_row(r)
            d["season"] = r.season
            results.append(d)
        return results


def _format_passing_row(r) -> dict:
    dropbacks = r.dropbacks or 0
    att = r.att or 0
    return {
        "player_id": r.player_id,
        "player": r.player,
        "team": r.team or "",
        "position": r.position or "",
        "games": r.games or 0,
        "comp": r.comp or 0,
        "att": att,
        "comp_pct": round((r.comp or 0) / att * 100, 1) if att else 0,
        "yards": r.yards or 0,
        "yards_per_att": round((r.yards or 0) / att, 1) if att else 0,
        "td": r.td or 0,
        "interceptions": r.interceptions or 0,
        "sacks": r.sacks or 0,
        "pass_epa": round(r.pass_epa or 0, 1),
        "dropbacks": dropbacks,
        "epa_per_dropback": round((r.pass_epa or 0) / dropbacks, 3) if dropbacks else 0,
    }


def _leaderboard_rushing(conn, mode, season_list, season_range, position, team, min_plays, limit, game_type="regular"):
    epa_t = player_season_epa_table
    pg = player_game_rushing_table
    pp = player_positions_table

    sf_epa = _season_filter(epa_t, season_list, season_range)
    sf_pg = _season_filter(pg, season_list, season_range)
    gt_epa = epa_t.c.game_type == game_type
    gt_pg = pg.c.game_type == game_type

    if mode == "career":
        trad_sub = (
            select(
                pg.c.player_id,
                func.count(func.distinct(pg.c.game_id)).label("games"),
                func.sum(pg.c.att).label("att"),
                func.sum(pg.c.yards).label("yards"),
                func.sum(pg.c.td).label("td"),
                func.sum(pg.c.fumbles).label("fumbles"),
            )
            .where(sf_pg)
            .where(gt_pg)
            .where(pg.c.player_id.isnot(None))
            .group_by(pg.c.player_id)
        ).subquery("trad")

        epa_sub = (
            select(
                epa_t.c.player_id,
                func.max(epa_t.c.player).label("player"),
                func.sum(epa_t.c.rush_epa).label("rush_epa"),
                func.sum(epa_t.c.rush_attempts).label("rush_attempts"),
            )
            .where(sf_epa)
            .where(gt_epa)
            .where(epa_t.c.rush_attempts > 0)
            .where(epa_t.c.player_id.isnot(None))
            .group_by(epa_t.c.player_id)
        ).subquery("epa")

        team_sub = (
            select(epa_t.c.player_id, func.max(epa_t.c.team).label("team"))
            .where(sf_epa).where(gt_epa).where(epa_t.c.rush_attempts > 0)
            .group_by(epa_t.c.player_id)
        ).subquery("team_info")

        pos_sub = (
            select(pp.c.player_id, pp.c.position, func.count().label("cnt"))
            .where(_season_filter(pp, season_list, season_range))
            .group_by(pp.c.player_id, pp.c.position)
        ).subquery("pos_cnt")
        pos_ranked = (
            select(pos_sub.c.player_id, pos_sub.c.position)
            .distinct(pos_sub.c.player_id)
            .order_by(pos_sub.c.player_id, desc(pos_sub.c.cnt))
        ).subquery("pos_best")

        stmt = (
            select(
                epa_sub.c.player_id, epa_sub.c.player, team_sub.c.team, pos_ranked.c.position,
                trad_sub.c.games, trad_sub.c.att, trad_sub.c.yards, trad_sub.c.td,
                trad_sub.c.fumbles, epa_sub.c.rush_epa, epa_sub.c.rush_attempts,
            )
            .outerjoin(trad_sub, epa_sub.c.player_id == trad_sub.c.player_id)
            .outerjoin(team_sub, epa_sub.c.player_id == team_sub.c.player_id)
            .outerjoin(pos_ranked, epa_sub.c.player_id == pos_ranked.c.player_id)
            .where(epa_sub.c.rush_attempts >= min_plays)
        )
        if position:
            stmt = stmt.where(pos_ranked.c.position == position)
        if team:
            stmt = stmt.where(team_sub.c.team == team)
        stmt = stmt.order_by(desc(epa_sub.c.rush_epa / epa_sub.c.rush_attempts)).limit(limit)

        return [_format_rushing_row(r) for r in conn.execute(stmt)]

    else:
        stmt = (
            select(
                epa_t.c.player_id, epa_t.c.player, epa_t.c.team, epa_t.c.season,
                pp.c.position,
                func.sum(pg.c.att).label("att"), func.sum(pg.c.yards).label("yards"),
                func.sum(pg.c.td).label("td"), func.sum(pg.c.fumbles).label("fumbles"),
                func.count(func.distinct(pg.c.game_id)).label("games"),
                epa_t.c.rush_epa, epa_t.c.rush_attempts,
            )
            .outerjoin(pg, (epa_t.c.player_id == pg.c.player_id) & (epa_t.c.season == pg.c.season) & (pg.c.game_type == game_type))
            .outerjoin(pp, (epa_t.c.player_id == pp.c.player_id) & (epa_t.c.season == pp.c.season))
            .where(sf_epa).where(gt_epa).where(epa_t.c.rush_attempts >= min_plays)
            .group_by(epa_t.c.player_id, epa_t.c.player, epa_t.c.team, epa_t.c.season,
                      pp.c.position, epa_t.c.rush_epa, epa_t.c.rush_attempts)
        )
        if position:
            stmt = stmt.where(pp.c.position == position)
        if team:
            stmt = stmt.where(epa_t.c.team == team)
        stmt = stmt.order_by(desc(epa_t.c.rush_epa / epa_t.c.rush_attempts)).limit(limit)

        results = []
        for r in conn.execute(stmt):
            d = _format_rushing_row(r)
            d["season"] = r.season
            results.append(d)
        return results


def _format_rushing_row(r) -> dict:
    att = r.att or 0
    rush_attempts = r.rush_attempts or 0
    return {
        "player_id": r.player_id,
        "player": r.player,
        "team": r.team or "",
        "position": r.position or "",
        "games": r.games or 0,
        "att": att,
        "yards": r.yards or 0,
        "yards_per_att": round((r.yards or 0) / att, 1) if att else 0,
        "td": r.td or 0,
        "fumbles": r.fumbles or 0,
        "rush_epa": round(r.rush_epa or 0, 1),
        "rush_attempts": rush_attempts,
        "epa_per_rush": round((r.rush_epa or 0) / rush_attempts, 3) if rush_attempts else 0,
    }


def _leaderboard_receiving(conn, mode, season_list, season_range, position, team, min_plays, limit, game_type="regular"):
    epa_t = player_season_epa_table
    pg = player_game_receiving_table
    pp = player_positions_table

    sf_epa = _season_filter(epa_t, season_list, season_range)
    sf_pg = _season_filter(pg, season_list, season_range)
    gt_epa = epa_t.c.game_type == game_type
    gt_pg = pg.c.game_type == game_type

    if mode == "career":
        trad_sub = (
            select(
                pg.c.player_id,
                func.count(func.distinct(pg.c.game_id)).label("games"),
                func.sum(pg.c.receptions).label("receptions"),
                func.sum(pg.c.yards).label("yards"),
                func.sum(pg.c.td).label("td"),
                func.sum(pg.c.fumbles).label("fumbles"),
            )
            .where(sf_pg).where(gt_pg).where(pg.c.player_id.isnot(None))
            .group_by(pg.c.player_id)
        ).subquery("trad")

        epa_sub = (
            select(
                epa_t.c.player_id, func.max(epa_t.c.player).label("player"),
                func.sum(epa_t.c.recv_epa).label("recv_epa"),
                func.sum(epa_t.c.targets).label("targets"),
            )
            .where(sf_epa).where(gt_epa).where(epa_t.c.targets > 0).where(epa_t.c.player_id.isnot(None))
            .group_by(epa_t.c.player_id)
        ).subquery("epa")

        team_sub = (
            select(epa_t.c.player_id, func.max(epa_t.c.team).label("team"))
            .where(sf_epa).where(gt_epa).where(epa_t.c.targets > 0)
            .group_by(epa_t.c.player_id)
        ).subquery("team_info")

        pos_sub = (
            select(pp.c.player_id, pp.c.position, func.count().label("cnt"))
            .where(_season_filter(pp, season_list, season_range))
            .group_by(pp.c.player_id, pp.c.position)
        ).subquery("pos_cnt")
        pos_ranked = (
            select(pos_sub.c.player_id, pos_sub.c.position)
            .distinct(pos_sub.c.player_id)
            .order_by(pos_sub.c.player_id, desc(pos_sub.c.cnt))
        ).subquery("pos_best")

        stmt = (
            select(
                epa_sub.c.player_id, epa_sub.c.player, team_sub.c.team, pos_ranked.c.position,
                trad_sub.c.games, trad_sub.c.receptions, trad_sub.c.yards, trad_sub.c.td,
                trad_sub.c.fumbles, epa_sub.c.recv_epa, epa_sub.c.targets,
            )
            .outerjoin(trad_sub, epa_sub.c.player_id == trad_sub.c.player_id)
            .outerjoin(team_sub, epa_sub.c.player_id == team_sub.c.player_id)
            .outerjoin(pos_ranked, epa_sub.c.player_id == pos_ranked.c.player_id)
            .where(epa_sub.c.targets >= min_plays)
        )
        if position:
            stmt = stmt.where(pos_ranked.c.position == position)
        if team:
            stmt = stmt.where(team_sub.c.team == team)
        stmt = stmt.order_by(desc(epa_sub.c.recv_epa / epa_sub.c.targets)).limit(limit)

        return [_format_receiving_row(r) for r in conn.execute(stmt)]

    else:
        stmt = (
            select(
                epa_t.c.player_id, epa_t.c.player, epa_t.c.team, epa_t.c.season,
                pp.c.position,
                func.sum(pg.c.receptions).label("receptions"), func.sum(pg.c.yards).label("yards"),
                func.sum(pg.c.td).label("td"), func.sum(pg.c.fumbles).label("fumbles"),
                func.count(func.distinct(pg.c.game_id)).label("games"),
                epa_t.c.recv_epa, epa_t.c.targets,
            )
            .outerjoin(pg, (epa_t.c.player_id == pg.c.player_id) & (epa_t.c.season == pg.c.season) & (pg.c.game_type == game_type))
            .outerjoin(pp, (epa_t.c.player_id == pp.c.player_id) & (epa_t.c.season == pp.c.season))
            .where(sf_epa).where(gt_epa).where(epa_t.c.targets >= min_plays)
            .group_by(epa_t.c.player_id, epa_t.c.player, epa_t.c.team, epa_t.c.season,
                      pp.c.position, epa_t.c.recv_epa, epa_t.c.targets)
        )
        if position:
            stmt = stmt.where(pp.c.position == position)
        if team:
            stmt = stmt.where(epa_t.c.team == team)
        stmt = stmt.order_by(desc(epa_t.c.recv_epa / epa_t.c.targets)).limit(limit)

        results = []
        for r in conn.execute(stmt):
            d = _format_receiving_row(r)
            d["season"] = r.season
            results.append(d)
        return results


def _format_receiving_row(r) -> dict:
    targets = r.targets or 0
    receptions = r.receptions or 0
    return {
        "player_id": r.player_id,
        "player": r.player,
        "team": r.team or "",
        "position": r.position or "",
        "games": r.games or 0,
        "receptions": receptions,
        "targets": targets,
        "yards": r.yards or 0,
        "yards_per_rec": round((r.yards or 0) / receptions, 1) if receptions else 0,
        "td": r.td or 0,
        "fumbles": r.fumbles or 0,
        "recv_epa": round(r.recv_epa or 0, 1),
        "epa_per_target": round((r.recv_epa or 0) / targets, 3) if targets else 0,
    }


def _leaderboard_defense(conn, mode, season_list, season_range, position, team, min_plays, limit, game_type="regular"):
    epa_t = player_season_epa_table
    pg = player_game_defensive_table
    pp = player_positions_table

    sf_epa = _season_filter(epa_t, season_list, season_range)
    sf_pg = _season_filter(pg, season_list, season_range)
    gt_epa = epa_t.c.game_type == game_type
    gt_pg = pg.c.game_type == game_type

    if mode == "career":
        trad_sub = (
            select(
                pg.c.player_id,
                func.count(func.distinct(pg.c.game_id)).label("games"),
                func.sum(pg.c.tackles).label("tackles"),
                func.sum(pg.c.sacks).label("sacks"),
                func.sum(pg.c.interceptions).label("interceptions"),
                func.sum(pg.c.fumble_recoveries).label("fumble_recoveries"),
                func.sum(pg.c.forced_fumbles).label("forced_fumbles"),
            )
            .where(sf_pg).where(gt_pg).where(pg.c.player_id.isnot(None))
            .group_by(pg.c.player_id)
        ).subquery("trad")

        epa_sub = (
            select(
                epa_t.c.player_id, func.max(epa_t.c.player).label("player"),
                func.sum(epa_t.c.def_epa).label("def_epa"),
                func.sum(epa_t.c.def_plays).label("def_plays"),
            )
            .where(sf_epa).where(gt_epa).where(epa_t.c.def_plays > 0).where(epa_t.c.player_id.isnot(None))
            .group_by(epa_t.c.player_id)
        ).subquery("epa")

        team_sub = (
            select(epa_t.c.player_id, func.max(epa_t.c.team).label("team"))
            .where(sf_epa).where(gt_epa).where(epa_t.c.def_plays > 0)
            .group_by(epa_t.c.player_id)
        ).subquery("team_info")

        pos_sub = (
            select(pp.c.player_id, pp.c.position, func.count().label("cnt"))
            .where(_season_filter(pp, season_list, season_range))
            .group_by(pp.c.player_id, pp.c.position)
        ).subquery("pos_cnt")
        pos_ranked = (
            select(pos_sub.c.player_id, pos_sub.c.position)
            .distinct(pos_sub.c.player_id)
            .order_by(pos_sub.c.player_id, desc(pos_sub.c.cnt))
        ).subquery("pos_best")

        stmt = (
            select(
                epa_sub.c.player_id, epa_sub.c.player, team_sub.c.team, pos_ranked.c.position,
                trad_sub.c.games, trad_sub.c.tackles, trad_sub.c.sacks,
                trad_sub.c.interceptions, trad_sub.c.fumble_recoveries, trad_sub.c.forced_fumbles,
                epa_sub.c.def_epa, epa_sub.c.def_plays,
            )
            .outerjoin(trad_sub, epa_sub.c.player_id == trad_sub.c.player_id)
            .outerjoin(team_sub, epa_sub.c.player_id == team_sub.c.player_id)
            .outerjoin(pos_ranked, epa_sub.c.player_id == pos_ranked.c.player_id)
            .where(epa_sub.c.def_plays >= min_plays)
        )
        if position:
            stmt = stmt.where(pos_ranked.c.position == position)
        if team:
            stmt = stmt.where(team_sub.c.team == team)
        # Lower EPA = better for defense
        stmt = stmt.order_by(epa_sub.c.def_epa / epa_sub.c.def_plays).limit(limit)

        return [_format_defense_row(r) for r in conn.execute(stmt)]

    else:
        stmt = (
            select(
                epa_t.c.player_id, epa_t.c.player, epa_t.c.team, epa_t.c.season,
                pp.c.position,
                func.sum(pg.c.tackles).label("tackles"),
                func.sum(cast(pg.c.sacks, Float)).label("sacks"),
                func.sum(pg.c.interceptions).label("interceptions"),
                func.sum(pg.c.fumble_recoveries).label("fumble_recoveries"),
                func.sum(pg.c.forced_fumbles).label("forced_fumbles"),
                func.count(func.distinct(pg.c.game_id)).label("games"),
                epa_t.c.def_epa, epa_t.c.def_plays,
            )
            .outerjoin(pg, (epa_t.c.player_id == pg.c.player_id) & (epa_t.c.season == pg.c.season) & (pg.c.game_type == game_type))
            .outerjoin(pp, (epa_t.c.player_id == pp.c.player_id) & (epa_t.c.season == pp.c.season))
            .where(sf_epa).where(gt_epa).where(epa_t.c.def_plays >= min_plays)
            .group_by(epa_t.c.player_id, epa_t.c.player, epa_t.c.team, epa_t.c.season,
                      pp.c.position, epa_t.c.def_epa, epa_t.c.def_plays)
        )
        if position:
            stmt = stmt.where(pp.c.position == position)
        if team:
            stmt = stmt.where(epa_t.c.team == team)
        stmt = stmt.order_by(epa_t.c.def_epa / epa_t.c.def_plays).limit(limit)

        results = []
        for r in conn.execute(stmt):
            d = _format_defense_row(r)
            d["season"] = r.season
            results.append(d)
        return results


def _format_defense_row(r) -> dict:
    def_plays = r.def_plays or 0
    return {
        "player_id": r.player_id,
        "player": r.player,
        "team": r.team or "",
        "position": r.position or "",
        "games": r.games or 0,
        "tackles": r.tackles or 0,
        "sacks": round(float(r.sacks or 0), 1),
        "interceptions": r.interceptions or 0,
        "fumble_recoveries": r.fumble_recoveries or 0,
        "forced_fumbles": r.forced_fumbles or 0,
        "def_epa": round(r.def_epa or 0, 1),
        "def_plays": def_plays,
        "epa_per_def_play": round((r.def_epa or 0) / def_plays, 3) if def_plays else 0,
    }


def _compute_defensive_epa(conn, season: int, game_type: str = "regular") -> dict[str, dict]:
    """Compute defensive EPA per team (EPA allowed).

    For each play with EPA, determines the defending team and sums EPA.
    Returns {team: {total_epa, pass_epa, rush_epa, plays}}.
    """
    mapping = _resolve_possession_team(conn, season)
    if not mapping:
        return {}

    p = plays_table
    e = play_epa_table
    scrimmage_types = ["pass", "rush", "sack"]

    stmt = (
        select(
            p.c.game_id,
            p.c.possession_team_id,
            p.c.play_type,
            e.c.epa,
        )
        .join(e, p.c.id == e.c.play_id)
        .where(p.c.season == season)
        .where(p.c.game_type == game_type)
        .where(p.c.play_type.in_(scrimmage_types))
        .where(e.c.epa.isnot(None))
    )

    result = {}
    for row in conn.execute(stmt):
        poss_team = mapping.get((row.game_id, row.possession_team_id))
        if not poss_team:
            continue

        # Find defending team
        game_teams = [
            t for (gid, tid), t in mapping.items()
            if gid == row.game_id and t != poss_team
        ]
        if not game_teams:
            continue
        def_team = game_teams[0]

        if def_team not in result:
            result[def_team] = {"total_epa": 0, "pass_epa": 0, "rush_epa": 0, "plays": 0}

        result[def_team]["total_epa"] += row.epa
        result[def_team]["plays"] += 1
        if row.play_type in ("pass", "sack"):
            result[def_team]["pass_epa"] += row.epa
        elif row.play_type == "rush":
            result[def_team]["rush_epa"] += row.epa

    return result


@router.get("/plays")
def list_plays_epa(
    request: Request,
    season: int = Query(default=None),
    season_min: int = Query(default=None),
    season_max: int = Query(default=None),
    play_type: str = Query(default=None),
    position: str = Query(default=None),
    player_id: int = Query(default=None),
    side: str = Query(default="offensive"),
    game_type: str = Query(default="regular"),
    team: str = Query(default=None),
    sort_by: str = Query(default="season"),
    sort_dir: str = Query(default="desc"),
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0),
):
    """Browse individual plays with EPA, filtered by position and side."""
    engine = request.app.state.engine
    p = plays_table
    pe = play_epa_table
    pp = player_positions_table.alias("pp_pos")

    if side == "defensive":
        player_id_cols = [
            p.c.player_id_tackler, p.c.player_id_sacker,
            p.c.player_id_interceptor, p.c.player_id_fumble_recoverer,
        ]
        side_filter = or_(*(col.isnot(None) for col in player_id_cols))
    else:
        player_id_cols = [
            p.c.player_id_passer, p.c.player_id_rusher,
            p.c.player_id_receiver, p.c.player_id_kicker,
        ]
        side_filter = or_(*(col.isnot(None) for col in player_id_cols))

    week_expr = (
        cast(
            func.floor(
                (p.c.game_id - func.min(p.c.game_id).over(partition_by=p.c.season)) / 7.0
            ),
            Integer,
        ) + 1
    ).label("week")

    stmt = (
        select(
            p.c.id, p.c.season, p.c.game_id, week_expr, p.c.quarter, p.c.clock,
            p.c.down, p.c.distance, p.c.yard_line, p.c.yard_line_team,
            p.c.play_type, p.c.passer, p.c.rusher, p.c.receiver,
            p.c.tackler, p.c.sacker, p.c.interceptor, p.c.fumble_recoverer,
            p.c.yards_gained, p.c.first_down, p.c.touchdown,
            p.c.interception, p.c.fumble, p.c.safety,
            p.c.description, p.c.home_team, p.c.away_team, p.c.possession_team_id,
            pe.c.ep_before, pe.c.ep_after, pe.c.epa,
        )
        .select_from(p)
        .join(pe, pe.c.play_id == p.c.id, isouter=True)
        .where(side_filter)
        .where(p.c.game_type == game_type)
    )

    if season is not None:
        stmt = stmt.where(p.c.season == season)
        eff_min = eff_max = season
    else:
        eff_min = season_min or 1
        eff_max = season_max or 999
        stmt = stmt.where(p.c.season.between(eff_min, eff_max))

    if play_type:
        stmt = stmt.where(p.c.play_type == play_type)

    # Pre-resolve team → possession_team_id mapping when filtering by team+side
    _team_ptids: set[int] | None = None
    if team:
        with engine.connect() as conn:
            map_stmt = (
                select(p.c.season, p.c.home_team, p.c.away_team, p.c.possession_team_id)
                .where(p.c.season.between(eff_min, eff_max))
                .where(p.c.possession_team_id.isnot(None))
                .where(p.c.home_team.isnot(None))
                .where(or_(p.c.home_team == team, p.c.away_team == team))
                .distinct()
            )
            candidates: dict[tuple[int, int], set] = {}
            for r in conn.execute(map_stmt):
                key = (int(r.season), int(r.possession_team_id))
                teams = {r.home_team, r.away_team}
                if key not in candidates:
                    candidates[key] = teams.copy()
                else:
                    candidates[key] &= teams
            # ptids that resolve to this team
            _team_ptids = {
                ptid for (_, ptid), abbrs in candidates.items()
                if len(abbrs) == 1 and next(iter(abbrs)) == team
            }

        stmt = stmt.where(or_(p.c.home_team == team, p.c.away_team == team))
        if _team_ptids:
            if side == "offensive":
                stmt = stmt.where(p.c.possession_team_id.in_(_team_ptids))
            else:
                stmt = stmt.where(p.c.possession_team_id.notin_(_team_ptids))

    if position:
        # Match if ANY player on the relevant side has the requested position
        pos_match = or_(*(
            select(literal_column("1"))
            .select_from(player_positions_table)
            .where(player_positions_table.c.player_id == col)
            .where(player_positions_table.c.season == p.c.season)
            .where(player_positions_table.c.position == position)
            .correlate(p)
            .exists()
            for col in player_id_cols
        ))
        stmt = stmt.where(pos_match)

    if player_id is not None:
        if side == "defensive":
            stmt = stmt.where(or_(
                p.c.player_id_tackler == player_id,
                p.c.player_id_sacker == player_id,
                p.c.player_id_interceptor == player_id,
                p.c.player_id_fumble_recoverer == player_id,
            ))
        else:
            stmt = stmt.where(or_(
                p.c.player_id_passer == player_id,
                p.c.player_id_rusher == player_id,
                p.c.player_id_receiver == player_id,
            ))

    # Sorting
    sort_col = {
        "epa": pe.c.epa,
        "yards": p.c.yards_gained,
    }.get(sort_by)
    if sort_col is not None:
        stmt = stmt.order_by(
            desc(sort_col).nulls_last() if sort_dir != "asc" else sort_col.nulls_last()
        )
    else:
        stmt = stmt.order_by(desc(p.c.season), p.c.game_id, p.c.play_index)

    stmt = stmt.limit(limit).offset(offset)

    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

        # Build (season, possession_team_id) → abbr mapping.
        team_map: dict[tuple[int, int], str] = {}
        if rows:
            map_stmt = (
                select(p.c.season, p.c.home_team, p.c.away_team, p.c.possession_team_id)
                .where(p.c.season.between(eff_min, eff_max))
                .where(p.c.possession_team_id.isnot(None))
                .where(p.c.home_team.isnot(None))
                .distinct()
            )
            candidates2: dict[tuple[int, int], set] = {}
            for r in conn.execute(map_stmt):
                key = (int(r.season), int(r.possession_team_id))
                teams = {r.home_team, r.away_team}
                if key not in candidates2:
                    candidates2[key] = teams.copy()
                else:
                    candidates2[key] &= teams
            team_map = {k: next(iter(s)) for k, s in candidates2.items() if len(s) == 1}

    result = []
    for row in rows:
        off_abbr = team_map.get((row.season, row.possession_team_id)) if row.possession_team_id else None
        if off_abbr and row.home_team and row.away_team:
            off_team = off_abbr
            def_team = row.away_team if off_abbr == row.home_team else row.home_team
        else:
            off_team = off_abbr
            def_team = None

        # Build player lists — each entry has name and role
        off_players = []
        if row.passer:
            off_players.append({"name": row.passer, "role": "passer"})
        if row.receiver:
            off_players.append({"name": row.receiver, "role": "receiver"})
        if row.rusher:
            off_players.append({"name": row.rusher, "role": "rusher"})

        def_players = []
        if row.interceptor:
            def_players.append({"name": row.interceptor, "role": "interceptor"})
        if row.fumble_recoverer:
            def_players.append({"name": row.fumble_recoverer, "role": "fumble_recoverer"})
        if row.sacker:
            def_players.append({"name": row.sacker, "role": "sacker"})
        if row.tackler:
            def_players.append({"name": row.tackler, "role": "tackler"})

        result.append({
            "id": row.id,
            "season": row.season,
            "game_id": row.game_id,
            "week": row.week,
            "quarter": row.quarter,
            "clock": row.clock,
            "down": row.down,
            "distance": row.distance,
            "yard_line": row.yard_line,
            "yard_line_team": row.yard_line_team,
            "play_type": row.play_type,
            "off_team": off_team,
            "def_team": def_team,
            "passer": row.passer,
            "rusher": row.rusher,
            "receiver": row.receiver,
            "off_player": row.passer or row.rusher or row.receiver,
            "off_players": off_players,
            "tackler": row.tackler,
            "sacker": row.sacker,
            "interceptor": row.interceptor,
            "fumble_recoverer": row.fumble_recoverer,
            "def_player": row.interceptor or row.fumble_recoverer or row.sacker or row.tackler,
            "def_players": def_players,
            "yards_gained": row.yards_gained,
            "first_down": row.first_down,
            "touchdown": row.touchdown,
            "interception": row.interception,
            "fumble": row.fumble,
            "safety": row.safety,
            "ep_before": row.ep_before,
            "ep_after": row.ep_after,
            "epa": row.epa,
            "description": row.description,
        })

    return result


# ---------------------------------------------------------------------------
# Visualization endpoints
# ---------------------------------------------------------------------------


@router.get("/viz/ep-by-distance")
def viz_ep_by_distance(
    request: Request,
    season_min: int = Query(default=None),
    season_max: int = Query(default=None),
    game_type: str = Query(default="regular"),
):
    """Average EP by distance to first down, split by down number."""
    engine = request.app.state.engine
    eff_min = season_min or 1
    eff_max = season_max or 999
    cache_key = ("ep-by-distance", eff_min, eff_max, game_type)

    def _compute():
        p = plays_table
        pe = play_epa_table

        stmt = (
            select(
                p.c.distance,
                p.c.down,
                func.avg(pe.c.ep_before).label("avg_ep"),
                func.count().label("count"),
            )
            .select_from(p)
            .join(pe, pe.c.play_id == p.c.id)
            .where(p.c.play_type.in_(["pass", "rush", "sack", "field_goal"]))
            .where(p.c.down.in_([1, 2, 3, 4]))
            .where(p.c.game_type == game_type)
            .where(p.c.season.between(eff_min, eff_max))
            .where(p.c.distance.isnot(None))
            .where(p.c.distance >= 1)
            .where(p.c.distance <= 30)
            .where(pe.c.ep_before.isnot(None))
            .group_by(p.c.distance, p.c.down)
            .having(func.count() >= 100)
            .order_by(p.c.distance)
        )

        with engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        return [
            {
                "distance": row.distance,
                "down": row.down,
                "avg_ep": round(float(row.avg_ep), 4),
                "count": row.count,
            }
            for row in rows
        ]

    return _viz_cached(cache_key, _compute)


@router.get("/viz/epa-by-down-distance")
def viz_epa_by_down_distance(
    request: Request,
    season_min: int = Query(default=None),
    season_max: int = Query(default=None),
    game_type: str = Query(default="regular"),
):
    """Success rate by down and distance bucket, split by pass vs rush."""
    engine = request.app.state.engine
    eff_min = season_min or 1
    eff_max = season_max or 999
    cache_key = ("epa-by-down-distance", eff_min, eff_max, game_type)

    def _compute():
        p = plays_table

        distance_bucket = case(
            (p.c.distance <= 3, literal_column("'Short'")),
            (p.c.distance <= 7, literal_column("'Medium'")),
            (p.c.distance <= 12, literal_column("'Long'")),
            else_=literal_column("'Very Long'"),
        ).label("distance_bucket")

        play_type_col = case(
            (p.c.play_type == "sack", literal_column("'pass'")),
            else_=p.c.play_type,
        ).label("play_type")

        e = play_epa_table
        success_expr = _success_check()

        stmt = (
            select(
                p.c.down,
                distance_bucket,
                play_type_col,
                func.avg(cast(cast(success_expr, Integer), Float)).label("success_rate"),
                func.count().label("count"),
            )
            .select_from(p.join(e, p.c.id == e.c.play_id))
            .where(p.c.play_type.in_(["pass", "rush", "sack"]))
            .where(p.c.down.in_([1, 2, 3, 4]))
            .where(p.c.game_type == game_type)
            .where(p.c.season.between(eff_min, eff_max))
            .where(p.c.distance.isnot(None))
            .where(p.c.distance >= 1)
            .where(e.c.epa.isnot(None))
            .group_by(p.c.down, distance_bucket, play_type_col)
            .having(func.count() >= 50)
        )

        with engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        return [
            {
                "down": row.down,
                "distance_bucket": row.distance_bucket,
                "play_type": row.play_type,
                "success_rate": round(float(row.success_rate) * 100, 1),
                "count": row.count,
            }
            for row in rows
        ]

    return _viz_cached(cache_key, _compute)


@router.get("/viz/ep-by-yardline")
def viz_ep_by_yardline(
    request: Request,
    season_min: int = Query(default=None),
    season_max: int = Query(default=None),
    game_type: str = Query(default="regular"),
):
    """Average EP by distance to endzone (yardline_100), split by down."""
    engine = request.app.state.engine
    eff_min = season_min or 1
    eff_max = season_max or 999
    cache_key = ("ep-by-yardline", eff_min, eff_max, game_type)

    def _compute():
        p = plays_table
        pe = play_epa_table

        bucket = cast(p.c.yardline_100 / 5, Integer) * 5

        stmt = (
            select(
                bucket.label("yardline"),
                p.c.down,
                func.avg(pe.c.ep_before).label("avg_ep"),
                func.count().label("count"),
            )
            .select_from(p)
            .join(pe, pe.c.play_id == p.c.id)
            .where(p.c.play_type.in_(["pass", "rush", "sack", "field_goal"]))
            .where(p.c.down.in_([1, 2, 3, 4]))
            .where(p.c.game_type == game_type)
            .where(p.c.season.between(eff_min, eff_max))
            .where(p.c.yardline_100.isnot(None))
            .where(pe.c.ep_before.isnot(None))
            .where(p.c.yardline_100 >= 1)
            .where(p.c.yardline_100 <= 99)
            .group_by(bucket, p.c.down)
            .having(func.count() >= 100)
            .order_by(bucket, p.c.down)
        )

        with engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        return [
            {
                "yardline": row.yardline,
                "down": row.down,
                "avg_ep": round(float(row.avg_ep), 4),
                "count": row.count,
            }
            for row in rows
        ]

    return _viz_cached(cache_key, _compute)


@router.get("/viz/ep-by-time")
def viz_ep_by_time(
    request: Request,
    season_min: int = Query(default=None),
    season_max: int = Query(default=None),
    game_type: str = Query(default="regular"),
):
    """Average EP by time remaining in half, split by down."""
    engine = request.app.state.engine
    eff_min = season_min or 1
    eff_max = season_max or 999
    cache_key = ("ep-by-time", eff_min, eff_max, game_type)

    def _compute():
        p = plays_table
        pe = play_epa_table

        minute_bucket = cast(p.c.half_seconds / 60, Integer).label("minute")

        stmt = (
            select(
                minute_bucket,
                p.c.down,
                func.avg(pe.c.ep_before).label("avg_ep"),
                func.count().label("count"),
            )
            .select_from(p)
            .join(pe, pe.c.play_id == p.c.id)
            .where(p.c.play_type.in_(["pass", "rush", "sack", "field_goal"]))
            .where(p.c.down.in_([1, 2, 3, 4]))
            .where(p.c.game_type == game_type)
            .where(p.c.season.between(eff_min, eff_max))
            .where(p.c.half_seconds.isnot(None))
            .where(pe.c.ep_before.isnot(None))
            .group_by(minute_bucket, p.c.down)
            .having(func.count() >= 100)
            .order_by(minute_bucket, p.c.down)
        )

        with engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        return [
            {
                "minute": row.minute,
                "down": row.down,
                "avg_ep": round(float(row.avg_ep), 4),
                "count": row.count,
            }
            for row in rows
        ]

    return _viz_cached(cache_key, _compute)


@router.get("/viz/ep-by-drive-start")
def viz_ep_by_drive_start(
    request: Request,
    season_min: int = Query(default=None),
    season_max: int = Query(default=None),
    game_type: str = Query(default="regular"),
):
    """Average EP at the start of each drive, bucketed by starting yardline."""
    engine = request.app.state.engine
    eff_min = season_min or 1
    eff_max = season_max or 999
    cache_key = ("ep-by-drive-start", eff_min, eff_max, game_type)

    def _compute():
        p = plays_table
        pe = play_epa_table

        # Compute half from quarter (portable)
        half = case(
            (p.c.quarter <= 2, 1),
            (p.c.quarter <= 4, 2),
            else_=3,
        )

        # Window: LAG over scrimmage plays within each game
        win = {"partition_by": p.c.game_id, "order_by": p.c.play_index}
        prev_ptid = func.lag(p.c.possession_team_id).over(**win)
        prev_half = func.lag(half).over(**win)

        # CTE: scrimmage plays with drive-start detection columns
        prev_idx = func.lag(p.c.play_index).over(**win)
        scrimmage = (
            select(
                p.c.id.label("play_id"),
                p.c.game_id,
                p.c.play_index,
                p.c.yardline_100,
                prev_ptid.label("prev_ptid"),
                prev_half.label("prev_half"),
                prev_idx.label("prev_idx"),
                half.label("half"),
                p.c.possession_team_id,
            )
            .where(p.c.play_type.in_(["pass", "rush", "sack", "field_goal"]))
            .where(p.c.game_type == game_type)
            .where(p.c.season.between(eff_min, eff_max))
            .where(p.c.possession_team_id.isnot(None))
            .where(p.c.yardline_100.isnot(None))
            .where(p.c.quarter.isnot(None))
        ).cte("scrimmage")

        # Check if a kickoff occurred between previous and current scrimmage play
        kickoff_between = exists(
            select(literal_column("1")).select_from(p)
            .where(p.c.game_id == scrimmage.c.game_id)
            .where(p.c.play_type == "kickoff")
            .where(p.c.play_index > scrimmage.c.prev_idx)
            .where(p.c.play_index < scrimmage.c.play_index)
        )

        # Filter to drive starts: first play, possession/half changed, or kickoff between
        drive_starts = (
            select(scrimmage)
            .where(
                or_(
                    scrimmage.c.prev_ptid.is_(None),
                    scrimmage.c.possession_team_id != scrimmage.c.prev_ptid,
                    scrimmage.c.half != scrimmage.c.prev_half,
                    kickoff_between,
                )
            )
        ).cte("drive_starts")

        bucket = cast(drive_starts.c.yardline_100 / 5, Integer) * 5

        stmt = (
            select(
                bucket.label("yardline"),
                func.avg(pe.c.ep_before).label("avg_ep"),
                func.count().label("count"),
            )
            .select_from(drive_starts)
            .join(pe, pe.c.play_id == drive_starts.c.play_id)
            .where(pe.c.ep_before.isnot(None))
            .where(drive_starts.c.yardline_100 >= 1)
            .where(drive_starts.c.yardline_100 <= 99)
            .group_by(bucket)
            .having(func.count() >= 30)
            .order_by(bucket)
        )

        with engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        return [
            {
                "yardline": row.yardline,
                "avg_ep": round(float(row.avg_ep), 4),
                "count": row.count,
            }
            for row in rows
        ]

    return _viz_cached(cache_key, _compute)


def _fourth_down_score_filter(p, ptid_abbrs, score_filter):
    """Return a list of SQL WHERE clauses for winning/losing/all score filter."""
    if score_filter == "all":
        return []

    # Build score_diff: possession team's score minus opponent's score.
    # If possession_team matches home_team abbrs -> score_home - score_away, else opposite.
    home_cases = [
        (and_(p.c.possession_team_id == ptid, p.c.home_team.in_(abbrs)), 1)
        for ptid, abbrs in ptid_abbrs.items()
    ]
    is_home = case(*home_cases, else_=0)
    score_diff = case(
        (is_home == 1, p.c.score_home - p.c.score_away),
        else_=p.c.score_away - p.c.score_home,
    )

    filters = [p.c.score_home.isnot(None), p.c.score_away.isnot(None)]
    if score_filter == "winning":
        filters.append(score_diff > 0)
    elif score_filter == "losing":
        filters.append(score_diff < 0)
    elif score_filter == "tied":
        filters.append(score_diff == 0)
    return filters


def _fourth_down_count_exprs(p):
    """Return (go_count, punt_count, fg_count, total) aggregate expressions."""
    go_count = func.sum(case((p.c.play_type.in_(["pass", "rush", "sack"]), 1), else_=0))
    punt_count = func.sum(case((p.c.play_type == "punt", 1), else_=0))
    fg_count = func.sum(case((p.c.play_type == "field_goal", 1), else_=0))
    return go_count, punt_count, fg_count, func.count()


def _fourth_down_rows_to_dicts(rows, bucket_key):
    """Convert rows with go_count/punt_count/fg_count/total to percentage dicts."""
    return [
        {
            bucket_key: getattr(row, bucket_key),
            "go_pct": round(100 * row.go_count / row.total, 2),
            "punt_pct": round(100 * row.punt_count / row.total, 2),
            "fg_pct": round(100 * row.fg_count / row.total, 2),
            "total": row.total,
        }
        for row in rows
    ]


@router.get("/viz/fourth-down-decisions")
def viz_fourth_down_decisions(
    request: Request,
    season_min: int = Query(default=None),
    season_max: int = Query(default=None),
    game_type: str = Query(default="regular"),
    score_filter: str = Query(default="all"),
):
    """4th down play type percentages (go for it / punt / field goal) by field position."""
    engine = request.app.state.engine
    eff_min = season_min or 1
    eff_max = season_max or 999
    cache_key = ("fourth-down-decisions", eff_min, eff_max, game_type, score_filter)

    def _compute():
        p = plays_table

        bucket = cast(p.c.yardline_100 / 5, Integer) * 5

        go_count, punt_count, fg_count, total = _fourth_down_count_exprs(p)

        stmt = (
            select(
                bucket.label("yardline"),
                go_count.label("go_count"),
                punt_count.label("punt_count"),
                fg_count.label("fg_count"),
                total.label("total"),
            )
            .where(p.c.down == 4)
            .where(p.c.play_type.in_(["pass", "rush", "sack", "punt", "field_goal"]))
            .where(p.c.game_type == game_type)
            .where(p.c.season.between(eff_min, eff_max))
            .where(p.c.yardline_100.isnot(None))
            .where(p.c.yardline_100 >= 1)
            .where(p.c.yardline_100 <= 99)
        )

        # Score filter still needs team mapping for score_diff computation
        if score_filter != "all":
            with engine.connect() as conn:
                season_stmt = select(distinct(p.c.season)).where(
                    p.c.season.between(eff_min, eff_max)
                )
                seasons = [r[0] for r in conn.execute(season_stmt).fetchall()]
            ptid_abbrs = get_team_id_to_all_abbrs(engine, [int(s) for s in seasons]) if seasons else {}
            for f in _fourth_down_score_filter(p, ptid_abbrs, score_filter):
                stmt = stmt.where(f)

        stmt = stmt.group_by(bucket).having(func.count() >= 20).order_by(bucket)

        with engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        return _fourth_down_rows_to_dicts(rows, "yardline")

    return _viz_cached(cache_key, _compute)


@router.get("/viz/fourth-down-by-time")
def viz_fourth_down_by_time(
    request: Request,
    season_min: int = Query(default=None),
    season_max: int = Query(default=None),
    game_type: str = Query(default="regular"),
    score_filter: str = Query(default="all"),
):
    """4th down play type percentages by time remaining in half."""
    engine = request.app.state.engine
    eff_min = season_min or 1
    eff_max = season_max or 999
    cache_key = ("fourth-down-by-time", eff_min, eff_max, game_type, score_filter)

    def _compute():
        p = plays_table

        minute_bucket = cast(p.c.half_seconds / 60, Integer)

        go_count, punt_count, fg_count, total = _fourth_down_count_exprs(p)

        stmt = (
            select(
                minute_bucket.label("minute"),
                go_count.label("go_count"),
                punt_count.label("punt_count"),
                fg_count.label("fg_count"),
                total.label("total"),
            )
            .where(p.c.down == 4)
            .where(p.c.play_type.in_(["pass", "rush", "sack", "punt", "field_goal"]))
            .where(p.c.game_type == game_type)
            .where(p.c.season.between(eff_min, eff_max))
            .where(p.c.half_seconds.isnot(None))
            .where(p.c.possession_team_id.isnot(None))
        )

        # Score filter still needs team mapping for score_diff computation
        if score_filter != "all":
            with engine.connect() as conn:
                season_stmt = select(distinct(p.c.season)).where(
                    p.c.season.between(eff_min, eff_max)
                )
                seasons = [r[0] for r in conn.execute(season_stmt).fetchall()]
            ptid_abbrs = get_team_id_to_all_abbrs(engine, [int(s) for s in seasons]) if seasons else {}
            for f in _fourth_down_score_filter(p, ptid_abbrs, score_filter):
                stmt = stmt.where(f)

        stmt = stmt.group_by(minute_bucket).having(func.count() >= 20).order_by(minute_bucket)

        with engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        return _fourth_down_rows_to_dicts(rows, "minute")

    return _viz_cached(cache_key, _compute)