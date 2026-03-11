"""EPA query endpoints."""

from fastapi import APIRouter, Query, Request
from sqlalchemy import Integer, case, cast, desc, func, literal_column, or_, select, Float

from isfl_epa.storage.database import (
    games_table,
    get_team_id_to_abbr,
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
        return [dict(row._mapping) for row in conn.execute(stmt)]


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
        return [dict(row._mapping) for row in conn.execute(stmt)]


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
        return [dict(row._mapping) for row in conn.execute(stmt)]


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
        return [dict(row._mapping) for row in conn.execute(stmt)]


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
            plays_table.c.passer,
            plays_table.c.rusher,
            plays_table.c.receiver,
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
    """SQL CASE expression for PFR-style success rate.

    1st down: yards_gained >= 0.4 * distance
    2nd down: yards_gained >= 0.6 * distance
    3rd/4th down: yards_gained >= distance
    """
    p = plays_table
    return case(
        (p.c.down == 1, p.c.yards_gained >= cast(p.c.distance * 0.4, Float)),
        (p.c.down == 2, p.c.yards_gained >= cast(p.c.distance * 0.6, Float)),
        (p.c.down >= 3, p.c.yards_gained >= p.c.distance),
        else_=False,
    )


@router.get("/team-dashboard")
def team_dashboard(
    request: Request,
    season: int = Query(...),
    side: str = Query(default="offensive"),
    game_type: str = Query(default="regular"),
):
    """Combined team stats for the dashboard visualization."""
    engine = request.app.state.engine

    if side == "defensive":
        return _defensive_dashboard(engine, season, game_type)
    return _offensive_dashboard(engine, season, game_type)


def _compute_offensive_epa(conn, season: int, game_type: str = "regular") -> dict[str, dict]:
    """Compute offensive EPA per team from play-level data.

    Returns {team: {total_epa, pass_epa, rush_epa, plays, epa_per_play}}.
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

        if poss_team not in result:
            result[poss_team] = {"total_epa": 0, "pass_epa": 0, "rush_epa": 0, "plays": 0}

        result[poss_team]["total_epa"] += row.epa
        result[poss_team]["plays"] += 1
        if row.play_type in ("pass", "sack"):
            result[poss_team]["pass_epa"] += row.epa
        elif row.play_type == "rush":
            result[poss_team]["rush_epa"] += row.epa

    for team_data in result.values():
        plays = team_data["plays"]
        team_data["epa_per_play"] = team_data["total_epa"] / plays if plays else 0

    return result


def _offensive_dashboard(engine, season: int, game_type: str = "regular") -> list[dict]:
    """Build offensive team dashboard data."""
    with engine.connect() as conn:
        # 1. EPA data computed from play-level data
        epa_rows = _compute_offensive_epa(conn, season, game_type)

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

        # 3. Success rate from play-level data
        success_by_team = _compute_success_rate(conn, season, offensive=True, game_type=game_type)

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

            results.append({
                "team": team,
                "record": f"{wins}-{losses}" + (f"-{ties}" if ties else ""),
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "points_for": trad.get("points_for") or 0,
                "epa_per_play": round(epa.get("epa_per_play", 0), 3),
                "total_epa": round(epa.get("total_epa", 0), 2),
                "success_pct": round(success_by_team.get(team, 0) * 100, 1),
                "epa_per_pass": round(epa.get("pass_epa", 0) / dropbacks, 3) if dropbacks else 0,
                "epa_per_rush": round(epa.get("rush_epa", 0) / rush_att, 3) if rush_att else 0,
                "pass_yards": trad.get("pass_yards") or 0,
                "comp_pct": round((trad.get("pass_comp") or 0) / pass_att * 100, 1) if pass_att else 0,
                "pass_td": trad.get("pass_td") or 0,
                "rush_yards": trad.get("rush_yards") or 0,
                "rush_td": trad.get("rush_td") or 0,
                "sack_pct": round(sacks_taken / dropbacks * 100, 1) if dropbacks else 0,
                "int_pct": round(ints_thrown / pass_att * 100, 1) if pass_att else 0,
            })

        results.sort(key=lambda r: r["epa_per_play"], reverse=True)
        for i, r in enumerate(results):
            r["rank"] = i + 1
        return results


def _defensive_dashboard(engine, season: int, game_type: str = "regular") -> list[dict]:
    """Build defensive team dashboard data."""
    with engine.connect() as conn:
        # 1. Defensive EPA: sum of opponent's EPA against each team
        def_epa = _compute_defensive_epa(conn, season, game_type=game_type)

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

        # 4. Defensive success rate
        success_by_team = _compute_success_rate(conn, season, offensive=False, game_type=game_type)

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
            total_epa = epa_data.get("total_epa", 0)
            plays = epa_data.get("plays", 0)
            pass_epa = epa_data.get("pass_epa", 0)
            rush_epa = epa_data.get("rush_epa", 0)
            sacks_made = own.get("sacks_made") or 0

            wins = own.get("wins") or 0
            losses = own.get("losses") or 0
            ties = own.get("ties") or 0

            results.append({
                "team": team,
                "record": f"{wins}-{losses}" + (f"-{ties}" if ties else ""),
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "points_against": own.get("points_against") or 0,
                "epa_per_play": round(total_epa / plays, 3) if plays else 0,
                "total_epa": round(total_epa, 2),
                "success_pct": round(success_by_team.get(team, 0) * 100, 1),
                "epa_per_pass": round(pass_epa / opp_dropbacks, 3) if opp_dropbacks else 0,
                "epa_per_rush": round(rush_epa / opp_rush_att, 3) if opp_rush_att else 0,
                "pass_yards": trad.get("pass_yards") or 0,
                "comp_pct": round((trad.get("pass_comp") or 0) / opp_pass_att * 100, 1) if opp_pass_att else 0,
                "pass_td": trad.get("pass_td") or 0,
                "rush_yards": trad.get("rush_yards") or 0,
                "rush_td": trad.get("rush_td") or 0,
                "sack_pct": round(sacks_made / opp_dropbacks * 100, 1) if opp_dropbacks else 0,
                "int_pct": round(opp_ints / opp_pass_att * 100, 1) if opp_pass_att else 0,
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
    """Compute PFR-style success rate per team.

    If offensive=True, groups by possession team (how successful is the offense).
    If offensive=False, groups by defending team (how successful are opponents).
    """
    mapping = _resolve_possession_team(conn, season)
    if not mapping:
        return {}

    p = plays_table
    e = play_epa_table
    scrimmage_types = ["pass", "rush", "sack"]

    # Fetch all scrimmage plays with down/distance/yards
    stmt = (
        select(
            p.c.game_id,
            p.c.possession_team_id,
            p.c.down,
            p.c.distance,
            p.c.yards_gained,
        )
        .where(p.c.season == season)
        .where(p.c.game_type == game_type)
        .where(p.c.play_type.in_(scrimmage_types))
        .where(p.c.down.isnot(None))
        .where(p.c.distance.isnot(None))
        .where(p.c.yards_gained.isnot(None))
    )

    # Compute success per team
    team_total = {}
    team_success = {}
    for row in conn.execute(stmt):
        poss_team = mapping.get((row.game_id, row.possession_team_id))
        if not poss_team:
            continue

        # Determine the defending team for defensive view
        # Find the other team in this game
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

        # PFR success: 1st → 40%, 2nd → 60%, 3rd/4th → 100%
        down = row.down
        distance = row.distance
        yards = row.yards_gained or 0
        if down == 1:
            success = yards >= distance * 0.4
        elif down == 2:
            success = yards >= distance * 0.6
        else:
            success = yards >= distance

        team_total[team] = team_total.get(team, 0) + 1
        if success:
            team_success[team] = team_success.get(team, 0) + 1

    return {
        team: team_success.get(team, 0) / total
        for team, total in team_total.items()
        if total > 0
    }


@router.get("/positions")
def available_positions(request: Request):
    """List distinct positions from player_positions table."""
    engine = request.app.state.engine
    pp = player_positions_table
    stmt = select(pp.c.position).distinct().order_by(pp.c.position)
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(stmt)]


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
            return _leaderboard_passing(conn, mode, season_list if mode == "season" else None,
                                        season_range if mode != "season" else None,
                                        position, team, min_plays or 100, limit, game_type)
        elif category == "rushing":
            return _leaderboard_rushing(conn, mode, season_list if mode == "season" else None,
                                        season_range if mode != "season" else None,
                                        position, team, min_plays or 50, limit, game_type)
        elif category == "receiving":
            return _leaderboard_receiving(conn, mode, season_list if mode == "season" else None,
                                          season_range if mode != "season" else None,
                                          position, team, min_plays or 30, limit, game_type)
        elif category == "defense":
            return _leaderboard_defense(conn, mode, season_list if mode == "season" else None,
                                        season_range if mode != "season" else None,
                                        position, team, min_plays or 50, limit, game_type)
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
                trad_sub.c.interceptions, trad_sub.c.fumble_recoveries,
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
        primary_id_expr = func.coalesce(
            p.c.player_id_tackler, p.c.player_id_sacker, p.c.player_id_interceptor
        )
        side_filter = or_(
            p.c.player_id_tackler.isnot(None),
            p.c.player_id_sacker.isnot(None),
            p.c.player_id_interceptor.isnot(None),
        )
    else:
        primary_id_expr = func.coalesce(
            p.c.player_id_passer, p.c.player_id_rusher,
            p.c.player_id_receiver, p.c.player_id_kicker,
        )
        side_filter = or_(
            p.c.player_id_passer.isnot(None),
            p.c.player_id_rusher.isnot(None),
            p.c.player_id_receiver.isnot(None),
            p.c.player_id_kicker.isnot(None),
        )

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
            p.c.tackler, p.c.sacker, p.c.interceptor,
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

    if team:
        stmt = stmt.where(or_(p.c.home_team == team, p.c.away_team == team))

    if position:
        stmt = stmt.join(
            pp,
            (pp.c.player_id == primary_id_expr) & (pp.c.season == p.c.season),
        ).where(pp.c.position == position)

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

        # Build possession_team_id → abbr mapping for this season range
        team_map: dict[int, str] = {}
        if rows:
            map_stmt = (
                select(p.c.home_team, p.c.away_team, p.c.possession_team_id)
                .where(p.c.season.between(eff_min, eff_max))
                .where(p.c.possession_team_id.isnot(None))
                .where(p.c.home_team.isnot(None))
                .distinct()
            )
            candidates: dict[int, set] = {}
            for r in conn.execute(map_stmt):
                ptid = int(r.possession_team_id)
                teams = {r.home_team, r.away_team}
                if ptid not in candidates:
                    candidates[ptid] = teams.copy()
                else:
                    candidates[ptid] &= teams
            team_map = {ptid: next(iter(s)) for ptid, s in candidates.items() if len(s) == 1}

    result = []
    for row in rows:
        off_abbr = team_map.get(row.possession_team_id) if row.possession_team_id else None
        if off_abbr and row.home_team and row.away_team:
            off_team = off_abbr
            def_team = row.away_team if off_abbr == row.home_team else row.home_team
        else:
            off_team = off_abbr
            def_team = None

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
            "tackler": row.tackler,
            "sacker": row.sacker,
            "interceptor": row.interceptor,
            "def_player": row.interceptor or row.sacker or row.tackler,
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
