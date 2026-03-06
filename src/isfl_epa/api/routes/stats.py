"""Stats query endpoints."""

from fastapi import APIRouter, Query, Request
from sqlalchemy import desc, func, select

from isfl_epa.storage.database import (
    player_game_defensive_table,
    player_game_passing_table,
    player_game_receiving_table,
    player_game_rushing_table,
    team_games_table,
)

router = APIRouter()


def _season_leaders(engine, table, season: int, top: int, order_col: str):
    """Generic season leader query: group by player, sum stats, order by column."""
    # Get all non-grouping numeric columns to sum
    group_cols = {"player_id", "player", "team"}
    skip_cols = {"id", "game_id", "season"}
    sum_cols = [c for c in table.columns if c.name not in group_cols | skip_cols]

    stmt = (
        select(
            table.c.player_id,
            table.c.player,
            table.c.team,
            *[func.sum(c).label(c.name) for c in sum_cols],
            func.count().label("games"),
        )
        .where(table.c.season == season)
        .group_by(table.c.player_id, table.c.player, table.c.team)
        .order_by(desc(order_col))
        .limit(top)
    )
    with engine.connect() as conn:
        result = conn.execute(stmt)
        return [dict(row._mapping) for row in result]


@router.get("/passing")
def passing_leaders(
    request: Request,
    season: int = Query(...),
    top: int = Query(default=20, le=100),
):
    """Season passing leaders by yards."""
    return _season_leaders(request.app.state.engine, player_game_passing_table, season, top, "yards")


@router.get("/rushing")
def rushing_leaders(
    request: Request,
    season: int = Query(...),
    top: int = Query(default=20, le=100),
):
    """Season rushing leaders by yards."""
    return _season_leaders(request.app.state.engine, player_game_rushing_table, season, top, "yards")


@router.get("/receiving")
def receiving_leaders(
    request: Request,
    season: int = Query(...),
    top: int = Query(default=20, le=100),
):
    """Season receiving leaders by yards."""
    return _season_leaders(request.app.state.engine, player_game_receiving_table, season, top, "yards")


@router.get("/defensive")
def defensive_leaders(
    request: Request,
    season: int = Query(...),
    top: int = Query(default=20, le=100),
):
    """Season defensive leaders by tackles."""
    return _season_leaders(request.app.state.engine, player_game_defensive_table, season, top, "tackles")


@router.get("/team")
def team_stats(
    request: Request,
    season: int = Query(...),
):
    """Team season stats (aggregated from per-game data)."""
    engine = request.app.state.engine
    t = team_games_table
    skip = {"id", "game_id", "opponent", "is_home"}
    sum_cols = [c for c in t.columns if c.name not in {"team", "season"} | skip]

    stmt = (
        select(
            t.c.team,
            func.count().label("games"),
            *[func.sum(c).label(c.name) for c in sum_cols],
        )
        .where(t.c.season == season)
        .group_by(t.c.team)
        .order_by(desc("total_yards"))
    )
    with engine.connect() as conn:
        result = conn.execute(stmt)
        return [dict(row._mapping) for row in result]


@router.get("/player/{player_id}/game-log")
def player_game_log(
    request: Request,
    player_id: int,
    season: int | None = None,
    category: str = Query(default="passing"),
):
    """Player game log for a specific stat category."""
    table_map = {
        "passing": player_game_passing_table,
        "rushing": player_game_rushing_table,
        "receiving": player_game_receiving_table,
        "defensive": player_game_defensive_table,
    }
    table = table_map.get(category)
    if table is None:
        return {"error": f"Unknown category: {category}"}

    engine = request.app.state.engine
    stmt = select(table).where(table.c.player_id == player_id)
    if season is not None:
        stmt = stmt.where(table.c.season == season)
    stmt = stmt.order_by(table.c.game_id)

    with engine.connect() as conn:
        result = conn.execute(stmt)
        return [dict(row._mapping) for row in result]
