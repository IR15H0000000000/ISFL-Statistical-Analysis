"""Play query endpoints."""

from fastapi import APIRouter, Query, Request
from sqlalchemy import select

from isfl_epa.storage.database import plays_table

router = APIRouter()


@router.get("/")
def list_plays(
    request: Request,
    season: int | None = None,
    game_id: int | None = None,
    play_type: str | None = None,
    player_id: int | None = None,
    limit: int = Query(default=100, le=1000),
    offset: int = 0,
):
    """Query plays with optional filters."""
    engine = request.app.state.engine
    stmt = select(plays_table)

    if season is not None:
        stmt = stmt.where(plays_table.c.season == season)
    if game_id is not None:
        stmt = stmt.where(plays_table.c.game_id == game_id)
    if play_type is not None:
        stmt = stmt.where(plays_table.c.play_type == play_type)
    if player_id is not None:
        stmt = stmt.where(
            (plays_table.c.player_id_passer == player_id)
            | (plays_table.c.player_id_rusher == player_id)
            | (plays_table.c.player_id_receiver == player_id)
            | (plays_table.c.player_id_tackler == player_id)
            | (plays_table.c.player_id_sacker == player_id)
            | (plays_table.c.player_id_interceptor == player_id)
            | (plays_table.c.player_id_kicker == player_id)
            | (plays_table.c.player_id_returner == player_id)
        )

    stmt = stmt.offset(offset).limit(limit)
    with engine.connect() as conn:
        result = conn.execute(stmt)
        return [dict(row._mapping) for row in result]


@router.get("/{game_id}")
def get_game_plays(request: Request, game_id: int):
    """Get all plays for a specific game."""
    engine = request.app.state.engine
    stmt = (
        select(plays_table)
        .where(plays_table.c.game_id == game_id)
        .order_by(plays_table.c.id)
    )
    with engine.connect() as conn:
        result = conn.execute(stmt)
        return [dict(row._mapping) for row in result]
