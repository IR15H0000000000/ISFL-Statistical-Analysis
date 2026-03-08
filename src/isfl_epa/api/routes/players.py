"""Player query endpoints."""

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import func, select

from isfl_epa.storage.database import (
    player_game_passing_table,
    player_game_rushing_table,
    player_game_receiving_table,
    player_names_table,
    players_table,
    plays_table,
)

router = APIRouter()


@router.get("/")
def search_players(
    request: Request,
    name: str | None = None,
    season: int | None = None,
    limit: int = Query(default=20, le=100),
):
    """Search players by name or season."""
    engine = request.app.state.engine

    if name:
        # Search player_names for matching names
        stmt = (
            select(
                players_table.c.player_id,
                players_table.c.canonical_name,
                players_table.c.first_seen_season,
                players_table.c.last_seen_season,
            )
            .join(player_names_table, players_table.c.player_id == player_names_table.c.player_id)
            .where(func.lower(player_names_table.c.name).contains(name.lower()))
        )
        if season is not None:
            stmt = stmt.where(player_names_table.c.season == season)
        stmt = stmt.distinct().limit(limit)
    else:
        stmt = select(players_table).limit(limit)
        if season is not None:
            stmt = stmt.where(
                (players_table.c.first_seen_season <= season)
                & (players_table.c.last_seen_season >= season)
            )

    with engine.connect() as conn:
        result = conn.execute(stmt)
        return [dict(row._mapping) for row in result]


@router.get("/{player_id}")
def get_player(request: Request, player_id: int):
    """Get player profile with career stats summary."""
    engine = request.app.state.engine

    # Player info
    with engine.connect() as conn:
        player = conn.execute(
            select(players_table).where(players_table.c.player_id == player_id)
        ).first()
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        # Aliases
        aliases = conn.execute(
            select(player_names_table).where(player_names_table.c.player_id == player_id)
        ).fetchall()

        # Career passing totals
        passing = conn.execute(
            select(
                func.sum(player_game_passing_table.c.comp).label("comp"),
                func.sum(player_game_passing_table.c.att).label("att"),
                func.sum(player_game_passing_table.c.yards).label("yards"),
                func.sum(player_game_passing_table.c.td).label("td"),
                func.sum(player_game_passing_table.c.interceptions).label("interceptions"),
                func.count().label("games"),
            ).where(player_game_passing_table.c.player_id == player_id)
        ).first()

        # Career rushing totals
        rushing = conn.execute(
            select(
                func.sum(player_game_rushing_table.c.att).label("att"),
                func.sum(player_game_rushing_table.c.yards).label("yards"),
                func.sum(player_game_rushing_table.c.td).label("td"),
                func.count().label("games"),
            ).where(player_game_rushing_table.c.player_id == player_id)
        ).first()

        # Career receiving totals
        receiving = conn.execute(
            select(
                func.sum(player_game_receiving_table.c.receptions).label("receptions"),
                func.sum(player_game_receiving_table.c.yards).label("yards"),
                func.sum(player_game_receiving_table.c.td).label("td"),
                func.count().label("games"),
            ).where(player_game_receiving_table.c.player_id == player_id)
        ).first()

    return {
        "player": dict(player._mapping),
        "aliases": [dict(a._mapping) for a in aliases],
        "career_passing": dict(passing._mapping) if passing and passing.games else None,
        "career_rushing": dict(rushing._mapping) if rushing and rushing.games else None,
        "career_receiving": dict(receiving._mapping) if receiving and receiving.games else None,
    }


@router.get("/{player_id}/plays")
def get_player_plays(
    request: Request,
    player_id: int,
    season: int | None = None,
    play_type: str | None = None,
    limit: int = Query(default=100, le=1000),
    offset: int = 0,
):
    """Get all plays involving a specific player."""
    engine = request.app.state.engine
    stmt = select(plays_table).where(
        (plays_table.c.player_id_passer == player_id)
        | (plays_table.c.player_id_rusher == player_id)
        | (plays_table.c.player_id_receiver == player_id)
        | (plays_table.c.player_id_tackler == player_id)
        | (plays_table.c.player_id_sacker == player_id)
        | (plays_table.c.player_id_interceptor == player_id)
        | (plays_table.c.player_id_kicker == player_id)
        | (plays_table.c.player_id_returner == player_id)
    )
    if season is not None:
        stmt = stmt.where(plays_table.c.season == season)
    if play_type is not None:
        stmt = stmt.where(plays_table.c.play_type == play_type)
    stmt = stmt.offset(offset).limit(limit)

    with engine.connect() as conn:
        result = conn.execute(stmt)
        return [dict(row._mapping) for row in result]
