"""PostgreSQL storage layer using SQLAlchemy Core.

Schema:
- players / player_names: player registry
- plays: one row per ParsedPlay with player_id foreign keys
- team_games: per-team per-game stats
- player_game_passing/rushing/receiving/defensive: per-player per-game stats
"""

from __future__ import annotations

import os

from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    insert,
    select,
)
from sqlalchemy.engine import Engine

from isfl_epa.parser.schema import Game, PlayType
from isfl_epa.players.registry import PlayerRegistry
from isfl_epa.stats.aggregation import (
    game_player_defensive,
    game_player_passing,
    game_player_receiving,
    game_player_rushing,
    game_team_stats,
)

DEFAULT_DATABASE_URL = "postgresql+psycopg://isfl:isfl@localhost:5432/isfl"

metadata = MetaData()

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------

players_table = Table(
    "players", metadata,
    Column("player_id", Integer, primary_key=True, autoincrement=True),
    Column("canonical_name", Text, nullable=False),
    Column("first_seen_season", Integer),
    Column("last_seen_season", Integer),
)

player_names_table = Table(
    "player_names", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("player_id", Integer, ForeignKey("players.player_id"), nullable=False),
    Column("name", Text, nullable=False),
    Column("season", Integer, nullable=False),
    Column("team", String(10)),
    Index("ix_player_names_unique", "name", "season", unique=True),
)

plays_table = Table(
    "plays", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("league", String(10)),
    Column("quarter", Integer),
    Column("clock", String(10)),
    Column("play_index", Integer),
    Column("play_type", String(20)),
    Column("description", Text),
    Column("css", String(5)),
    # Situation
    Column("down", Integer),
    Column("distance", Integer),
    Column("distance_text", String(20)),
    Column("yard_line", Integer),
    Column("yard_line_team", String(10)),
    Column("possession_team_id", Integer),
    # Score
    Column("score_away", Integer),
    Column("score_home", Integer),
    Column("away_team", String(10)),
    Column("home_team", String(10)),
    # Outcomes
    Column("yards_gained", Integer),
    Column("first_down", Boolean, default=False),
    Column("touchdown", Boolean, default=False),
    Column("fumble", Boolean, default=False),
    Column("fumble_lost", Boolean, default=False),
    Column("interception", Boolean, default=False),
    Column("safety", Boolean, default=False),
    Column("turnover_on_downs", Boolean, default=False),
    Column("penalty", Boolean, default=False),
    Column("penalty_team", String(10)),
    Column("penalty_type", String(50)),
    Column("penalty_auto_first", Boolean, default=False),
    # Player names (raw)
    Column("passer", Text),
    Column("rusher", Text),
    Column("receiver", Text),
    Column("tackler", Text),
    Column("sacker", Text),
    Column("interceptor", Text),
    Column("kicker", Text),
    Column("returner", Text),
    Column("fumbler", Text),
    Column("fumble_recoverer", Text),
    # Player IDs (foreign keys)
    Column("player_id_passer", Integer, ForeignKey("players.player_id")),
    Column("player_id_rusher", Integer, ForeignKey("players.player_id")),
    Column("player_id_receiver", Integer, ForeignKey("players.player_id")),
    Column("player_id_tackler", Integer, ForeignKey("players.player_id")),
    Column("player_id_sacker", Integer, ForeignKey("players.player_id")),
    Column("player_id_interceptor", Integer, ForeignKey("players.player_id")),
    Column("player_id_kicker", Integer, ForeignKey("players.player_id")),
    Column("player_id_returner", Integer, ForeignKey("players.player_id")),
    # Kicking
    Column("kick_yards", Integer),
    Column("fg_distance", Integer),
    Column("fg_good", Boolean),
    Column("pat_good", Boolean),
    # Indexes
    Index("ix_plays_game_id", "game_id"),
    Index("ix_plays_season", "season"),
    Index("ix_plays_play_type", "play_type"),
    Index("ix_plays_season_team", "season", "possession_team_id"),
    Index("ix_plays_passer", "player_id_passer"),
    Index("ix_plays_rusher", "player_id_rusher"),
    Index("ix_plays_receiver", "player_id_receiver"),
    Index("ix_plays_sacker", "player_id_sacker"),
    Index("ix_plays_interceptor", "player_id_interceptor"),
)

team_games_table = Table(
    "team_games", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("team", String(10), nullable=False),
    Column("opponent", String(10)),
    Column("is_home", Boolean),
    Column("points_for", Integer, default=0),
    Column("points_against", Integer, default=0),
    Column("pass_comp", Integer, default=0),
    Column("pass_att", Integer, default=0),
    Column("pass_yards", Integer, default=0),
    Column("pass_td", Integer, default=0),
    Column("rush_att", Integer, default=0),
    Column("rush_yards", Integer, default=0),
    Column("rush_td", Integer, default=0),
    Column("total_yards", Integer, default=0),
    Column("first_downs", Integer, default=0),
    Column("interceptions_thrown", Integer, default=0),
    Column("fumbles_lost", Integer, default=0),
    Column("turnovers", Integer, default=0),
    Column("third_down_att", Integer, default=0),
    Column("third_down_conv", Integer, default=0),
    Column("sacks_taken", Integer, default=0),
    Column("sacks_made", Integer, default=0),
    Index("ix_team_games_season", "season"),
    Index("ix_team_games_team", "team"),
    Index("ix_team_games_game_id", "game_id"),
)

player_game_passing_table = Table(
    "player_game_passing", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("player_id", Integer, ForeignKey("players.player_id")),
    Column("player", Text, nullable=False),
    Column("team", String(10)),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("comp", Integer, default=0),
    Column("att", Integer, default=0),
    Column("yards", Integer, default=0),
    Column("td", Integer, default=0),
    Column("interceptions", Integer, default=0),
    Column("sacks", Integer, default=0),
    Column("sack_yards", Integer, default=0),
    Index("ix_pgp_player_id", "player_id"),
    Index("ix_pgp_season", "season"),
)

player_game_rushing_table = Table(
    "player_game_rushing", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("player_id", Integer, ForeignKey("players.player_id")),
    Column("player", Text, nullable=False),
    Column("team", String(10)),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("att", Integer, default=0),
    Column("yards", Integer, default=0),
    Column("td", Integer, default=0),
    Column("fumbles", Integer, default=0),
    Index("ix_pgru_player_id", "player_id"),
    Index("ix_pgru_season", "season"),
)

player_game_receiving_table = Table(
    "player_game_receiving", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("player_id", Integer, ForeignKey("players.player_id")),
    Column("player", Text, nullable=False),
    Column("team", String(10)),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("receptions", Integer, default=0),
    Column("yards", Integer, default=0),
    Column("td", Integer, default=0),
    Column("fumbles", Integer, default=0),
    Index("ix_pgrec_player_id", "player_id"),
    Index("ix_pgrec_season", "season"),
)

player_game_defensive_table = Table(
    "player_game_defensive", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("player_id", Integer, ForeignKey("players.player_id")),
    Column("player", Text, nullable=False),
    Column("team", String(10)),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("tackles", Integer, default=0),
    Column("sacks", Float, default=0),
    Column("interceptions", Integer, default=0),
    Column("fumble_recoveries", Integer, default=0),
    Index("ix_pgd_player_id", "player_id"),
    Index("ix_pgd_season", "season"),
)

# ---------------------------------------------------------------------------
# Engine / connection
# ---------------------------------------------------------------------------


def get_engine(database_url: str | None = None) -> Engine:
    url = database_url or os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    return create_engine(url)


def create_tables(engine: Engine) -> None:
    metadata.create_all(engine)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _resolve_player_id(registry: PlayerRegistry, name: str | None, season: int, team: str | None) -> int | None:
    if not name or not registry:
        return None
    return registry.get_or_create(name, season, team)


def load_registry(engine: Engine, registry: PlayerRegistry) -> None:
    """Write the in-memory player registry to PostgreSQL."""
    with engine.begin() as conn:
        for p in registry.all_players():
            conn.execute(insert(players_table).values(
                player_id=p["player_id"],
                canonical_name=p["canonical_name"],
                first_seen_season=p["first_seen_season"],
                last_seen_season=p["last_seen_season"],
            ))
        for pid, aliases in ((pid, registry.get_aliases(pid)) for pid in (p["player_id"] for p in registry.all_players())):
            for alias in aliases:
                conn.execute(insert(player_names_table).values(
                    player_id=pid,
                    name=alias["name"],
                    season=alias["season"],
                    team=alias["team"],
                ))


def load_season(
    engine: Engine,
    games: list[Game],
    registry: PlayerRegistry,
) -> None:
    """Load all plays and stats for a list of games into PostgreSQL."""
    with engine.begin() as conn:
        for game in games:
            _load_game_plays(conn, game, registry)
            _load_game_stats(conn, game, registry)


def _load_game_plays(conn, game: Game, registry: PlayerRegistry) -> None:
    """Insert all plays for a game."""
    for idx, play in enumerate(game.plays):
        team_abbr = None
        if play.possession_team_id == game.home_team_id:
            team_abbr = game.home_team
        elif play.possession_team_id == game.away_team_id:
            team_abbr = game.away_team

        conn.execute(insert(plays_table).values(
            game_id=game.id,
            season=game.season,
            league=game.league,
            quarter=play.quarter,
            clock=play.clock,
            play_index=idx,
            play_type=play.play_type.value,
            description=play.description,
            css=play.css,
            down=play.down,
            distance=play.distance,
            distance_text=play.distance_text,
            yard_line=play.yard_line,
            yard_line_team=play.yard_line_team,
            possession_team_id=play.possession_team_id,
            score_away=play.score_away,
            score_home=play.score_home,
            away_team=play.away_team,
            home_team=play.home_team,
            yards_gained=play.yards_gained,
            first_down=play.first_down,
            touchdown=play.touchdown,
            fumble=play.fumble,
            fumble_lost=play.fumble_lost,
            interception=play.interception,
            safety=play.safety,
            turnover_on_downs=play.turnover_on_downs,
            penalty=play.penalty,
            penalty_team=play.penalty_team,
            penalty_type=play.penalty_type,
            penalty_auto_first=play.penalty_auto_first,
            passer=play.passer,
            rusher=play.rusher,
            receiver=play.receiver,
            tackler=play.tackler,
            sacker=play.sacker,
            interceptor=play.interceptor,
            kicker=play.kicker,
            returner=play.returner,
            fumbler=play.fumbler,
            fumble_recoverer=play.fumble_recoverer,
            player_id_passer=_resolve_player_id(registry, play.passer, game.season, team_abbr),
            player_id_rusher=_resolve_player_id(registry, play.rusher, game.season, team_abbr),
            player_id_receiver=_resolve_player_id(registry, play.receiver, game.season, team_abbr),
            player_id_tackler=_resolve_player_id(registry, play.tackler, game.season, None),
            player_id_sacker=_resolve_player_id(registry, play.sacker, game.season, None),
            player_id_interceptor=_resolve_player_id(registry, play.interceptor, game.season, None),
            player_id_kicker=_resolve_player_id(registry, play.kicker, game.season, team_abbr),
            player_id_returner=_resolve_player_id(registry, play.returner, game.season, None),
            kick_yards=play.kick_yards,
            fg_distance=play.fg_distance,
            fg_good=play.fg_good,
            pat_good=play.pat_good,
        ))


def _load_game_stats(conn, game: Game, registry: PlayerRegistry) -> None:
    """Insert team and player game stats."""
    # Team stats
    for tg in game_team_stats(game):
        conn.execute(insert(team_games_table).values(**tg.model_dump()))

    # Player passing
    for ps in game_player_passing(game, registry):
        conn.execute(insert(player_game_passing_table).values(**ps.model_dump()))

    # Player rushing
    for rs in game_player_rushing(game, registry):
        conn.execute(insert(player_game_rushing_table).values(**rs.model_dump()))

    # Player receiving
    for rc in game_player_receiving(game, registry):
        conn.execute(insert(player_game_receiving_table).values(**rc.model_dump()))

    # Player defensive
    for df in game_player_defensive(game, registry):
        conn.execute(insert(player_game_defensive_table).values(**df.model_dump()))


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


def query_plays(engine: Engine, **filters) -> list[dict]:
    """Query plays with optional filters: season, game_id, play_type, player_id."""
    stmt = select(plays_table)
    if "season" in filters:
        stmt = stmt.where(plays_table.c.season == filters["season"])
    if "game_id" in filters:
        stmt = stmt.where(plays_table.c.game_id == filters["game_id"])
    if "play_type" in filters:
        stmt = stmt.where(plays_table.c.play_type == filters["play_type"])
    if "player_id" in filters:
        pid = filters["player_id"]
        stmt = stmt.where(
            (plays_table.c.player_id_passer == pid)
            | (plays_table.c.player_id_rusher == pid)
            | (plays_table.c.player_id_receiver == pid)
            | (plays_table.c.player_id_tackler == pid)
            | (plays_table.c.player_id_sacker == pid)
            | (plays_table.c.player_id_interceptor == pid)
            | (plays_table.c.player_id_kicker == pid)
            | (plays_table.c.player_id_returner == pid)
        )
    if "limit" in filters:
        stmt = stmt.limit(filters["limit"])
    if "offset" in filters:
        stmt = stmt.offset(filters["offset"])

    with engine.connect() as conn:
        result = conn.execute(stmt)
        return [dict(row._mapping) for row in result]


def query_player_plays(engine: Engine, player_id: int, **filters) -> list[dict]:
    """Query all plays involving a specific player."""
    return query_plays(engine, player_id=player_id, **filters)
