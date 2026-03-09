"""PostgreSQL storage layer using SQLAlchemy Core.

Schema:
- players / player_names: player registry
- plays: one row per ParsedPlay with player_id foreign keys
- team_games: per-team per-game stats
- player_game_passing/rushing/receiving/defensive: per-player per-game stats
"""

from __future__ import annotations

import logging

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
    func,
    insert,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from isfl_epa.parser.schema import Game, PlayType
from isfl_epa.players.registry import PlayerRegistry, _normalize
from isfl_epa.stats.aggregation import (
    game_player_defensive,
    game_player_passing,
    game_player_receiving,
    game_player_rushing,
    game_team_stats,
)

from isfl_epa.config import get_database_url

logger = logging.getLogger(__name__)

metadata = MetaData()

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------

games_table = Table(
    "games", metadata,
    Column("game_id", Integer, primary_key=True),
    Column("season", Integer, nullable=False),
    Column("league", String(50)),
    Column("game_type", String(20), nullable=False, server_default="regular"),
    Index("ix_games_season", "season"),
    Index("ix_games_type", "game_type"),
)

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
    Column("team", String(50)),
    Index("ix_player_names_unique", "name", "season", unique=True),
)

plays_table = Table(
    "plays", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("league", String(50)),
    Column("game_type", String(20), server_default="regular"),
    Column("quarter", Integer),
    Column("clock", String(50)),
    Column("play_index", Integer),
    Column("play_type", String(20)),
    Column("description", Text),
    Column("css", String(5)),
    # Situation
    Column("down", Integer),
    Column("distance", Integer),
    Column("distance_text", String(20)),
    Column("yard_line", Integer),
    Column("yard_line_team", String(50)),
    Column("possession_team_id", Integer),
    # Score
    Column("score_away", Integer),
    Column("score_home", Integer),
    Column("away_team", String(50)),
    Column("home_team", String(50)),
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
    Column("penalty_team", Text),
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
    Column("team", String(50), nullable=False),
    Column("opponent", String(50)),
    Column("game_type", String(20), server_default="regular"),
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
    Column("team", String(50)),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("game_type", String(20), server_default="regular"),
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
    Column("team", String(50)),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("game_type", String(20), server_default="regular"),
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
    Column("team", String(50)),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("game_type", String(20), server_default="regular"),
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
    Column("team", String(50)),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("game_type", String(20), server_default="regular"),
    Column("tackles", Integer, default=0),
    Column("sacks", Float, default=0),
    Column("interceptions", Integer, default=0),
    Column("fumble_recoveries", Integer, default=0),
    Index("ix_pgd_player_id", "player_id"),
    Index("ix_pgd_season", "season"),
)

play_epa_table = Table(
    "play_epa", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("play_id", Integer, ForeignKey("plays.id"), unique=True),
    Column("game_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("ep_before", Float),
    Column("ep_after", Float),
    Column("epa", Float),
    Index("ix_play_epa_game_id", "game_id"),
    Index("ix_play_epa_season", "season"),
)

player_season_epa_table = Table(
    "player_season_epa", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("player_id", Integer, ForeignKey("players.player_id")),
    Column("player", Text, nullable=False),
    Column("team", String(50)),
    Column("season", Integer, nullable=False),
    Column("game_type", String(20), server_default="regular"),
    Column("pass_epa", Float, default=0),
    Column("dropbacks", Integer, default=0),
    Column("epa_per_dropback", Float, default=0),
    Column("rush_epa", Float, default=0),
    Column("rush_attempts", Integer, default=0),
    Column("epa_per_rush", Float, default=0),
    Column("recv_epa", Float, default=0),
    Column("targets", Integer, default=0),
    Column("epa_per_target", Float, default=0),
    # Defensive EPA (attributed to tackler/sacker/interceptor)
    Column("def_epa", Float, default=0),
    Column("def_plays", Integer, default=0),
    Column("epa_per_def_play", Float, default=0),
    Index("ix_pse_player_id", "player_id"),
    Index("ix_pse_season", "season"),
)

player_positions_table = Table(
    "player_positions", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("player_id", Integer, ForeignKey("players.player_id")),
    Column("index_player_id", Integer),
    Column("season", Integer, nullable=False),
    Column("team", String(50)),
    Column("position", String(10), nullable=False),
    Column("overall", Integer),
    Index("ix_ppos_player_id", "player_id"),
    Index("ix_ppos_season", "season"),
    Index("ix_ppos_position", "position"),
    Index("ix_ppos_unique", "player_id", "season", unique=True),
)

team_season_epa_table = Table(
    "team_season_epa", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("team", String(50), nullable=False),
    Column("season", Integer, nullable=False),
    Column("game_type", String(20), server_default="regular"),
    Column("total_epa", Float, default=0),
    Column("pass_epa", Float, default=0),
    Column("rush_epa", Float, default=0),
    Column("plays", Integer, default=0),
    Column("epa_per_play", Float, default=0),
    Index("ix_tse_season", "season"),
    Index("ix_tse_team", "team"),
)

# ---------------------------------------------------------------------------
# Engine / connection
# ---------------------------------------------------------------------------


def get_engine(database_url: str | None = None) -> Engine:
    url = database_url or get_database_url()
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


def init_registry_from_db(engine: Engine, registry: PlayerRegistry) -> None:
    """Seed an empty registry with existing player data so IDs are stable across builds."""
    with engine.connect() as conn:
        # Load canonical player records
        players = conn.execute(select(players_table)).fetchall()
        for p in players:
            registry._players[p.player_id] = {
                "canonical_name": p.canonical_name,
                "first_seen_season": p.first_seen_season,
                "last_seen_season": p.last_seen_season,
            }
            if p.player_id not in registry._aliases:
                registry._aliases[p.player_id] = []

        # Load all known name aliases so get_or_create finds them
        rows = conn.execute(select(player_names_table)).fetchall()
        for row in rows:
            norm = _normalize(row.name)
            if norm not in registry._name_to_id:
                registry._name_to_id[norm] = row.player_id
            registry._aliases.setdefault(row.player_id, [])
            alias = {"name": row.name, "season": row.season, "team": row.team}
            if alias not in registry._aliases[row.player_id]:
                registry._aliases[row.player_id].append(alias)

        # Set next_id beyond the current max so new players get unique IDs
        max_id = conn.execute(select(func.max(players_table.c.player_id))).scalar() or 0
        registry._next_id = max_id + 1


def load_registry(engine: Engine, registry: PlayerRegistry) -> None:
    """Write the in-memory player registry to PostgreSQL (upsert-safe)."""
    with engine.begin() as conn:
        for p in registry.all_players():
            conn.execute(
                pg_insert(players_table).values(
                    player_id=p["player_id"],
                    canonical_name=p["canonical_name"],
                    first_seen_season=p["first_seen_season"],
                    last_seen_season=p["last_seen_season"],
                ).on_conflict_do_update(
                    index_elements=["player_id"],
                    set_={
                        "first_seen_season": p["first_seen_season"],
                        "last_seen_season": p["last_seen_season"],
                    },
                )
            )
        for pid, aliases in ((pid, registry.get_aliases(pid)) for pid in (p["player_id"] for p in registry.all_players())):
            for alias in aliases:
                conn.execute(
                    pg_insert(player_names_table).values(
                        player_id=pid,
                        name=alias["name"],
                        season=alias["season"],
                        team=alias["team"],
                    ).on_conflict_do_nothing()
                )


def load_season(
    engine: Engine,
    games: list[Game],
    registry: PlayerRegistry,
) -> None:
    """Load all plays and stats for a list of games into PostgreSQL (batch)."""
    all_plays = []
    all_team_stats = []
    all_passing = []
    all_rushing = []
    all_receiving = []
    all_defensive = []

    # Build game_id -> game_type lookup
    game_type_map = {game.id: game.game_type for game in games}

    for game in games:
        all_plays.extend(_build_play_dicts(game, registry))
        for tg in game_team_stats(game):
            d = tg.model_dump()
            d["game_type"] = game.game_type
            all_team_stats.append(d)
        for ps in game_player_passing(game, registry):
            d = ps.model_dump()
            d["game_type"] = game.game_type
            all_passing.append(d)
        for rs in game_player_rushing(game, registry):
            d = rs.model_dump()
            d["game_type"] = game.game_type
            all_rushing.append(d)
        for rc in game_player_receiving(game, registry):
            d = rc.model_dump()
            d["game_type"] = game.game_type
            all_receiving.append(d)
        for dd in game_player_defensive(game, registry):
            d = dd.model_dump()
            d["game_type"] = game.game_type
            all_defensive.append(d)

    # Determine season from the games being loaded
    season = games[0].season if games else None

    # Build games table rows
    all_games = [
        {"game_id": game.id, "season": game.season, "league": game.league, "game_type": game.game_type}
        for game in games
    ]

    with engine.begin() as conn:
        # Clear existing data for this season to avoid duplicates on re-runs
        if season is not None:
            for table in (
                player_game_defensive_table,
                player_game_receiving_table,
                player_game_rushing_table,
                player_game_passing_table,
                team_games_table,
                plays_table,
            ):
                conn.execute(table.delete().where(table.c.season == season))
            conn.execute(games_table.delete().where(games_table.c.season == season))

        if all_games:
            conn.execute(
                pg_insert(games_table).on_conflict_do_update(
                    index_elements=["game_id"],
                    set_={"game_type": pg_insert(games_table).excluded.game_type},
                ),
                all_games,
            )
        if all_plays:
            conn.execute(insert(plays_table), all_plays)
        if all_team_stats:
            conn.execute(insert(team_games_table), all_team_stats)
        if all_passing:
            conn.execute(insert(player_game_passing_table), all_passing)
        if all_rushing:
            conn.execute(insert(player_game_rushing_table), all_rushing)
        if all_receiving:
            conn.execute(insert(player_game_receiving_table), all_receiving)
        if all_defensive:
            conn.execute(insert(player_game_defensive_table), all_defensive)

    logger.info(
        "load_season: batch inserted %d plays, %d team stats, "
        "%d passing, %d rushing, %d receiving, %d defensive rows",
        len(all_plays), len(all_team_stats), len(all_passing),
        len(all_rushing), len(all_receiving), len(all_defensive),
    )


def _build_play_dicts(game: Game, registry: PlayerRegistry) -> list[dict]:
    """Build list of play insert dicts for a game."""
    rows = []
    for idx, play in enumerate(game.plays):
        team_abbr = None
        if play.possession_team_id == game.home_team_id:
            team_abbr = game.home_team
        elif play.possession_team_id == game.away_team_id:
            team_abbr = game.away_team

        rows.append({
            "game_id": game.id,
            "season": game.season,
            "league": game.league,
            "game_type": game.game_type,
            "quarter": play.quarter,
            "clock": play.clock,
            "play_index": idx,
            "play_type": play.play_type.value,
            "description": play.description,
            "css": play.css,
            "down": play.down,
            "distance": play.distance,
            "distance_text": play.distance_text,
            "yard_line": play.yard_line,
            "yard_line_team": play.yard_line_team,
            "possession_team_id": play.possession_team_id,
            "score_away": play.score_away,
            "score_home": play.score_home,
            "away_team": play.away_team,
            "home_team": play.home_team,
            "yards_gained": play.yards_gained,
            "first_down": play.first_down,
            "touchdown": play.touchdown,
            "fumble": play.fumble,
            "fumble_lost": play.fumble_lost,
            "interception": play.interception,
            "safety": play.safety,
            "turnover_on_downs": play.turnover_on_downs,
            "penalty": play.penalty,
            "penalty_team": play.penalty_team,
            "penalty_type": play.penalty_type,
            "penalty_auto_first": play.penalty_auto_first,
            "passer": play.passer,
            "rusher": play.rusher,
            "receiver": play.receiver,
            "tackler": play.tackler,
            "sacker": play.sacker,
            "interceptor": play.interceptor,
            "kicker": play.kicker,
            "returner": play.returner,
            "fumbler": play.fumbler,
            "fumble_recoverer": play.fumble_recoverer,
            "player_id_passer": _resolve_player_id(registry, play.passer, game.season, team_abbr),
            "player_id_rusher": _resolve_player_id(registry, play.rusher, game.season, team_abbr),
            "player_id_receiver": _resolve_player_id(registry, play.receiver, game.season, team_abbr),
            "player_id_tackler": _resolve_player_id(registry, play.tackler, game.season, None),
            "player_id_sacker": _resolve_player_id(registry, play.sacker, game.season, None),
            "player_id_interceptor": _resolve_player_id(registry, play.interceptor, game.season, None),
            "player_id_kicker": _resolve_player_id(registry, play.kicker, game.season, team_abbr),
            "player_id_returner": _resolve_player_id(registry, play.returner, game.season, None),
            "kick_yards": play.kick_yards,
            "fg_distance": play.fg_distance,
            "fg_good": play.fg_good,
            "pat_good": play.pat_good,
        })
    return rows


def load_epa_season(engine: Engine, epa_df, season: int) -> None:
    """Load EPA results for a season into PostgreSQL (upsert-safe)."""
    import pandas as pd

    # Enrich epa_df with game_type from the games table
    if "game_type" not in epa_df.columns:
        with engine.connect() as rconn:
            gt_rows = rconn.execute(
                select(games_table.c.game_id, games_table.c.game_type)
                .where(games_table.c.season == season)
            ).fetchall()
        gt_map = {r.game_id: r.game_type for r in gt_rows}
        epa_df = epa_df.copy()
        epa_df["game_type"] = epa_df["game_id"].map(gt_map).fillna("regular")

    with engine.begin() as conn:
        # Clear existing EPA data for this season
        conn.execute(play_epa_table.delete().where(play_epa_table.c.season == season))
        conn.execute(player_season_epa_table.delete().where(player_season_epa_table.c.season == season))
        conn.execute(team_season_epa_table.delete().where(team_season_epa_table.c.season == season))

        # Exclude preseason from all EPA storage
        epa_df = epa_df[epa_df["game_type"] != "preseason"]

        # Insert per-play EPA (only plays with valid EPA) — batch insert
        valid = epa_df.dropna(subset=["epa"])
        if "id" in valid.columns:
            rows = [
                {
                    "play_id": int(r.id) if pd.notna(r.id) else None,
                    "game_id": int(r.game_id),
                    "season": season,
                    "ep_before": float(r.ep_before),
                    "ep_after": float(r.ep_after),
                    "epa": float(r.epa),
                }
                for r in valid.itertuples(index=False)
                if pd.notna(r.id)
            ]
            if rows:
                conn.execute(insert(play_epa_table), rows)
                logger.info("load_epa_season: inserted %d play EPA rows (batch)", len(rows))

        # Aggregate and insert player/team EPA separately for each game_type
        for game_type in ("regular", "playoff"):
            gt_df = epa_df[epa_df["game_type"] == game_type]
            if gt_df.empty:
                continue
            _load_player_epa(conn, gt_df, season, game_type)
            _load_team_epa(conn, gt_df, season, game_type)


def _load_player_epa(conn, epa_df, season: int, game_type: str = "regular") -> None:
    """Aggregate and insert per-player EPA stats."""
    import pandas as pd

    from isfl_epa.players.registry import _strip_special, _strip_tags

    def _clean_name(name):
        return _strip_special(_strip_tags(name))

    valid = epa_df.dropna(subset=["epa"])
    logger.info("_load_player_epa: S%d %s — %d valid EPA plays", season, game_type, len(valid))

    _insert_passing_epa(conn, valid, season, _clean_name, game_type)
    _upsert_rushing_epa(conn, valid, season, _clean_name, game_type)
    _upsert_receiving_epa(conn, valid, season, _clean_name, game_type)
    _upsert_defensive_epa(conn, valid, season, _clean_name, game_type)


def _insert_passing_epa(conn, valid, season: int, strip_fn, game_type: str = "regular") -> None:
    """Aggregate and insert passing EPA (creates new rows) — batch."""
    import pandas as pd

    pass_plays = valid[valid["passer"].notna() & valid["play_type"].isin(["pass", "sack"])]
    if pass_plays.empty:
        return

    pass_agg = pass_plays.groupby(["player_id_passer"]).agg(
        passer=("passer", "first"),
        possession_team=("possession_team", "first"),
        pass_epa=("epa", "sum"),
        dropbacks=("epa", "count"),
    ).reset_index()
    pass_agg["passer"] = pass_agg["passer"].apply(strip_fn)
    pass_agg["epa_per_dropback"] = pass_agg["pass_epa"] / pass_agg["dropbacks"]
    logger.debug("_insert_passing_epa: %d passers (%s)", len(pass_agg), game_type)

    rows = [
        {
            "player_id": int(row.player_id_passer) if pd.notna(row.player_id_passer) else None,
            "player": row.passer,
            "team": getattr(row, "possession_team", None),
            "season": season,
            "game_type": game_type,
            "pass_epa": float(row.pass_epa),
            "dropbacks": int(row.dropbacks),
            "epa_per_dropback": float(row.epa_per_dropback),
        }
        for row in pass_agg.itertuples(index=False)
    ]
    if rows:
        conn.execute(insert(player_season_epa_table), rows)


def _fetch_existing_player_epa(conn, season: int, game_type: str = "regular") -> dict[int, int]:
    """Fetch all existing player_season_epa rows for a season and game_type.

    Returns dict mapping player_id -> row id (for targeted updates).
    """
    t = player_season_epa_table
    rows = conn.execute(
        select(t.c.player_id, t.c.id)
        .where(t.c.season == season)
        .where(t.c.game_type == game_type)
    ).fetchall()
    return {r.player_id: r.id for r in rows if r.player_id is not None}


def _upsert_rushing_epa(conn, valid, season: int, strip_fn, game_type: str = "regular") -> None:
    """Aggregate and upsert rushing EPA — batch with single SELECT."""
    import pandas as pd

    rush_plays = valid[(valid["rusher"].notna()) & (valid["play_type"] == "rush")]
    if rush_plays.empty:
        return

    rush_agg = rush_plays.groupby(["player_id_rusher"]).agg(
        rusher=("rusher", "first"),
        possession_team=("possession_team", "first"),
        rush_epa=("epa", "sum"),
        rush_attempts=("epa", "count"),
    ).reset_index()
    rush_agg["rusher"] = rush_agg["rusher"].apply(strip_fn)
    rush_agg["epa_per_rush"] = rush_agg["rush_epa"] / rush_agg["rush_attempts"]
    logger.debug("_upsert_rushing_epa: %d rushers (%s)", len(rush_agg), game_type)

    # Single SELECT to find all existing rows for this season + game_type
    existing = _fetch_existing_player_epa(conn, season, game_type)

    insert_rows = []
    for row in rush_agg.itertuples(index=False):
        pid = int(row.player_id_rusher) if pd.notna(row.player_id_rusher) else None
        vals = {
            "rush_epa": float(row.rush_epa),
            "rush_attempts": int(row.rush_attempts),
            "epa_per_rush": float(row.epa_per_rush),
        }
        if pid is not None and pid in existing:
            conn.execute(
                player_season_epa_table.update()
                .where(player_season_epa_table.c.id == existing[pid])
                .values(**vals)
            )
        else:
            insert_rows.append({
                "player_id": pid, "player": row.rusher,
                "team": getattr(row, "possession_team", None), "season": season,
                "game_type": game_type,
                **vals,
            })
    if insert_rows:
        conn.execute(insert(player_season_epa_table), insert_rows)


def _upsert_receiving_epa(conn, valid, season: int, strip_fn, game_type: str = "regular") -> None:
    """Aggregate and upsert receiving EPA — batch with single SELECT."""
    import pandas as pd

    recv_plays = valid[(valid["receiver"].notna()) & (valid["play_type"] == "pass")]
    if recv_plays.empty:
        return

    recv_agg = recv_plays.groupby(["player_id_receiver"]).agg(
        receiver=("receiver", "first"),
        possession_team=("possession_team", "first"),
        recv_epa=("epa", "sum"),
        targets=("epa", "count"),
    ).reset_index()
    recv_agg["receiver"] = recv_agg["receiver"].apply(strip_fn)
    recv_agg["epa_per_target"] = recv_agg["recv_epa"] / recv_agg["targets"]
    logger.debug("_upsert_receiving_epa: %d receivers (%s)", len(recv_agg), game_type)

    existing = _fetch_existing_player_epa(conn, season, game_type)

    insert_rows = []
    for row in recv_agg.itertuples(index=False):
        pid = int(row.player_id_receiver) if pd.notna(row.player_id_receiver) else None
        vals = {
            "recv_epa": float(row.recv_epa),
            "targets": int(row.targets),
            "epa_per_target": float(row.epa_per_target),
        }
        if pid is not None and pid in existing:
            conn.execute(
                player_season_epa_table.update()
                .where(player_season_epa_table.c.id == existing[pid])
                .values(**vals)
            )
        else:
            insert_rows.append({
                "player_id": pid, "player": row.receiver,
                "team": getattr(row, "possession_team", None), "season": season,
                "game_type": game_type,
                **vals,
            })
    if insert_rows:
        conn.execute(insert(player_season_epa_table), insert_rows)


def _upsert_defensive_epa(conn, valid, season: int, strip_fn, game_type: str = "regular") -> None:
    """Aggregate and upsert defensive EPA (sacker > interceptor > tackler)."""
    import pandas as pd

    def _def_player(row):
        sacker_id = getattr(row, "player_id_sacker", None)
        if pd.notna(sacker_id):
            return int(sacker_id), getattr(row, "sacker", None)
        int_id = getattr(row, "player_id_interceptor", None)
        if pd.notna(int_id):
            return int(int_id), getattr(row, "interceptor", None)
        tackler_id = getattr(row, "player_id_tackler", None)
        if pd.notna(tackler_id):
            return int(tackler_id), getattr(row, "tackler", None)
        return None, None

    scrimmage = valid[valid["play_type"].isin(["pass", "rush", "sack"])]

    # Build per-game team pair lookup for defensive team resolution
    _game_teams: dict[int, list[str]] = {}
    if "possession_team" in scrimmage.columns:
        for gid, grp in scrimmage.groupby("game_id"):
            teams = grp["possession_team"].dropna().unique().tolist()
            _game_teams[int(gid)] = teams

    def _def_team(row):
        poss = getattr(row, "possession_team", None)
        gid = getattr(row, "game_id", None)
        if not poss or pd.isna(poss) or gid is None:
            return None
        teams = _game_teams.get(int(gid), [])
        opponents = [t for t in teams if t != poss]
        return opponents[0] if opponents else None

    def_records = []
    for row in scrimmage.itertuples(index=False):
        pid, pname = _def_player(row)
        if pid is not None:
            def_records.append({
                "player_id": pid, "player": pname, "epa": row.epa,
                "team": _def_team(row),
            })

    if not def_records:
        return

    def_df = pd.DataFrame(def_records)
    def_agg = def_df.groupby(["player_id"]).agg(
        player=("player", "first"),
        team=("team", "first"),
        def_epa=("epa", "sum"),
        def_plays=("epa", "count"),
    ).reset_index()
    def_agg["epa_per_def_play"] = def_agg["def_epa"] / def_agg["def_plays"]
    def_agg["player"] = def_agg["player"].apply(strip_fn)
    logger.debug("_upsert_defensive_epa: %d defenders (%s)", len(def_agg), game_type)

    existing = _fetch_existing_player_epa(conn, season, game_type)
    # Also fetch team info for rows that might need team backfill
    existing_teams: dict[int, str | None] = {}
    if existing:
        t = player_season_epa_table
        team_rows = conn.execute(
            select(t.c.player_id, t.c.team).where(
                (t.c.season == season) & (t.c.game_type == game_type)
            )
        ).fetchall()
        existing_teams = {r.player_id: r.team for r in team_rows if r.player_id is not None}

    insert_rows = []
    for row in def_agg.itertuples(index=False):
        pid = int(row.player_id)
        vals = {
            "def_epa": float(row.def_epa),
            "def_plays": int(row.def_plays),
            "epa_per_def_play": float(row.epa_per_def_play),
        }
        if pid in existing:
            if not existing_teams.get(pid) and row.team:
                vals["team"] = row.team
            conn.execute(
                player_season_epa_table.update()
                .where(player_season_epa_table.c.id == existing[pid])
                .values(**vals)
            )
        else:
            insert_rows.append({
                "player_id": pid,
                "player": row.player,
                "team": row.team,
                "season": season,
                "game_type": game_type,
                **vals,
            })
    if insert_rows:
        conn.execute(insert(player_season_epa_table), insert_rows)


def _load_team_epa(conn, epa_df, season: int, game_type: str = "regular") -> None:
    """Aggregate and insert per-team EPA stats."""
    valid = epa_df.dropna(subset=["epa"])
    scrimmage = valid[valid["play_type"].isin(["pass", "rush", "sack"])]
    if scrimmage.empty:
        return

    team_agg = scrimmage.groupby("possession_team").agg(
        total_epa=("epa", "sum"),
        plays=("epa", "count"),
    ).reset_index()
    team_agg["epa_per_play"] = team_agg["total_epa"] / team_agg["plays"]

    # Pass/rush split
    pass_epa = scrimmage[scrimmage["play_type"].isin(["pass", "sack"])].groupby("possession_team")["epa"].sum()
    rush_epa = scrimmage[scrimmage["play_type"] == "rush"].groupby("possession_team")["epa"].sum()

    rows = [
        {
            "team": row.possession_team,
            "season": season,
            "game_type": game_type,
            "total_epa": float(row.total_epa),
            "pass_epa": float(pass_epa.get(row.possession_team, 0)),
            "rush_epa": float(rush_epa.get(row.possession_team, 0)),
            "plays": int(row.plays),
            "epa_per_play": float(row.epa_per_play),
        }
        for row in team_agg.itertuples(index=False)
    ]
    if rows:
        conn.execute(insert(team_season_epa_table), rows)


def load_player_positions(
    engine: Engine,
    roster_entries: list[dict],
    season: int,
) -> dict[str, int]:
    """Load player position data from scraped roster entries.

    Args:
        engine: SQLAlchemy engine
        roster_entries: list of dicts with keys: player_id, position, overall,
                        index_player_id, team (optional)
        season: season number

    Returns:
        dict with 'matched' and 'unmatched' counts.
    """
    matched = 0
    unmatched = 0

    with engine.begin() as conn:
        # Clear existing position data for this season
        conn.execute(
            player_positions_table.delete().where(
                player_positions_table.c.season == season
            )
        )

        for entry in roster_entries:
            pid = entry.get("player_id")
            if pid is None:
                unmatched += 1
                continue

            conn.execute(
                pg_insert(player_positions_table).values(
                    player_id=pid,
                    index_player_id=entry.get("index_player_id"),
                    season=season,
                    team=entry.get("team"),
                    position=entry["position"],
                    overall=entry.get("overall"),
                ).on_conflict_do_update(
                    index_elements=["player_id", "season"],
                    set_={
                        "position": entry["position"],
                        "overall": entry.get("overall"),
                        "index_player_id": entry.get("index_player_id"),
                        "team": entry.get("team"),
                    },
                )
            )
            matched += 1

    return {"matched": matched, "unmatched": unmatched}


# ---------------------------------------------------------------------------
# Team ID mapping
# ---------------------------------------------------------------------------


def get_team_id_to_abbr(engine: Engine, season: int) -> dict[int, str]:
    """Build team_id -> team abbreviation mapping using intersection.

    Team IDs are fixed per team across all games.  For each ptid, intersect
    {home_team, away_team} across all games.  After 2+ games vs different
    opponents, only the correct team remains.
    """
    p = plays_table
    # Get distinct (game_id, home_team, away_team, possession_team_id) tuples
    stmt = (
        select(
            p.c.game_id,
            p.c.home_team,
            p.c.away_team,
            p.c.possession_team_id,
        )
        .where(p.c.season == season)
        .where(p.c.possession_team_id.isnot(None))
        .where(p.c.home_team.isnot(None))
        .where(p.c.away_team.isnot(None))
        .distinct()
    )
    ptid_candidates: dict[int, set[str]] = {}
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            teams = {row.home_team, row.away_team}
            ptid = int(row.possession_team_id)
            if ptid not in ptid_candidates:
                ptid_candidates[ptid] = teams.copy()
            else:
                ptid_candidates[ptid] &= teams

    return {
        ptid: next(iter(teams))
        for ptid, teams in ptid_candidates.items()
        if len(teams) == 1
    }


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


# ---------------------------------------------------------------------------
# Duplicate player detection and merging
# ---------------------------------------------------------------------------

def find_duplicate_players(engine: Engine) -> list[dict]:
    """Find player_ids that share the same normalized name.

    Returns list of dicts with keys: normalized_name, player_ids, names.
    The first player_id in each group is the suggested keep_id (lowest).
    """
    from collections import defaultdict

    with engine.connect() as conn:
        rows = conn.execute(select(player_names_table)).fetchall()

    # Group by normalized name → set of player_ids
    groups: dict[str, dict] = defaultdict(lambda: {"player_ids": set(), "names": set()})
    for row in rows:
        norm = _normalize(row.name)
        groups[norm]["player_ids"].add(row.player_id)
        groups[norm]["names"].add(row.name)

    # Return only groups with 2+ distinct player_ids
    duplicates = []
    for norm, info in sorted(groups.items()):
        if len(info["player_ids"]) >= 2:
            pids = sorted(info["player_ids"])
            duplicates.append({
                "normalized_name": norm,
                "player_ids": pids,
                "keep_id": pids[0],
                "remove_ids": pids[1:],
                "names": sorted(info["names"]),
            })
    return duplicates


def merge_players_db(engine: Engine, merge_pairs: list[tuple[int, int]]) -> int:
    """Batch-merge duplicate player IDs. Each pair is (keep_id, remove_id).

    All merges happen in a single transaction for speed. Returns count of merges.
    """
    from sqlalchemy import delete, text, update

    if not merge_pairs:
        return 0

    logger.info("merge_players_db: merging %d pairs", len(merge_pairs))

    # Build mapping: remove_id -> keep_id
    id_map = {remove: keep for keep, remove in merge_pairs}
    remove_ids = list(id_map.keys())

    plays_pid_col_names = [
        "player_id_passer", "player_id_rusher", "player_id_receiver",
        "player_id_tackler", "player_id_sacker", "player_id_interceptor",
        "player_id_kicker", "player_id_returner",
    ]

    delete_tables = [
        player_season_epa_table,
        player_game_passing_table,
        player_game_rushing_table,
        player_game_receiving_table,
        player_game_defensive_table,
    ]

    with engine.begin() as conn:
        # 1. Update plays table — per-pair parameterized updates
        for remove_id, keep_id in id_map.items():
            for col_name in plays_pid_col_names:
                conn.execute(text(
                    f"UPDATE plays SET {col_name} = :keep_id "
                    f"WHERE {col_name} = :remove_id"
                ), {"keep_id": keep_id, "remove_id": remove_id})

        # 2. player_names — reassign, handling unique constraint conflicts
        # First find which (name, season) pairs already exist under keep_ids
        # Delete conflicting rows, then bulk reassign the rest
        for remove_id, keep_id in id_map.items():
            # Delete rows that would conflict
            conn.execute(text("""
                DELETE FROM player_names pn1
                WHERE pn1.player_id = :remove_id
                AND EXISTS (
                    SELECT 1 FROM player_names pn2
                    WHERE pn2.player_id = :keep_id
                    AND pn2.name = pn1.name AND pn2.season = pn1.season
                )
            """), {"remove_id": remove_id, "keep_id": keep_id})
            # Reassign remaining
            conn.execute(text(
                "UPDATE player_names SET player_id = :keep_id "
                "WHERE player_id = :remove_id"
            ), {"keep_id": keep_id, "remove_id": remove_id})

        # 3. player_positions — same pattern
        for remove_id, keep_id in id_map.items():
            conn.execute(text("""
                DELETE FROM player_positions pp1
                WHERE pp1.player_id = :remove_id
                AND EXISTS (
                    SELECT 1 FROM player_positions pp2
                    WHERE pp2.player_id = :keep_id
                    AND pp2.season = pp1.season
                )
            """), {"remove_id": remove_id, "keep_id": keep_id})
            conn.execute(text(
                "UPDATE player_positions SET player_id = :keep_id "
                "WHERE player_id = :remove_id"
            ), {"keep_id": keep_id, "remove_id": remove_id})

        # 4. Bulk delete remove_id rows from aggregated stats tables
        for tbl in delete_tables:
            conn.execute(delete(tbl).where(tbl.c.player_id.in_(remove_ids)))

        # 5. Merge players table season ranges, then delete remove_ids
        for remove_id, keep_id in id_map.items():
            conn.execute(text("""
                UPDATE players SET
                    first_seen_season = LEAST(
                        first_seen_season,
                        (SELECT first_seen_season FROM players WHERE player_id = :remove_id)
                    ),
                    last_seen_season = GREATEST(
                        last_seen_season,
                        (SELECT last_seen_season FROM players WHERE player_id = :remove_id)
                    )
                WHERE player_id = :keep_id
            """), {"keep_id": keep_id, "remove_id": remove_id})
        conn.execute(text(
            "DELETE FROM players WHERE player_id = ANY(:ids)"
        ), {"ids": remove_ids})

    return len(merge_pairs)
