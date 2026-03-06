"""Parquet read/write for bulk play data analysis."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from isfl_epa.parser.schema import Game
from isfl_epa.players.registry import PlayerRegistry

DATA_DIR = Path("data/processed")


def _play_to_row(play, game: Game, play_index: int, registry: PlayerRegistry | None) -> dict:
    """Convert a ParsedPlay to a flat dict for DataFrame."""
    team_abbr = None
    if play.possession_team_id == game.home_team_id:
        team_abbr = game.home_team
    elif play.possession_team_id == game.away_team_id:
        team_abbr = game.away_team

    row = play.model_dump()
    row["season"] = game.season
    row["league"] = game.league
    row["play_index"] = play_index
    row["possession_team"] = team_abbr

    # Add player IDs if registry provided
    if registry:
        for field in ("passer", "rusher", "receiver", "tackler", "sacker",
                       "interceptor", "kicker", "returner"):
            name = getattr(play, field, None)
            row[f"player_id_{field}"] = registry.get_or_create(name, game.season, team_abbr) if name else None
    return row


def write_season_plays(
    games: list[Game],
    season: int,
    league: str,
    registry: PlayerRegistry | None = None,
) -> Path:
    """Write all plays for a season to a Parquet file."""
    rows = []
    for game in games:
        for idx, play in enumerate(game.plays):
            rows.append(_play_to_row(play, game, idx, registry))

    df = pd.DataFrame(rows)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{league}_S{season}_plays.parquet"
    df.to_parquet(path, index=False)
    return path


def read_season_plays(season: int, league: str) -> pd.DataFrame:
    """Read a season's plays from Parquet."""
    path = DATA_DIR / f"{league}_S{season}_plays.parquet"
    return pd.read_parquet(path)
