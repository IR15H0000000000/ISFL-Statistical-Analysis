"""Pydantic models for EPA results.

Each model represents an aggregated EPA stat line. Per-play EPA is stored
directly in the play_epa database table / Parquet column.
"""

from pydantic import BaseModel


class PlayEPA(BaseModel):
    play_id: int
    game_id: int
    season: int
    ep_before: float
    ep_after: float
    epa: float
    play_type: str
    player_id_passer: int | None = None
    player_id_rusher: int | None = None
    player_id_receiver: int | None = None
    possession_team: str | None = None


class PlayerEPASeason(BaseModel):
    player_id: int
    player: str
    team: str | None = None
    season: int
    # Passing
    pass_epa: float = 0.0
    dropbacks: int = 0
    epa_per_dropback: float = 0.0
    # Rushing
    rush_epa: float = 0.0
    rush_attempts: int = 0
    epa_per_rush: float = 0.0
    # Receiving
    recv_epa: float = 0.0
    targets: int = 0
    epa_per_target: float = 0.0
    # Defensive
    def_epa: float = 0.0
    def_plays: int = 0
    epa_per_def_play: float = 0.0


class TeamEPASeason(BaseModel):
    team: str
    season: int
    total_epa: float = 0.0
    pass_epa: float = 0.0
    rush_epa: float = 0.0
    plays: int = 0
    epa_per_play: float = 0.0
