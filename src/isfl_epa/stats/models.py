"""Pydantic models for player and team stat lines.

Each model represents a per-game stat line. Season totals are derived
via DataFrame groupby operations in the aggregation module.
"""

from pydantic import BaseModel


class PlayerPassing(BaseModel):
    player_id: int | None = None
    player: str
    team: str | None = None
    game_id: int
    season: int
    comp: int = 0
    att: int = 0
    yards: int = 0
    td: int = 0
    interceptions: int = 0
    sacks: int = 0
    sack_yards: int = 0


class PlayerRushing(BaseModel):
    player_id: int | None = None
    player: str
    team: str | None = None
    game_id: int
    season: int
    att: int = 0
    yards: int = 0
    td: int = 0
    fumbles: int = 0


class PlayerReceiving(BaseModel):
    player_id: int | None = None
    player: str
    team: str | None = None
    game_id: int
    season: int
    receptions: int = 0
    yards: int = 0
    td: int = 0
    fumbles: int = 0


class PlayerDefensive(BaseModel):
    player_id: int | None = None
    player: str
    team: str | None = None
    game_id: int
    season: int
    tackles: int = 0
    sacks: float = 0
    interceptions: int = 0
    fumble_recoveries: int = 0
    forced_fumbles: int = 0


class TeamGame(BaseModel):
    game_id: int
    season: int
    team: str
    opponent: str
    is_home: bool
    # Scoring
    points_for: int = 0
    points_against: int = 0
    # Passing
    pass_comp: int = 0
    pass_att: int = 0
    pass_yards: int = 0
    pass_td: int = 0
    # Rushing
    rush_att: int = 0
    rush_yards: int = 0
    rush_td: int = 0
    # Totals
    total_yards: int = 0
    first_downs: int = 0
    # Turnovers
    interceptions_thrown: int = 0
    fumbles_lost: int = 0
    forced_fumbles: int = 0
    fumble_recoveries: int = 0
    turnovers: int = 0
    # Situational
    third_down_att: int = 0
    third_down_conv: int = 0
    # Sacks
    sacks_taken: int = 0
    sacks_made: int = 0
