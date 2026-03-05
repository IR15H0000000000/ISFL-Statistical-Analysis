from enum import Enum

from pydantic import BaseModel


class PlayType(str, Enum):
    PASS = "pass"
    RUSH = "rush"
    SACK = "sack"
    PUNT = "punt"
    KICKOFF = "kickoff"
    FIELD_GOAL = "field_goal"
    KNEEL = "kneel"
    SPIKE = "spike"
    PENALTY = "penalty"
    TIMEOUT = "timeout"
    QUARTER_MARKER = "quarter_marker"
    UNKNOWN = "unknown"


class ParsedPlay(BaseModel):
    game_id: int
    quarter: int  # 1-4, 5 for OT
    clock: str
    # Score (None for S1-26 HTML games)
    score_away: int | None = None
    score_home: int | None = None
    away_team: str | None = None
    home_team: str | None = None
    possession_team_id: int | None = None
    # Situation
    down: int | None = None
    distance: int | None = None  # None for "inches"
    distance_text: str | None = None  # raw: "10", "inches", "Goal"
    yard_line: int | None = None
    yard_line_team: str | None = None
    # Play info
    play_type: PlayType
    description: str  # raw m field
    css: str = ""
    # Outcomes
    yards_gained: int | None = None
    first_down: bool = False
    touchdown: bool = False
    fumble: bool = False
    fumble_lost: bool = False
    interception: bool = False
    safety: bool = False
    turnover_on_downs: bool = False
    penalty: bool = False
    penalty_team: str | None = None
    penalty_type: str | None = None
    penalty_auto_first: bool = False
    # Players
    passer: str | None = None
    rusher: str | None = None
    receiver: str | None = None
    tackler: str | None = None
    sacker: str | None = None
    interceptor: str | None = None
    kicker: str | None = None
    returner: str | None = None
    fumbler: str | None = None
    fumble_recoverer: str | None = None
    # Kicking
    kick_yards: int | None = None
    fg_distance: int | None = None
    fg_good: bool | None = None
    pat_good: bool | None = None


class Game(BaseModel):
    id: int
    season: int
    league: str
    home_team: str | None = None
    away_team: str | None = None
    home_team_id: int | None = None
    away_team_id: int | None = None
    plays: list[ParsedPlay]
