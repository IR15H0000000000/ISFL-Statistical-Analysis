"""Shared feature engineering for EPA model training and prediction.

Centralizes feature computation that was previously duplicated across
dataset.py (build_feature_matrix, build_drive_feature_matrix) and
calculator.py (_prepare_features).
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from isfl_epa.config import (
    DISTANCE_CLIP,
    ENGINE_CUTOFF_SEASON,
    SCORE_CLIP,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FEATURE_COLS = [
    "down", "distance", "yardline_100", "score_differential",
    "half_seconds_remaining", "is_home", "is_overtime", "engine_era",
]

ERA_FEATURE_COLS = [
    "down", "distance", "yardline_100", "score_differential",
    "half_seconds_remaining", "is_home", "is_overtime",
]

SCRIMMAGE_TYPES = {"pass", "rush", "sack", "field_goal"}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def clock_to_seconds(clock: str, quarter: int) -> int:
    """Convert clock string and quarter to half_seconds_remaining.

    Q1: clock + 900 (one full quarter left in first half)
    Q2: clock
    Q3: clock + 900 (one full quarter left in second half)
    Q4: clock
    Q5 (OT): 0
    """
    if quarter >= 5:
        return 0
    try:
        parts = str(clock).split(":")
        minutes = int(parts[0])
        seconds = int(parts[1]) if len(parts) > 1 else 0
        clock_secs = minutes * 60 + seconds
    except (ValueError, IndexError):
        return 0

    if quarter in (1, 3):
        return clock_secs + 900
    return clock_secs


def half_number(quarter: int) -> int:
    """Map quarter to half: 1/2 -> 1, 3/4 -> 2, OT -> 3."""
    if quarter <= 2:
        return 1
    if quarter <= 4:
        return 2
    return 3


def compute_yardline_100(df: pd.DataFrame) -> pd.Series:
    """Convert yard_line to yards from possession team's own end zone (0-100).

    If yard_line_team matches the possession team's abbreviation, the
    yardline_100 is the raw yard_line. Otherwise it's 100 - yard_line.
    """
    yl = df["yard_line"]
    same_side = df["yard_line_team"] == df["possession_team"]
    result = pd.Series(np.where(same_side, yl, 100 - yl), index=df.index)
    # Null out where inputs are missing
    missing = df["yard_line"].isna() | df["yard_line_team"].isna() | df["possession_team"].isna()
    result[missing] = np.nan
    return result


# ---------------------------------------------------------------------------
# Scrimmage play mask
# ---------------------------------------------------------------------------


def valid_play_mask(df: pd.DataFrame, label_col: str | None = None) -> pd.Series:
    """Return a boolean mask for valid scrimmage plays.

    Filters to plays that:
    - Are scrimmage types (pass, rush, sack, field_goal)
    - Have valid down, distance, yard_line, and score columns
    - Are not 2-point conversions
    - Have a non-null label if *label_col* is specified
    """
    mask = (
        df["play_type"].isin(SCRIMMAGE_TYPES)
        & df["down"].notna()
        & df["distance"].notna()
        & df["yard_line"].notna()
        & df["score_away"].notna()
        & df["score_home"].notna()
        & ~df["description"].str.contains("2 point|conversion", case=False, na=False)
    )
    if label_col is not None:
        mask = mask & df[label_col].notna()
    return mask


# ---------------------------------------------------------------------------
# Vectorized feature preparation
# ---------------------------------------------------------------------------


def prepare_features(
    df: pd.DataFrame,
    *,
    include_engine_era: bool = True,
) -> pd.DataFrame:
    """Add computed feature columns to a plays DataFrame (vectorized).

    Adds: yardline_100, score_differential, half_seconds_remaining,
    is_home, is_overtime, engine_era (optional), and clips distance.

    Does NOT filter rows — callers decide what to keep.
    """
    out = df.copy()

    # Yardline
    out["yardline_100"] = compute_yardline_100(out)

    # Half seconds remaining (vectorized)
    clock_parts = out["clock"].astype(str).str.split(":", expand=True)
    minutes = pd.to_numeric(clock_parts[0], errors="coerce").fillna(0).astype(int)
    seconds = (
        pd.to_numeric(clock_parts[1], errors="coerce").fillna(0).astype(int)
        if 1 in clock_parts.columns
        else 0
    )
    clock_secs = minutes * 60 + seconds
    quarter = out["quarter"]
    half_secs = np.where(quarter.isin([1, 3]), clock_secs + 900, clock_secs)
    half_secs = np.where(quarter >= 5, 0, half_secs)
    out["half_seconds_remaining"] = half_secs

    # Score differential (vectorized)
    # Use possession_team vs home_team when available, fall back to
    # possession_team_id vs game home_tid for parquet data.
    if "possession_team" in out.columns and "home_team" in out.columns:
        has_team_names = out["possession_team"].notna() & out["home_team"].notna()
    else:
        has_team_names = pd.Series(False, index=out.index)

    if has_team_names.any():
        is_home_flag = (out["possession_team"] == out["home_team"]).astype(int)
    else:
        is_home_flag = pd.Series(0, index=out.index)

    # For rows without team names, fall back to possession_team_id approach
    if not has_team_names.all():
        # Vectorized: home team = max team_id per game (when 2+ teams present)
        pairs = out[["game_id", "possession_team_id"]].dropna(subset=["possession_team_id"])
        game_max = pairs.groupby("game_id")["possession_team_id"].max()
        game_min = pairs.groupby("game_id")["possession_team_id"].min()
        # NaN if only one team seen in a game
        game_home_tid = game_max.where(game_max != game_min)
        home_tid_col = out["game_id"].map(game_home_tid)
        tid_is_home = (out["possession_team_id"] == home_tid_col).astype(int)
        is_home_flag = is_home_flag.where(has_team_names, tid_is_home)

    sh = out["score_home"].fillna(0)
    sa = out["score_away"].fillna(0)
    score_diff = np.where(is_home_flag, sh - sa, sa - sh)
    out["score_differential"] = pd.Series(score_diff, index=out.index).clip(-SCORE_CLIP, SCORE_CLIP)
    out["is_home"] = is_home_flag.values

    out["is_overtime"] = (quarter >= 5).astype(int)

    if include_engine_era:
        out["engine_era"] = (out["season"] >= ENGINE_CUTOFF_SEASON).astype(int)

    out["distance"] = out["distance"].clip(upper=DISTANCE_CLIP)

    return out
