"""EPA calculation using next-play lookahead.

For each play:
  EP_before = EP(current game state)
  EP_after  = EP_before of the next play (sign-flipped if possession changed)
  EPA       = EP_after - EP_before

Special cases: scoring plays get fixed EP_after values, last play of
half gets EP_after = 0.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from isfl_epa.config import ENGINE_CUTOFF_SEASON
from isfl_epa.epa.dataset import (
    clock_to_seconds,
    compute_yardline_100,
    load_training_plays,
)
from isfl_epa.epa.model import EPModel, EPModelPair

FEATURE_COLS = [
    "down", "distance", "yardline_100", "score_differential",
    "half_seconds_remaining", "is_home", "is_overtime", "engine_era",
]

ERA_FEATURE_COLS = [
    "down", "distance", "yardline_100", "score_differential",
    "half_seconds_remaining", "is_home", "is_overtime",
]


def _prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add computed feature columns to a plays DataFrame."""
    out = df.copy()
    out["yardline_100"] = compute_yardline_100(out)
    out["half_seconds_remaining"] = out.apply(
        lambda r: clock_to_seconds(r["clock"], r["quarter"]), axis=1
    )

    def _score_diff(row):
        poss_team = row.get("possession_team")
        home = row.get("home_team")
        if poss_team == home:
            return (row.get("score_home") or 0) - (row.get("score_away") or 0)
        else:
            return (row.get("score_away") or 0) - (row.get("score_home") or 0)

    out["score_differential"] = out.apply(_score_diff, axis=1).clip(-28, 28)
    out["is_home"] = (out["possession_team"] == out["home_team"]).astype(int)
    out["is_overtime"] = (out["quarter"] >= 5).astype(int)
    out["engine_era"] = (out["season"] >= ENGINE_CUTOFF_SEASON).astype(int)
    out["distance"] = out["distance"].clip(upper=30)
    return out


def _half_number(quarter: int) -> int:
    if quarter <= 2:
        return 1
    if quarter <= 4:
        return 2
    return 3


def compute_epa_for_season(
    season: int,
    league: str,
    ep_model: EPModel | EPModelPair,
) -> pd.DataFrame:
    """Compute EPA for all plays in a season.

    Returns the original DataFrame with added columns:
    ep_before, ep_after, epa.
    """
    df = load_training_plays([season], league)
    if isinstance(ep_model, EPModelPair):
        model = ep_model.get_model(season)
        is_regression = model.model_type == "hgb_reg"
        return compute_epa_for_df(df, model, era_specific=True, drive_model=is_regression)
    return compute_epa_for_df(df, ep_model)


def compute_epa_for_df(
    df: pd.DataFrame, ep_model: EPModel, era_specific: bool = False,
    drive_model: bool = False,
) -> pd.DataFrame:
    """Compute EPA for a plays DataFrame.

    Args:
        drive_model: If True, use drive-outcome EP_after logic (0 on possession
            change instead of -next_ep).
    """
    df = _prepare_features(df)
    df["half"] = df["quarter"].apply(_half_number)

    # Identify plays with valid features for EP prediction
    # Exclude non-scrimmage play types that shouldn't have EPA
    _epa_play_types = {"pass", "rush", "sack", "field_goal"}
    valid_mask = (
        df["play_type"].isin(_epa_play_types)
        & df["down"].notna()
        & df["distance"].notna()
        & df["yardline_100"].notna()
        & df["score_away"].notna()
        & ~df["description"].str.contains("2 point|conversion", case=False, na=False)
    )

    # Predict EP_before for all valid plays
    cols = ERA_FEATURE_COLS if era_specific else FEATURE_COLS
    df["ep_before"] = np.nan
    if valid_mask.any():
        X = df.loc[valid_mask, cols].copy()
        df.loc[valid_mask, "ep_before"] = ep_model.predict_ep(X)

    # Compute EP_after using next-play lookahead per game
    df["ep_after"] = np.nan
    df["epa"] = np.nan

    for game_id in df["game_id"].unique():
        game_mask = df["game_id"] == game_id
        game_indices = df.loc[game_mask].index.tolist()

        for i, idx in enumerate(game_indices):
            row = df.loc[idx]

            if pd.isna(row["ep_before"]):
                continue

            ep_after = _compute_ep_after(df, game_indices, i, idx, row, drive_model)
            df.at[idx, "ep_after"] = ep_after
            df.at[idx, "epa"] = ep_after - row["ep_before"]

    return df


def _compute_ep_after(
    df: pd.DataFrame,
    game_indices: list,
    pos: int,
    idx: int,
    row: pd.Series,
    drive_model: bool = False,
) -> float:
    """Determine EP_after for a single play."""
    # Scoring plays on the current play get fixed EP_after values
    if row.get("touchdown"):
        pat_bonus = 1 if row.get("pat_good") else 0
        is_defensive_td = row.get("interception") or row.get("fumble_lost")
        if is_defensive_td:
            return -(6 + pat_bonus)
        return 6 + pat_bonus
    if row.get("play_type") == "field_goal" and row.get("fg_good") is True:
        return 3.0
    if row.get("safety"):
        return -2.0

    current_half = row.get("half")
    poss_current = row.get("possession_team_id")

    if drive_model:
        return _compute_ep_after_drive(df, game_indices, pos, current_half, poss_current)

    return _compute_ep_after_halfscore(df, game_indices, pos, current_half, poss_current)


def _compute_ep_after_drive(
    df: pd.DataFrame,
    game_indices: list,
    pos: int,
    current_half: int,
    poss_current,
) -> float:
    """EP_after for drive-outcome model.

    Scans forward through ALL subsequent plays (not just those with ep_before)
    to find the next play's ep_before, or drive boundaries/scoring events on
    plays that lack ep_before.
    """
    for i in range(pos + 1, len(game_indices)):
        next_idx = game_indices[i]
        nr = df.loc[next_idx]
        next_half = nr.get("half")

        # Half changed → drive ended
        if next_half != current_half:
            return 0.0

        poss_next = nr.get("possession_team_id")

        # Possession changed → drive ended without scoring
        if poss_next is not None and poss_current is not None and poss_next != poss_current:
            return 0.0

        # Same drive — if this play has ep_before, use it
        next_ep = nr.get("ep_before")
        if not pd.isna(next_ep):
            return next_ep

        # No ep_before — check if it's a scoring play (FG, safety)
        # that we'd otherwise miss
        if nr.get("touchdown"):
            pat_bonus = 1 if nr.get("pat_good") else 0
            is_def_td = nr.get("interception") or nr.get("fumble_lost")
            if is_def_td:
                return -(6 + pat_bonus)
            return 6 + pat_bonus

        if nr.get("play_type") == "field_goal" and nr.get("fg_good") is True:
            return 3.0

        if nr.get("safety"):
            return -2.0

        # Non-scoring play without ep_before — keep scanning

    # End of game
    return 0.0


def _compute_ep_after_halfscore(
    df: pd.DataFrame,
    game_indices: list,
    pos: int,
    current_half: int,
    poss_current,
) -> float:
    """EP_after for next-score-in-half model (original logic)."""
    next_idx = _find_next_valid_play(df, game_indices, pos)

    if next_idx is None:
        return 0.0

    next_row = df.loc[next_idx]
    if next_row.get("half") != current_half:
        return 0.0

    next_ep = next_row.get("ep_before")
    if pd.isna(next_ep):
        return 0.0

    poss_next = next_row.get("possession_team_id")
    if poss_current is not None and poss_next is not None and poss_current != poss_next:
        return -next_ep

    return next_ep


def _find_next_valid_play(
    df: pd.DataFrame, game_indices: list, current_pos: int
) -> int | None:
    """Find the next play index that has a valid ep_before."""
    for i in range(current_pos + 1, len(game_indices)):
        idx = game_indices[i]
        if not pd.isna(df.at[idx, "ep_before"]):
            return idx
    return None
