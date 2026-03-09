"""EPA calculation using next-play lookahead.

For each play:
  EP_before = EP(current game state)
  EP_after  = EP_before of the next play (sign-flipped if possession changed)
  EPA       = EP_after - EP_before

Special cases: scoring plays get fixed EP_after values, last play of
half gets EP_after = 0.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

from isfl_epa.epa.dataset import load_training_plays
from isfl_epa.epa.features import (
    ERA_FEATURE_COLS,
    FEATURE_COLS,
    SCRIMMAGE_TYPES,
    half_number,
    prepare_features,
    valid_play_mask,
)
from isfl_epa.epa.model import EPModel, EPModelPair


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
    df = prepare_features(df, include_engine_era=not era_specific)
    df["half"] = df["quarter"].apply(half_number)

    # Identify plays with valid features for EP prediction
    vmask = valid_play_mask(df)
    # Also require yardline_100 (computed by prepare_features)
    vmask = vmask & df["yardline_100"].notna()
    valid_mask = vmask

    # Predict EP_before for all valid plays
    cols = ERA_FEATURE_COLS if era_specific else FEATURE_COLS
    df["ep_before"] = np.nan
    if valid_mask.any():
        X = df.loc[valid_mask, cols].copy()
        df.loc[valid_mask, "ep_before"] = ep_model.predict_ep(X)

    # Pre-extract columns as numpy arrays for fast access in the game loop
    ep_before_arr = df["ep_before"].values.copy()
    game_id_arr = df["game_id"].values
    half_arr = df["half"].values
    poss_tid_arr = df["possession_team_id"].values
    td_arr = df["touchdown"].values
    int_arr = df["interception"].values
    fl_arr = df["fumble_lost"].values
    pat_arr = df["pat_good"].values
    pt_arr = df["play_type"].values
    fg_arr = df["fg_good"].values
    safety_arr = df["safety"].values

    ep_after_arr = np.full(len(df), np.nan)
    epa_arr = np.full(len(df), np.nan)

    # Compute EP_after using next-play lookahead per game
    for game_id_val in pd.unique(game_id_arr):
        game_pos = np.where(game_id_arr == game_id_val)[0]

        for pi in range(len(game_pos)):
            pos = game_pos[pi]
            if np.isnan(ep_before_arr[pos]):
                continue

            ep_after_val = _compute_ep_after_arr(
                ep_before_arr, half_arr, poss_tid_arr,
                td_arr, int_arr, fl_arr, pat_arr,
                pt_arr, fg_arr, safety_arr,
                game_pos, pi, drive_model,
            )
            ep_after_arr[pos] = ep_after_val
            epa_arr[pos] = ep_after_val - ep_before_arr[pos]

    df["ep_after"] = ep_after_arr
    df["epa"] = epa_arr

    epa_count = np.count_nonzero(~np.isnan(epa_arr))
    logger.info(
        "compute_epa_for_df: %d total plays, %d valid, %d with EPA, mean EPA=%.4f",
        len(df),
        valid_mask.sum(),
        epa_count,
        np.nanmean(epa_arr) if epa_count > 0 else 0.0,
    )
    return df


def _compute_ep_after_arr(
    ep_before: np.ndarray,
    halves: np.ndarray,
    poss_tid: np.ndarray,
    touchdowns: np.ndarray,
    interceptions: np.ndarray,
    fumbles_lost: np.ndarray,
    pat_good: np.ndarray,
    play_types: np.ndarray,
    fg_good: np.ndarray,
    safeties: np.ndarray,
    game_pos: np.ndarray,
    pi: int,
    drive_model: bool = False,
) -> float:
    """Determine EP_after for a single play using numpy arrays."""
    pos = game_pos[pi]

    # Scoring plays on the current play get fixed EP_after values
    if _is_truthy(touchdowns[pos]):
        pat_bonus = 1 if _is_truthy(pat_good[pos]) else 0
        is_defensive_td = _is_truthy(interceptions[pos]) or _is_truthy(fumbles_lost[pos])
        if is_defensive_td:
            return -(6 + pat_bonus)
        return 6 + pat_bonus
    if play_types[pos] == "field_goal" and fg_good[pos] is True:
        return 3.0
    if _is_truthy(safeties[pos]):
        return -2.0

    current_half = halves[pos]
    poss_current = poss_tid[pos]

    if drive_model:
        return _ep_after_drive_arr(
            ep_before, halves, poss_tid,
            touchdowns, interceptions, fumbles_lost, pat_good,
            play_types, fg_good, safeties,
            game_pos, pi, current_half, poss_current,
        )

    return _ep_after_halfscore_arr(
        ep_before, halves, poss_tid,
        game_pos, pi, current_half, poss_current,
    )


def _is_truthy(val) -> bool:
    """Check if a value is truthy, handling None/NaN/numpy types."""
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass
    return bool(val)


def _ep_after_drive_arr(
    ep_before: np.ndarray,
    halves: np.ndarray,
    poss_tid: np.ndarray,
    touchdowns: np.ndarray,
    interceptions: np.ndarray,
    fumbles_lost: np.ndarray,
    pat_good: np.ndarray,
    play_types: np.ndarray,
    fg_good: np.ndarray,
    safeties: np.ndarray,
    game_pos: np.ndarray,
    pi: int,
    current_half,
    poss_current,
) -> float:
    """EP_after for drive-outcome model using numpy arrays."""
    for i in range(pi + 1, len(game_pos)):
        npos = game_pos[i]

        # Half changed → drive ended
        if halves[npos] != current_half:
            return 0.0

        poss_next = poss_tid[npos]

        # Possession changed → drive ended without scoring
        if poss_next is not None and poss_current is not None:
            try:
                if not pd.isna(poss_next) and not pd.isna(poss_current) and poss_next != poss_current:
                    return 0.0
            except (TypeError, ValueError):
                if poss_next != poss_current:
                    return 0.0

        # Same drive — if this play has ep_before, use it
        next_ep = ep_before[npos]
        if not np.isnan(next_ep):
            return next_ep

        # No ep_before — check if it's a scoring play
        if _is_truthy(touchdowns[npos]):
            pat_bonus = 1 if _is_truthy(pat_good[npos]) else 0
            is_def_td = _is_truthy(interceptions[npos]) or _is_truthy(fumbles_lost[npos])
            if is_def_td:
                return -(6 + pat_bonus)
            return 6 + pat_bonus

        if play_types[npos] == "field_goal" and fg_good[npos] is True:
            return 3.0

        if _is_truthy(safeties[npos]):
            return -2.0

    # End of game
    return 0.0


def _ep_after_halfscore_arr(
    ep_before: np.ndarray,
    halves: np.ndarray,
    poss_tid: np.ndarray,
    game_pos: np.ndarray,
    pi: int,
    current_half,
    poss_current,
) -> float:
    """EP_after for next-score-in-half model using numpy arrays."""
    # Find next play with valid ep_before
    for i in range(pi + 1, len(game_pos)):
        npos = game_pos[i]
        if not np.isnan(ep_before[npos]):
            # Found next valid play
            if halves[npos] != current_half:
                return 0.0

            next_ep = ep_before[npos]
            poss_next = poss_tid[npos]

            if poss_current is not None and poss_next is not None:
                try:
                    if not pd.isna(poss_current) and not pd.isna(poss_next) and poss_current != poss_next:
                        return -next_ep
                except (TypeError, ValueError):
                    if poss_current != poss_next:
                        return -next_ep

            return next_ep

    return 0.0
