"""Data preparation for EP model training.

Loads parsed plays, labels each with the next scoring event in its half,
and builds a feature matrix for multinomial classification.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from isfl_epa.epa.features import (
    ERA_FEATURE_COLS,
    FEATURE_COLS,
    SCRIMMAGE_TYPES,
    clock_to_seconds,
    compute_yardline_100,
    half_number,
    prepare_features,
    valid_play_mask,
)

logger = logging.getLogger(__name__)

# Next-score labels and their point values (from possession team's perspective)
LABEL_POINT_VALUES = {
    "td_pos": 7,
    "fg_pos": 3,
    "safety_pos": 2,
    "td_neg": -7,
    "fg_neg": -3,
    "safety_neg": -2,
    "no_score": 0,
}

# Keep aliases for backward compatibility (used in calculator.py)
_SCRIMMAGE_TYPES = SCRIMMAGE_TYPES

# Play types that are never training rows
_EXCLUDE_TYPES = {
    "kickoff", "punt", "penalty", "timeout", "quarter_marker",
    "spike", "kneel", "unknown",
}


# ---------------------------------------------------------------------------
# Score reconstruction (DataFrame-level, for S1-26 parquet data)
# ---------------------------------------------------------------------------


def _reconstruct_scores_df(df: pd.DataFrame) -> pd.DataFrame:
    """Reconstruct score_away and score_home from scoring event flags.

    For parquet data where scores are all None but touchdown/fg_good/safety/
    pat_good flags are present. Uses convention: lower possession_team_id = away,
    higher = home.

    Scores are set to the state BEFORE each play (same as live game data).
    """
    df = df.copy()

    # Pre-compute home team ID per game (higher of the two IDs)
    game_home_tid = (
        df.groupby("game_id")["possession_team_id"]
        .apply(lambda s: max(s.dropna().unique()) if len(s.dropna().unique()) >= 2 else None)
    )

    for game_id, group in df.groupby("game_id"):
        # Skip if scores already present
        if group["score_away"].notna().any():
            continue

        home_tid = game_home_tid.get(game_id)
        if home_tid is None:
            continue

        indices = group.index
        poss_ids = group["possession_team_id"].values
        is_td = group["touchdown"].fillna(False).values.astype(bool)
        is_pat = group["pat_good"].fillna(False).values.astype(bool)
        is_fg = group["fg_good"].fillna(False).values.astype(bool)
        is_safety = group["safety"].fillna(False).values.astype(bool)
        is_home_poss = poss_ids == home_tid

        # Compute per-play score increments
        home_delta = np.zeros(len(group), dtype=int)
        away_delta = np.zeros(len(group), dtype=int)

        # TDs: 6 + 1 if pat_good
        td_pts = np.where(is_td, 6 + is_pat.astype(int), 0)
        home_delta += np.where(is_home_poss & is_td, td_pts, 0)
        away_delta += np.where(~is_home_poss & is_td, td_pts, 0)

        # FGs: 3 points
        home_delta += np.where(is_home_poss & is_fg & ~is_td, 3, 0)
        away_delta += np.where(~is_home_poss & is_fg & ~is_td, 3, 0)

        # Safeties: 2 to defensive team (opponent of possession)
        home_delta += np.where(~is_home_poss & is_safety, 2, 0)
        away_delta += np.where(is_home_poss & is_safety, 2, 0)

        # Cumsum gives score AFTER each play; shift right for "before" each play
        home_cumsum = np.cumsum(home_delta)
        away_cumsum = np.cumsum(away_delta)
        home_before = np.concatenate([[0], home_cumsum[:-1]])
        away_before = np.concatenate([[0], away_cumsum[:-1]])

        df.loc[indices, "score_home"] = home_before
        df.loc[indices, "score_away"] = away_before

    return df


# ---------------------------------------------------------------------------
# Team mapping inference (for parquet data missing team abbreviations)
# ---------------------------------------------------------------------------


def _infer_team_mapping(game_df: pd.DataFrame) -> dict:
    """Infer possession_team_id -> team abbreviation mapping for a game.

    Uses plays right after kickoffs: the receiving team is in their own
    territory, so yard_line_team matches their abbreviation.
    """
    team_ids = sorted(game_df["possession_team_id"].dropna().unique())
    if len(team_ids) < 2:
        logger.warning("_infer_team_mapping: game has < 2 team IDs: %s", team_ids)
        return {}

    abbrevs = sorted(game_df["yard_line_team"].dropna().unique())
    if len(abbrevs) < 2:
        logger.warning("_infer_team_mapping: game has < 2 yard_line_team abbrevs: %s", abbrevs)
        return {}

    # Count yard_line_team occurrences for each poss_id on post-kickoff plays
    votes: dict[float, dict[str, int]] = {}
    pt = game_df["play_type"].values
    poss = game_df["possession_team_id"].values
    ylt = game_df["yard_line_team"].values
    yl = game_df["yard_line"].values

    for i in range(len(pt) - 1):
        if pt[i] == "kickoff" and pt[i + 1] in ("pass", "rush", "sack"):
            p, t, y = poss[i + 1], ylt[i + 1], yl[i + 1]
            if p is not None and t is not None and y is not None:
                try:
                    if float(y) <= 50:
                        votes.setdefault(p, {})
                        votes[p][t] = votes[p].get(t, 0) + 1
                except (ValueError, TypeError):
                    pass

    mapping = {}
    for poss_id, abbrev_counts in votes.items():
        if abbrev_counts:
            mapping[poss_id] = max(abbrev_counts, key=abbrev_counts.get)

    return mapping


def _fill_team_info(df: pd.DataFrame) -> pd.DataFrame:
    """Fill possession_team, home_team, away_team from inference.

    Convention: lower possession_team_id = away, higher = home.
    """
    df = df.copy()

    for game_id, game_plays in df.groupby("game_id"):
        # Skip if already populated
        if game_plays["possession_team"].notna().any():
            continue

        team_ids = sorted(game_plays["possession_team_id"].dropna().unique())
        if len(team_ids) < 2:
            logger.debug("_fill_team_info: skipping game %s — only %d team IDs", game_id, len(team_ids))
            continue
        away_tid, home_tid = team_ids[0], team_ids[1]

        mapping = _infer_team_mapping(game_plays)
        if not mapping:
            logger.debug("_fill_team_info: skipping game %s — team mapping empty", game_id)
            continue

        away_abbr = mapping.get(away_tid)
        home_abbr = mapping.get(home_tid)
        if not away_abbr or not home_abbr:
            logger.debug("_fill_team_info: skipping game %s — incomplete mapping: %s", game_id, mapping)
            continue

        game_mask = df["game_id"] == game_id
        df.loc[game_mask, "home_team"] = home_abbr
        df.loc[game_mask, "away_team"] = away_abbr

        mask_home = game_mask & (df["possession_team_id"] == home_tid)
        mask_away = game_mask & (df["possession_team_id"] == away_tid)
        df.loc[mask_home, "possession_team"] = home_abbr
        df.loc[mask_away, "possession_team"] = away_abbr

    return df


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _derive_possession_team_from_home_away(df: pd.DataFrame) -> pd.Series:
    """Derive possession_team by intersecting team candidates across games.

    Team IDs are FIXED per team across all games in a season.  For each ptid,
    the correct team is the one that appears in EVERY game where that ptid
    is used — i.e., the intersection of {home_team, away_team} sets.
    """
    game_teams = df.groupby("game_id").agg(
        home_team=("home_team", "first"),
        away_team=("away_team", "first"),
    )

    ptid_candidates: dict[int, set[str]] = {}
    for game_id, info in game_teams.iterrows():
        home, away = info["home_team"], info["away_team"]
        if not home or not away:
            continue
        teams = {home, away}
        game_ptids = df.loc[
            (df["game_id"] == game_id) & df["possession_team_id"].notna(),
            "possession_team_id",
        ].unique()
        for ptid in game_ptids:
            p = int(ptid)
            if p not in ptid_candidates:
                ptid_candidates[p] = teams.copy()
            else:
                ptid_candidates[p] &= teams

    ptid_to_team = {}
    for ptid, teams in ptid_candidates.items():
        if len(teams) == 1:
            ptid_to_team[ptid] = next(iter(teams))
        else:
            logger.warning(
                "_derive_possession_team: ptid %d has %d candidates %s — skipping",
                ptid, len(teams), teams,
            )

    return df["possession_team_id"].map(ptid_to_team)


def _filter_preseason_from_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Remove preseason game_ids from a parquet-loaded DataFrame using the games table."""
    try:
        from sqlalchemy import select
        from isfl_epa.storage.database import get_engine, games_table

        engine = get_engine()
        game_ids = df["game_id"].unique().tolist()
        with engine.connect() as conn:
            rows = conn.execute(
                select(games_table.c.game_id)
                .where(games_table.c.game_id.in_(game_ids))
                .where(games_table.c.game_type == "preseason")
            ).fetchall()
        preseason_ids = {r.game_id for r in rows}
        if preseason_ids:
            logger.info("Filtering %d preseason games from parquet data", len(preseason_ids))
            df = df[~df["game_id"].isin(preseason_ids)]
    except Exception:
        logger.warning("Could not filter preseason from parquet (games table may not exist yet)")
    return df


def load_training_plays(
    seasons, league: str = "ISFL", database_url: str | None = None,
) -> pd.DataFrame:
    """Load and concatenate plays from PostgreSQL.

    All seasons are loaded from the database. For S1-26, scores are
    reconstructed from scoring flags and teams inferred from kickoff patterns.
    For S27+, possession_team is derived from home_team/away_team.
    """
    from sqlalchemy import select

    from isfl_epa.storage.database import get_engine, plays_table

    engine = get_engine(database_url)
    with engine.connect() as conn:
        stmt = (
            select(plays_table)
            .where(
                plays_table.c.season.in_(list(seasons))
                & (plays_table.c.league == league)
                & (plays_table.c.game_type != "preseason")
            )
            .order_by(
                plays_table.c.game_id,
                plays_table.c.play_index,
            )
        )
        db_df = pd.read_sql(stmt, conn)

    if db_df.empty:
        return pd.DataFrame()

    if "possession_team" not in db_df.columns:
        db_df["possession_team"] = None
    # For rows with home_team but no possession_team, derive it
    needs_poss = db_df["possession_team"].isna() & db_df["home_team"].notna()
    if needs_poss.any():
        derived = _derive_possession_team_from_home_away(db_df.loc[needs_poss])
        db_df.loc[needs_poss, "possession_team"] = derived
    # For rows missing both (S1-26 in DB), use kickoff inference
    still_missing = db_df["possession_team"].isna()
    if still_missing.any():
        if db_df.loc[still_missing, "score_away"].isna().any():
            db_df = _reconstruct_scores_df(db_df)
        db_df = _fill_team_info(db_df)

    return db_df


# ---------------------------------------------------------------------------
# Clock / half utilities — imported from features.py, re-exported here
# for backward compatibility. See isfl_epa.epa.features for implementations.
# ---------------------------------------------------------------------------

# clock_to_seconds, compute_yardline_100, half_number are imported above

# Keep _half_number alias for internal use
_half_number = half_number


# ---------------------------------------------------------------------------
# Next-score labeling (vectorized per game-half)
# ---------------------------------------------------------------------------


def label_next_score(df: pd.DataFrame) -> pd.DataFrame:
    """Label each play with the next scoring event in its half.

    Walks backwards within each half of a game. When a scoring play is
    found, all plays between it and the previous score get labeled
    relative to their possession team.

    Adds column: 'next_score_label' (one of the LABEL_POINT_VALUES keys).
    """
    # Validate sort order — critical for correct labeling
    if not df.empty and "play_index" in df.columns:
        diffs = df.groupby("game_id")["play_index"].diff().dropna()
        if (diffs < 0).any():
            logger.warning("label_next_score: DataFrame not sorted by game_id + play_index — sorting now")
            df = df.sort_values(["game_id", "play_index"]).reset_index(drop=True)
        else:
            df = df.copy()
    else:
        df = df.copy()
    quarter_vals = df["quarter"].values
    half_vals = np.where(quarter_vals <= 2, 1, np.where(quarter_vals <= 4, 2, 3))
    df["half"] = half_vals

    # Pre-allocate label array (positional, not index-based)
    all_labels = np.full(len(df), "no_score", dtype=object)

    # Pre-compute opponent lookup per game
    game_opponent: dict = {}
    for game_id, group in df.groupby("game_id"):
        tids = group["possession_team_id"].dropna().unique()
        if len(tids) >= 2:
            game_opponent[game_id] = {tids[0]: tids[1], tids[1]: tids[0]}

    # Pre-compute boolean arrays (positional)
    is_td = df["touchdown"].fillna(False).values.astype(bool)
    is_fg = ((df["play_type"] == "field_goal") & df["fg_good"].fillna(False).astype(bool)).values
    is_safety = df["safety"].fillna(False).values.astype(bool)
    poss_arr = df["possession_team_id"].values
    game_arr = df["game_id"].values
    half_arr = half_vals

    # Build group boundaries using sorted order (df should already be sorted by game+play_index)
    # Create a composite group key and find boundaries
    group_key = game_arr.astype(str) + "_" + half_arr.astype(str)
    changes = np.concatenate([[True], group_key[1:] != group_key[:-1]])
    group_starts = np.where(changes)[0]
    group_ends = np.concatenate([group_starts[1:], [len(df)]])

    for start, end in zip(group_starts, group_ends):
        game_id = game_arr[start]
        opponents = game_opponent.get(game_id, {})

        current_label = "no_score"
        current_label_team = None

        for i in range(end - 1, start - 1, -1):
            pid = poss_arr[i]

            if is_td[i]:
                current_label = "td"
                current_label_team = pid
            elif is_fg[i]:
                current_label = "fg"
                current_label_team = pid
            elif is_safety[i]:
                current_label = "safety"
                current_label_team = opponents.get(pid)

            if current_label == "no_score":
                all_labels[i] = "no_score"
            elif pid is not None and pid == current_label_team:
                all_labels[i] = f"{current_label}_pos"
            else:
                all_labels[i] = f"{current_label}_neg"

    df["next_score_label"] = all_labels
    return df


def _opponent_id(row, game_df: pd.DataFrame) -> int | None:
    """Get the opponent's possession_team_id for a given play."""
    team_ids = game_df["possession_team_id"].dropna().unique()
    poss = row.get("possession_team_id")
    for tid in team_ids:
        if tid != poss:
            return tid
    return None


# ---------------------------------------------------------------------------
# Drive-outcome labeling
# ---------------------------------------------------------------------------


def label_drive_outcome(df: pd.DataFrame) -> pd.DataFrame:
    """Label each play with the actual points scored on its current drive.

    Drive boundaries are detected by:
    - Change of possession_team_id within a game
    - Half changes (quarter 1-2 vs 3-4 vs 5+)
    - Scoring plays end the current drive

    Point values (from the drive's possession team perspective):
    - TD: +(6 + PAT bonus)  where bonus = 1 if pat_good, else 0
    - Defensive TD (INT/fumble + TD): -(6 + PAT bonus)
    - FG good: +3
    - Safety: -2
    - All other endings: 0

    Adds column: 'drive_points' (float).
    """
    # Validate sort order — critical for correct drive segmentation
    if not df.empty and "play_index" in df.columns:
        diffs = df.groupby("game_id")["play_index"].diff().dropna()
        if (diffs < 0).any():
            logger.warning("label_drive_outcome: DataFrame not sorted by game_id + play_index — sorting now")
            df = df.sort_values(["game_id", "play_index"]).reset_index(drop=True)
        else:
            df = df.copy()
    else:
        df = df.copy()
    quarter_vals = df["quarter"].values
    half_vals = np.where(quarter_vals <= 2, 1, np.where(quarter_vals <= 4, 2, 3))

    game_arr = df["game_id"].values
    poss_arr = df["possession_team_id"].values
    is_td = df["touchdown"].fillna(False).values.astype(bool)
    is_int = df["interception"].fillna(False).values.astype(bool)
    is_fumble = df["fumble_lost"].fillna(False).values.astype(bool)
    is_fg = ((df["play_type"] == "field_goal") & df["fg_good"].fillna(False).astype(bool)).values
    is_safety = df["safety"].fillna(False).values.astype(bool)
    pat_good = df["pat_good"].fillna(False).values.astype(bool)

    drive_points = np.zeros(len(df), dtype=float)

    # Process each game
    game_key = game_arr.astype(str)
    changes = np.concatenate([[True], game_key[1:] != game_key[:-1]])
    game_starts = np.where(changes)[0]
    game_ends = np.concatenate([game_starts[1:], [len(df)]])

    for g_start, g_end in zip(game_starts, game_ends):
        # Walk through plays in this game, tracking drive boundaries
        drive_start = g_start
        drive_poss = poss_arr[g_start]
        drive_half = half_vals[g_start]

        for i in range(g_start, g_end):
            curr_poss = poss_arr[i]
            curr_half = half_vals[i]

            # Check if this play starts a new drive
            new_drive = False
            if curr_half != drive_half:
                # Half changed — end previous drive with 0, start new
                drive_points[drive_start:i] = 0.0
                new_drive = True
            elif curr_poss != drive_poss and not pd.isna(curr_poss):
                # Possession changed — end previous drive with 0
                # (the turnover play itself was part of the old drive)
                drive_points[drive_start:i] = 0.0
                new_drive = True

            if new_drive:
                drive_start = i
                drive_poss = curr_poss
                drive_half = curr_half

            # Check if this play is a scoring play that ends the drive
            if is_td[i]:
                pat_bonus = 1 if pat_good[i] else 0
                if is_int[i] or is_fumble[i]:
                    # Defensive TD — opponent scores on this drive
                    pts = -(6 + pat_bonus)
                else:
                    # Offensive TD
                    pts = 6 + pat_bonus
                drive_points[drive_start:i + 1] = pts
                # Next play starts a new drive
                if i + 1 < g_end:
                    drive_start = i + 1
                    drive_poss = poss_arr[i + 1] if i + 1 < g_end else None
                    drive_half = half_vals[i + 1] if i + 1 < g_end else None
            elif is_fg[i]:
                drive_points[drive_start:i + 1] = 3.0
                if i + 1 < g_end:
                    drive_start = i + 1
                    drive_poss = poss_arr[i + 1] if i + 1 < g_end else None
                    drive_half = half_vals[i + 1] if i + 1 < g_end else None
            elif is_safety[i]:
                drive_points[drive_start:i + 1] = -2.0
                if i + 1 < g_end:
                    drive_start = i + 1
                    drive_poss = poss_arr[i + 1] if i + 1 < g_end else None
                    drive_half = half_vals[i + 1] if i + 1 < g_end else None

        # End of game — label any remaining drive plays as 0
        if drive_start < g_end:
            # Only overwrite if not already labeled by a scoring play
            unlabeled = drive_points[drive_start:g_end] == 0.0
            # Already 0, so nothing to do — but this covers the "end of game" case

    df["drive_points"] = drive_points
    return df


# ---------------------------------------------------------------------------
# Feature matrix (vectorized)
# ---------------------------------------------------------------------------


def build_feature_matrix(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Build feature matrix X and labels y for EP model training.

    Filters to scrimmage plays with valid game state, excludes 2-point
    conversions, and constructs features.
    """
    mask = valid_play_mask(df, label_col="next_score_label")
    filtered = df.loc[mask].copy()
    logger.info("build_feature_matrix: %d -> %d rows after scrimmage filter", len(df), len(filtered))

    if filtered.empty:
        return pd.DataFrame(), pd.Series(dtype=str)

    filtered = prepare_features(filtered, include_engine_era=True)
    filtered = filtered.dropna(subset=["yardline_100"])

    if filtered.empty:
        return pd.DataFrame(), pd.Series(dtype=str)

    X = filtered[FEATURE_COLS].copy()
    y = filtered["next_score_label"]

    return X, y


def build_era_feature_matrix(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Build feature matrix without engine_era (for era-specific models)."""
    X, y = build_feature_matrix(df)
    if not X.empty and "engine_era" in X.columns:
        X = X.drop(columns=["engine_era"])
    return X, y


def build_drive_feature_matrix(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series, np.ndarray, np.ndarray]:
    """Build feature matrix with drive_points as continuous target.

    Like build_feature_matrix but:
    - Requires 'drive_points' column (from label_drive_outcome)
    - Returns continuous y (drive_points) instead of categorical labels
    - Returns sample weights (1/drive_length) so each drive contributes equally
    - Drops engine_era (for era-specific models)
    """
    mask = valid_play_mask(df, label_col="drive_points")
    filtered = df.loc[mask].copy()
    logger.info("build_drive_feature_matrix: %d -> %d rows after scrimmage filter", len(df), len(filtered))

    if filtered.empty:
        return pd.DataFrame(), pd.Series(dtype=float), np.array([]), np.array([], dtype=bool)

    filtered = prepare_features(filtered, include_engine_era=False)
    filtered = filtered.dropna(subset=["yardline_100"])

    if filtered.empty:
        return pd.DataFrame(), pd.Series(dtype=float), np.array([]), np.array([], dtype=bool)

    X = filtered[ERA_FEATURE_COLS].copy()
    y = filtered["drive_points"].astype(float)

    # Compute drive IDs and sample weights
    half_vals = np.where(
        filtered["quarter"].values <= 2, 1,
        np.where(filtered["quarter"].values <= 4, 2, 3),
    )
    drive_key = (
        filtered["game_id"].astype(str).values + "_"
        + filtered["possession_team_id"].astype(str).values + "_"
        + half_vals.astype(str)
    )
    changes = np.concatenate([[True], drive_key[1:] != drive_key[:-1]])
    drive_ids = np.cumsum(changes)
    drive_lengths = pd.Series(drive_ids).groupby(drive_ids).transform("count").values
    weights = 1.0 / drive_lengths
    is_drive_start = changes

    return X, y, weights, is_drive_start
