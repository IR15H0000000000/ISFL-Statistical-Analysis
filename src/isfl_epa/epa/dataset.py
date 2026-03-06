"""Data preparation for EP model training.

Loads parsed plays, labels each with the next scoring event in its half,
and builds a feature matrix for multinomial classification.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from isfl_epa.config import ENGINE_CUTOFF_SEASON
from isfl_epa.storage.parquet import DATA_DIR, read_season_plays

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

# Play types to include in training (scrimmage plays with game state)
_SCRIMMAGE_TYPES = {"pass", "rush", "sack", "field_goal"}

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
        return {}

    abbrevs = sorted(game_df["yard_line_team"].dropna().unique())
    if len(abbrevs) < 2:
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
            continue
        away_tid, home_tid = team_ids[0], team_ids[1]

        mapping = _infer_team_mapping(game_plays)
        if not mapping:
            continue

        away_abbr = mapping.get(away_tid)
        home_abbr = mapping.get(home_tid)
        if not away_abbr or not home_abbr:
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
    """Derive possession_team from home_team/away_team + possession_team_id.

    For DB rows (S27-59) that have home_team/away_team but no possession_team.
    Convention: lower possession_team_id = away, higher = home.
    """
    # Build per-game mapping: home_tid -> home_team, away_tid -> away_team
    game_info = df.groupby("game_id").agg(
        home_team=("home_team", "first"),
        away_team=("away_team", "first"),
        min_tid=("possession_team_id", "min"),
        max_tid=("possession_team_id", "max"),
    )
    # away = min tid, home = max tid
    # Create a mapping: (game_id, poss_id) -> team abbreviation
    # Merge via game_id, then match on poss_id
    merged = df[["game_id", "possession_team_id"]].merge(game_info, on="game_id", how="left")
    result = pd.Series(index=df.index, dtype=object)
    is_home = merged["possession_team_id"] == merged["max_tid"]
    is_away = merged["possession_team_id"] == merged["min_tid"]
    result[is_home.values] = merged.loc[is_home.values, "home_team"].values
    result[is_away.values] = merged.loc[is_away.values, "away_team"].values
    return result


def load_training_plays(seasons, league: str = "ISFL") -> pd.DataFrame:
    """Load and concatenate plays from Parquet and/or database.

    Strategy:
    - S1-26 (parquet): Load from parquet, reconstruct scores from scoring
      flags, infer team abbreviations from kickoff patterns.
    - S27-59 (DB): Load from PostgreSQL, derive possession_team from
      home_team/away_team + possession_team_id.
    """
    parquet_frames = []
    db_seasons = []

    for s in seasons:
        path = DATA_DIR / f"{league}_S{s}_plays.parquet"
        if path.exists():
            parquet_frames.append(read_season_plays(s, league))
        else:
            db_seasons.append(s)

    # Load DB seasons (single query for all seasons)
    db_frames = []
    if db_seasons:
        from sqlalchemy import select

        from isfl_epa.storage.database import get_engine, plays_table

        engine = get_engine()
        with engine.connect() as conn:
            stmt = (
                select(plays_table)
                .where(
                    plays_table.c.season.in_(db_seasons)
                    & (plays_table.c.league == league)
                )
                .order_by(
                    plays_table.c.game_id,
                    plays_table.c.play_index,
                )
            )
            db_chunk = pd.read_sql(stmt, conn)
            if not db_chunk.empty:
                db_frames.append(db_chunk)

    # Process parquet data (S1-26): reconstruct scores + infer teams
    if parquet_frames:
        pq_df = pd.concat(parquet_frames, ignore_index=True)
        if pq_df["score_away"].isna().any():
            pq_df = _reconstruct_scores_df(pq_df)
        if "possession_team" not in pq_df.columns:
            pq_df["possession_team"] = None
        if pq_df["possession_team"].isna().any():
            pq_df = _fill_team_info(pq_df)
    else:
        pq_df = None

    # Process DB data (S27-59): derive possession_team from home/away
    if db_frames:
        db_df = pd.concat(db_frames, ignore_index=True)
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
    else:
        db_df = None

    # Combine
    frames = [f for f in [pq_df, db_df] if f is not None]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Clock / half utilities
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


def _half_number(quarter: int) -> int:
    """Map quarter to half: 1/2 -> 1, 3/4 -> 2, OT -> 3."""
    if quarter <= 2:
        return 1
    if quarter <= 4:
        return 2
    return 3


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


def build_feature_matrix(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Build feature matrix X and labels y for EP model training.

    Filters to scrimmage plays with valid game state, excludes 2-point
    conversions, and constructs features.
    """
    # Filter to valid scrimmage plays
    mask = (
        df["play_type"].isin(_SCRIMMAGE_TYPES)
        & df["down"].notna()
        & df["distance"].notna()
        & df["yard_line"].notna()
        & df["score_away"].notna()
        & df["score_home"].notna()
        & df["next_score_label"].notna()
        & ~df["description"].str.contains("2 point|conversion", case=False, na=False)
    )
    filtered = df.loc[mask].copy()

    if filtered.empty:
        return pd.DataFrame(), pd.Series(dtype=str)

    # Compute yardline_100 (vectorized)
    filtered["yardline_100"] = compute_yardline_100(filtered)
    filtered = filtered.dropna(subset=["yardline_100"])

    if filtered.empty:
        return pd.DataFrame(), pd.Series(dtype=str)

    # Pre-compute home team ID per game (vectorized lookup)
    game_home_tid = (
        filtered.groupby("game_id")["possession_team_id"]
        .apply(lambda s: max(s.dropna().unique()) if len(s.dropna().unique()) >= 2 else np.nan)
    )
    home_tid_col = filtered["game_id"].map(game_home_tid)
    is_home = (filtered["possession_team_id"] == home_tid_col).astype(int)

    # Score differential (vectorized)
    sh = filtered["score_home"].fillna(0)
    sa = filtered["score_away"].fillna(0)
    score_diff = np.where(is_home, sh - sa, sa - sh)

    filtered["score_differential"] = pd.Series(score_diff, index=filtered.index).clip(-28, 28)
    filtered["is_home"] = is_home.values

    # Half seconds remaining (vectorized)
    clock_parts = filtered["clock"].astype(str).str.split(":", expand=True)
    minutes = pd.to_numeric(clock_parts[0], errors="coerce").fillna(0).astype(int)
    seconds = pd.to_numeric(clock_parts[1], errors="coerce").fillna(0).astype(int) if 1 in clock_parts.columns else 0
    clock_secs = minutes * 60 + seconds
    quarter = filtered["quarter"]
    half_secs = np.where(quarter.isin([1, 3]), clock_secs + 900, clock_secs)
    half_secs = np.where(quarter >= 5, 0, half_secs)
    filtered["half_seconds_remaining"] = half_secs

    filtered["is_overtime"] = (quarter >= 5).astype(int)
    filtered["engine_era"] = (filtered["season"] >= ENGINE_CUTOFF_SEASON).astype(int)

    feature_cols = [
        "down", "distance", "yardline_100", "score_differential",
        "half_seconds_remaining", "is_home", "is_overtime", "engine_era",
    ]

    X = filtered[feature_cols].copy()
    X["distance"] = X["distance"].clip(upper=30)
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
) -> tuple[pd.DataFrame, pd.Series, np.ndarray]:
    """Build feature matrix with drive_points as continuous target.

    Like build_feature_matrix but:
    - Requires 'drive_points' column (from label_drive_outcome)
    - Returns continuous y (drive_points) instead of categorical labels
    - Returns sample weights (1/drive_length) so each drive contributes equally
    - Drops engine_era (for era-specific models)
    """
    mask = (
        df["play_type"].isin(_SCRIMMAGE_TYPES)
        & df["down"].notna()
        & df["distance"].notna()
        & df["yard_line"].notna()
        & df["score_away"].notna()
        & df["score_home"].notna()
        & df["drive_points"].notna()
        & ~df["description"].str.contains("2 point|conversion", case=False, na=False)
    )
    filtered = df.loc[mask].copy()

    if filtered.empty:
        return pd.DataFrame(), pd.Series(dtype=float), np.array([])

    filtered["yardline_100"] = compute_yardline_100(filtered)
    filtered = filtered.dropna(subset=["yardline_100"])

    if filtered.empty:
        return pd.DataFrame(), pd.Series(dtype=float), np.array([])

    # Home team ID per game
    game_home_tid = (
        filtered.groupby("game_id")["possession_team_id"]
        .apply(lambda s: max(s.dropna().unique()) if len(s.dropna().unique()) >= 2 else np.nan)
    )
    home_tid_col = filtered["game_id"].map(game_home_tid)
    is_home = (filtered["possession_team_id"] == home_tid_col).astype(int)

    # Score differential
    sh = filtered["score_home"].fillna(0)
    sa = filtered["score_away"].fillna(0)
    score_diff = np.where(is_home, sh - sa, sa - sh)
    filtered["score_differential"] = pd.Series(score_diff, index=filtered.index).clip(-28, 28)
    filtered["is_home"] = is_home.values

    # Half seconds remaining
    clock_parts = filtered["clock"].astype(str).str.split(":", expand=True)
    minutes = pd.to_numeric(clock_parts[0], errors="coerce").fillna(0).astype(int)
    seconds = pd.to_numeric(clock_parts[1], errors="coerce").fillna(0).astype(int) if 1 in clock_parts.columns else 0
    clock_secs = minutes * 60 + seconds
    quarter = filtered["quarter"]
    half_secs = np.where(quarter.isin([1, 3]), clock_secs + 900, clock_secs)
    half_secs = np.where(quarter >= 5, 0, half_secs)
    filtered["half_seconds_remaining"] = half_secs

    filtered["is_overtime"] = (quarter >= 5).astype(int)

    feature_cols = [
        "down", "distance", "yardline_100", "score_differential",
        "half_seconds_remaining", "is_home", "is_overtime",
    ]

    X = filtered[feature_cols].copy()
    X["distance"] = X["distance"].clip(upper=30)
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
