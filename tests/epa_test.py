"""Tests for EPA pipeline: score reconstruction, dataset, model, and calculator."""

import numpy as np
import pandas as pd
import pytest

from isfl_epa.epa.calculator import compute_epa_for_df
from isfl_epa.epa.dataset import (
    LABEL_POINT_VALUES,
    build_feature_matrix,
    clock_to_seconds,
    label_drive_outcome,
    label_next_score,
)
from isfl_epa.epa.model import EPModel, EPModelPair
from isfl_epa.epa.score_reconstruct import get_final_score, reconstruct_game_scores
from isfl_epa.parser.schema import Game, ParsedPlay, PlayType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_game(plays: list[ParsedPlay], season: int = 50) -> Game:
    return Game(
        id=1000, season=season, league="ISFL",
        home_team="HOM", away_team="AWY",
        home_team_id=1, away_team_id=2,
        plays=plays,
    )


def _play(play_type: PlayType, description: str, **kwargs) -> ParsedPlay:
    return ParsedPlay(
        game_id=1000, quarter=1, clock="10:00",
        play_type=play_type, description=description,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Score Reconstruction
# ---------------------------------------------------------------------------


class TestScoreReconstruct:
    def test_td_and_pat(self):
        game = _make_game([
            _play(PlayType.RUSH, "Rush by RB, A. for 5 yds",
                  rusher="RB, A.", yards_gained=5, possession_team_id=1),
            _play(PlayType.PASS, "Pass by QB, A., complete to WR, B. for 40 yds TOUCHDOWN",
                  passer="QB, A.", touchdown=True, possession_team_id=1, pat_good=True),
            _play(PlayType.KICKOFF, "Kickoff by K, A.",
                  kicker="K, A.", possession_team_id=1),
        ])
        result = reconstruct_game_scores(game)
        # Before TD: 0-0
        assert result.plays[0].score_home == 0
        assert result.plays[0].score_away == 0
        # After TD+PAT, kickoff should show 7-0
        assert result.plays[2].score_home == 7
        assert result.plays[2].score_away == 0

    def test_field_goal(self):
        game = _make_game([
            _play(PlayType.FIELD_GOAL, "30 yard FG by K, A. is good",
                  kicker="K, A.", fg_good=True, fg_distance=30,
                  possession_team_id=2),
        ])
        result = reconstruct_game_scores(game)
        assert result.plays[0].score_away == 0
        assert result.plays[0].score_home == 0
        away, home = get_final_score(result)
        assert away == 3
        assert home == 0

    def test_safety(self):
        game = _make_game([
            _play(PlayType.SACK, "Sack on QB, A. Safety",
                  sacker="DL, B.", passer="QB, A.", safety=True,
                  possession_team_id=1),
        ])
        result = reconstruct_game_scores(game)
        # Safety: defensive team (away, id=2) scores 2
        away, home = get_final_score(result)
        assert away == 2
        assert home == 0

    def test_skips_games_with_existing_scores(self):
        game = _make_game([
            _play(PlayType.RUSH, "Rush for 5",
                  rusher="RB, A.", yards_gained=5, possession_team_id=1,
                  score_home=14, score_away=7),
        ])
        result = reconstruct_game_scores(game)
        # Should not modify
        assert result.plays[0].score_home == 14
        assert result.plays[0].score_away == 7


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class TestClockToSeconds:
    def test_q1(self):
        assert clock_to_seconds("15:00", 1) == 1800
        assert clock_to_seconds("0:00", 1) == 900

    def test_q2(self):
        assert clock_to_seconds("15:00", 2) == 900
        assert clock_to_seconds("0:00", 2) == 0

    def test_q3(self):
        assert clock_to_seconds("10:00", 3) == 1500

    def test_q4(self):
        assert clock_to_seconds("5:00", 4) == 300

    def test_ot(self):
        assert clock_to_seconds("10:00", 5) == 0

    def test_invalid(self):
        assert clock_to_seconds("", 1) == 0


class TestLabelNextScore:
    def _make_df(self, plays_data: list[dict]) -> pd.DataFrame:
        """Create a minimal plays DataFrame for labeling."""
        defaults = {
            "game_id": 1000, "quarter": 1, "clock": "10:00",
            "play_type": "pass", "possession_team_id": 1,
            "touchdown": False, "fg_good": None, "safety": False,
            "description": "play",
        }
        rows = [{**defaults, **p} for p in plays_data]
        return pd.DataFrame(rows)

    def test_no_scoring(self):
        df = self._make_df([
            {"play_type": "rush"},
            {"play_type": "pass"},
        ])
        result = label_next_score(df)
        assert all(result["next_score_label"] == "no_score")

    def test_td_labels(self):
        df = self._make_df([
            {"play_type": "rush", "possession_team_id": 1},
            {"play_type": "pass", "possession_team_id": 1, "touchdown": True},
        ])
        result = label_next_score(df)
        # Both plays should be td_pos (possession team scores)
        assert result.iloc[0]["next_score_label"] == "td_pos"
        assert result.iloc[1]["next_score_label"] == "td_pos"

    def test_opponent_td_labels(self):
        df = self._make_df([
            {"play_type": "rush", "possession_team_id": 1},
            {"play_type": "rush", "possession_team_id": 2, "touchdown": True},
        ])
        result = label_next_score(df)
        # First play: opponent scores, so td_neg
        assert result.iloc[0]["next_score_label"] == "td_neg"
        # Second play: own team scores, so td_pos
        assert result.iloc[1]["next_score_label"] == "td_pos"

    def test_halftime_resets(self):
        df = self._make_df([
            {"play_type": "rush", "quarter": 2, "possession_team_id": 1},
            {"play_type": "rush", "quarter": 3, "possession_team_id": 1, "touchdown": True},
        ])
        result = label_next_score(df)
        # Q2 play shouldn't see the Q3 TD
        assert result.iloc[0]["next_score_label"] == "no_score"
        assert result.iloc[1]["next_score_label"] == "td_pos"


# ---------------------------------------------------------------------------
# EP Model
# ---------------------------------------------------------------------------


class TestEPModel:
    def _make_training_data(self, n=500):
        """Create synthetic training data."""
        rng = np.random.RandomState(42)
        X = pd.DataFrame({
            "down": rng.randint(1, 5, n),
            "distance": rng.randint(1, 20, n),
            "yardline_100": rng.randint(1, 100, n),
            "score_differential": rng.randint(-14, 15, n),
            "half_seconds_remaining": rng.randint(0, 1800, n),
            "is_home": rng.randint(0, 2, n),
            "is_overtime": np.zeros(n, dtype=int),
            "engine_era": rng.randint(0, 2, n),
        })
        labels = list(LABEL_POINT_VALUES.keys())
        y = pd.Series(rng.choice(labels, n))
        return X, y

    def test_train_and_predict(self):
        X, y = self._make_training_data()
        model = EPModel()
        metrics = model.train(X, y, model_type="hgb", calibrate=False)
        assert "train_log_loss" in metrics

        ep = model.predict_ep(X)
        assert len(ep) == len(X)
        # EP should be bounded roughly by min/max point values
        assert ep.min() >= -8
        assert ep.max() <= 8

    def test_save_load(self, tmp_path):
        X, y = self._make_training_data()
        model = EPModel()
        model.train(X, y, model_type="hgb", calibrate=False)

        path = tmp_path / "test_model.joblib"
        model.save(path)

        loaded = EPModel.load(path)
        ep_orig = model.predict_ep(X[:10])
        ep_loaded = loaded.predict_ep(X[:10])
        np.testing.assert_array_almost_equal(ep_orig, ep_loaded)

    def test_logistic_model(self):
        X, y = self._make_training_data()
        model = EPModel()
        metrics = model.train(X, y, model_type="logistic", calibrate=False)
        assert "train_log_loss" in metrics

        ep = model.predict_ep(X)
        assert len(ep) == len(X)

    def test_evaluate(self):
        X, y = self._make_training_data()
        model = EPModel()
        model.train(X, y, model_type="hgb", calibrate=False)
        metrics = model.evaluate(X, y)
        assert "log_loss" in metrics
        assert metrics["n_samples"] == len(y)


class TestEPModelPair:
    def _train_model(self, n=500, with_engine_era=False):
        """Create and train a small synthetic model."""
        rng = np.random.RandomState(42)
        cols = {
            "down": rng.randint(1, 5, n),
            "distance": rng.randint(1, 20, n),
            "yardline_100": rng.randint(1, 100, n),
            "score_differential": rng.randint(-14, 15, n),
            "half_seconds_remaining": rng.randint(0, 1800, n),
            "is_home": rng.randint(0, 2, n),
            "is_overtime": np.zeros(n, dtype=int),
        }
        if with_engine_era:
            cols["engine_era"] = rng.randint(0, 2, n)
        X = pd.DataFrame(cols)
        labels = list(LABEL_POINT_VALUES.keys())
        y = pd.Series(rng.choice(labels, n))
        model = EPModel()
        model.train(X, y, model_type="hgb", calibrate=False)
        return model

    def test_get_model_routes_by_season(self):
        m2016 = self._train_model()
        m2022 = self._train_model()
        pair = EPModelPair(model_2016=m2016, model_2022=m2022)

        assert pair.get_model(1) is m2016
        assert pair.get_model(26) is m2016
        assert pair.get_model(27) is m2022
        assert pair.get_model(59) is m2022

    def test_missing_era_raises(self):
        pair = EPModelPair(model_2016=None, model_2022=self._train_model())
        with pytest.raises(ValueError, match="2016"):
            pair.get_model(10)

        pair2 = EPModelPair(model_2016=self._train_model(), model_2022=None)
        with pytest.raises(ValueError, match="2022"):
            pair2.get_model(30)

    def test_save_load(self, tmp_path):
        m2016 = self._train_model()
        m2022 = self._train_model()
        pair = EPModelPair(model_2016=m2016, model_2022=m2022)

        p2016 = tmp_path / "ep_2016.joblib"
        p2022 = tmp_path / "ep_2022.joblib"
        pair.save(p2016, p2022)

        loaded = EPModelPair.load(p2016, p2022)
        assert loaded.model_2016 is not None
        assert loaded.model_2022 is not None

        rng = np.random.RandomState(0)
        X = pd.DataFrame({
            "down": rng.randint(1, 5, 10),
            "distance": rng.randint(1, 20, 10),
            "yardline_100": rng.randint(1, 100, 10),
            "score_differential": rng.randint(-14, 15, 10),
            "half_seconds_remaining": rng.randint(0, 1800, 10),
            "is_home": rng.randint(0, 2, 10),
            "is_overtime": np.zeros(10, dtype=int),
        })
        np.testing.assert_array_almost_equal(
            m2016.predict_ep(X), loaded.model_2016.predict_ep(X),
        )


# ---------------------------------------------------------------------------
# Drive-outcome labeling
# ---------------------------------------------------------------------------


class TestLabelDriveOutcome:
    def _make_df(self, plays_data: list[dict]) -> pd.DataFrame:
        defaults = {
            "game_id": 1000, "quarter": 1, "clock": "10:00",
            "play_type": "pass", "possession_team_id": 1,
            "touchdown": False, "fg_good": None, "safety": False,
            "interception": False, "fumble_lost": False,
            "pat_good": None, "description": "play",
        }
        rows = [{**defaults, **p} for p in plays_data]
        return pd.DataFrame(rows)

    def test_td_with_pat(self):
        df = self._make_df([
            {"play_type": "rush", "possession_team_id": 1},
            {"play_type": "pass", "possession_team_id": 1,
             "touchdown": True, "pat_good": True},
        ])
        result = label_drive_outcome(df)
        assert all(result["drive_points"] == 7)

    def test_td_missed_pat(self):
        df = self._make_df([
            {"play_type": "rush", "possession_team_id": 1},
            {"play_type": "pass", "possession_team_id": 1,
             "touchdown": True, "pat_good": False},
        ])
        result = label_drive_outcome(df)
        assert all(result["drive_points"] == 6)

    def test_pick_six(self):
        df = self._make_df([
            {"play_type": "rush", "possession_team_id": 1},
            {"play_type": "pass", "possession_team_id": 1,
             "interception": True, "touchdown": True, "pat_good": True},
        ])
        result = label_drive_outcome(df)
        assert all(result["drive_points"] == -7)

    def test_fumble_return_td(self):
        df = self._make_df([
            {"play_type": "rush", "possession_team_id": 1,
             "fumble_lost": True, "touchdown": True, "pat_good": True},
        ])
        result = label_drive_outcome(df)
        assert result.iloc[0]["drive_points"] == -7

    def test_field_goal(self):
        df = self._make_df([
            {"play_type": "rush", "possession_team_id": 1},
            {"play_type": "field_goal", "possession_team_id": 1, "fg_good": True},
        ])
        result = label_drive_outcome(df)
        assert all(result["drive_points"] == 3)

    def test_safety(self):
        df = self._make_df([
            {"play_type": "rush", "possession_team_id": 1, "safety": True},
        ])
        result = label_drive_outcome(df)
        assert result.iloc[0]["drive_points"] == -2

    def test_punt_turnover(self):
        """Drive ending in possession change (punt/turnover) = 0 points."""
        df = self._make_df([
            {"play_type": "rush", "possession_team_id": 1},
            {"play_type": "pass", "possession_team_id": 1},
            {"play_type": "rush", "possession_team_id": 2},  # new drive
        ])
        result = label_drive_outcome(df)
        # First two plays: drive ended without scoring → 0
        assert result.iloc[0]["drive_points"] == 0
        assert result.iloc[1]["drive_points"] == 0

    def test_halftime_resets(self):
        df = self._make_df([
            {"play_type": "rush", "quarter": 2, "possession_team_id": 1},
            {"play_type": "rush", "quarter": 3, "possession_team_id": 1,
             "touchdown": True, "pat_good": True},
        ])
        result = label_drive_outcome(df)
        # Q2 play: drive ended at half → 0
        assert result.iloc[0]["drive_points"] == 0
        # Q3 play: TD drive → 7
        assert result.iloc[1]["drive_points"] == 7


# ---------------------------------------------------------------------------
# EP Model (Regression)
# ---------------------------------------------------------------------------


class TestEPModelRegression:
    def _make_training_data(self, n=500):
        rng = np.random.RandomState(42)
        X = pd.DataFrame({
            "down": rng.randint(1, 5, n),
            "distance": rng.randint(1, 20, n),
            "yardline_100": rng.randint(1, 100, n),
            "score_differential": rng.randint(-14, 15, n),
            "half_seconds_remaining": rng.randint(0, 1800, n),
            "is_home": rng.randint(0, 2, n),
            "is_overtime": np.zeros(n, dtype=int),
        })
        # Simulate drive points: mostly 0, some 7, some 3, some -7
        vals = [0] * 300 + [7] * 80 + [3] * 50 + [-7] * 30 + [-2] * 10 + [6] * 30
        y = pd.Series(rng.choice(vals, n).astype(float))
        return X, y

    def test_train_and_predict(self):
        X, y = self._make_training_data()
        model = EPModel()
        metrics = model.train(X, y, model_type="hgb_reg")
        assert "train_mae" in metrics
        assert "train_r2" in metrics

        ep = model.predict_ep(X)
        assert len(ep) == len(X)
        assert ep.min() >= -10
        assert ep.max() <= 10

    def test_evaluate(self):
        X, y = self._make_training_data()
        model = EPModel()
        model.train(X, y, model_type="hgb_reg")
        metrics = model.evaluate(X, y)
        assert "mae" in metrics
        assert "r2" in metrics
        assert metrics["n_samples"] == len(y)

    def test_save_load(self, tmp_path):
        X, y = self._make_training_data()
        model = EPModel()
        model.train(X, y, model_type="hgb_reg")

        path = tmp_path / "test_reg_model.joblib"
        model.save(path)

        loaded = EPModel.load(path)
        ep_orig = model.predict_ep(X[:10])
        ep_loaded = loaded.predict_ep(X[:10])
        np.testing.assert_array_almost_equal(ep_orig, ep_loaded)


# ---------------------------------------------------------------------------
# EPA Calculator (drive model integration tests)
# ---------------------------------------------------------------------------


class _ConstantEPModel:
    """Stub model that returns a fixed EP value per yardline_100 bucket."""

    model_type = "hgb_reg"

    def predict_ep(self, X: pd.DataFrame) -> np.ndarray:
        # Simple: EP = yardline_100 / 50  (0 at own 0, 2.0 at opponent 0)
        return (X["yardline_100"].values / 50.0).astype(float)


def _game_df(plays: list[dict]) -> pd.DataFrame:
    """Build a minimal plays DataFrame suitable for compute_epa_for_df."""
    defaults = {
        "game_id": 9999, "season": 50, "league": "ISFL",
        "quarter": 1, "clock": "10:00",
        "play_type": "pass", "description": "play",
        "possession_team_id": 1, "possession_team": "HOM",
        "home_team": "HOM", "away_team": "AWY",
        "down": 1, "distance": 10,
        "yard_line": 25, "yard_line_team": "HOM",
        "score_home": 0, "score_away": 0,
        "touchdown": False, "interception": False, "fumble_lost": False,
        "fg_good": None, "safety": False, "pat_good": None,
    }
    rows = [{**defaults, **p} for p in plays]
    return pd.DataFrame(rows)


class TestCalculatorDriveModel:
    """Tests for drive-model specific EPA calculator logic."""

    def test_kickoff_excluded_from_epa(self):
        """Kickoff plays must NOT receive EPA even if they have down/distance."""
        df = _game_df([
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 30, "yard_line_team": "HOM",
             "touchdown": True, "pat_good": True},
            # Kickoff with down/distance data (the bug scenario)
            {"play_type": "kickoff", "possession_team_id": 1,
             "down": 1, "distance": 10,
             "yard_line": 35, "yard_line_team": "HOM"},
            {"play_type": "pass", "possession_team_id": 2,
             "possession_team": "AWY",
             "yard_line": 25, "yard_line_team": "AWY"},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        # Kickoff should have NaN EPA
        assert pd.isna(result.iloc[1]["epa"]), "Kickoff should not have EPA"
        # The scrimmage plays should have EPA
        assert pd.notna(result.iloc[0]["epa"]), "TD play should have EPA"
        assert pd.notna(result.iloc[2]["epa"]), "Scrimmage play should have EPA"

    def test_punt_excluded_from_epa(self):
        """Punt plays must NOT receive EPA."""
        df = _game_df([
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 40, "yard_line_team": "HOM"},
            {"play_type": "punt", "possession_team_id": 1,
             "down": 4, "distance": 5,
             "yard_line": 40, "yard_line_team": "HOM"},
            {"play_type": "rush", "possession_team_id": 2,
             "possession_team": "AWY",
             "yard_line": 20, "yard_line_team": "AWY"},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        assert pd.isna(result.iloc[1]["epa"]), "Punt should not have EPA"

    def test_penalty_excluded_from_epa(self):
        """Penalty plays must NOT receive EPA."""
        df = _game_df([
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 40, "yard_line_team": "HOM"},
            {"play_type": "penalty", "possession_team_id": 1,
             "down": 2, "distance": 5,
             "yard_line": 45, "yard_line_team": "HOM"},
            {"play_type": "rush", "possession_team_id": 1,
             "yard_line": 50, "yard_line_team": "HOM"},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        assert pd.isna(result.iloc[1]["epa"]), "Penalty should not have EPA"

    def test_drive_model_possession_change_returns_zero(self):
        """In drive model, possession change → ep_after=0 (not -next_ep)."""
        df = _game_df([
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 40, "yard_line_team": "HOM"},
            {"play_type": "rush", "possession_team_id": 2,
             "possession_team": "AWY",
             "yard_line": 30, "yard_line_team": "AWY"},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        # Play 0: possession changes on next play → ep_after should be 0
        assert result.iloc[0]["ep_after"] == 0.0

    def test_halfscore_model_possession_change_flips_sign(self):
        """In half-score model, possession change → ep_after = -next_ep."""
        df = _game_df([
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 40, "yard_line_team": "HOM"},
            {"play_type": "rush", "possession_team_id": 2,
             "possession_team": "AWY",
             "yard_line": 30, "yard_line_team": "AWY"},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=False)
        # Play 1 ep_before: yardline_100 = 30 (own territory) → EP = 30/50 = 0.6
        # Play 0 ep_after: -0.6 (flipped)
        assert result.iloc[0]["ep_after"] == pytest.approx(-0.6, abs=0.01)

    def test_forward_scan_prefers_ep_before_over_scoring(self):
        """When next play has ep_before AND is a scoring play, use ep_before
        (not the fixed scoring value). The scoring play handles itself."""
        df = _game_df([
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 40, "yard_line_team": "AWY"},  # yl100=60
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 5, "yard_line_team": "AWY",  # yl100=95
             "touchdown": True, "pat_good": True},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        # Play 0: next play (the TD) has ep_before = 95/50 = 1.9
        # Forward scan should return 1.9 (not 7.0)
        assert result.iloc[0]["ep_after"] == pytest.approx(1.9, abs=0.01)
        # Play 1 (TD itself): ep_after = 7 from scoring handler
        assert result.iloc[1]["ep_after"] == 7.0

    def test_forward_scan_finds_scoring_without_ep_before(self):
        """When a scoring play lacks ep_before (e.g. FG with no valid features),
        the forward scan should return the fixed scoring value."""
        df = _game_df([
            {"play_type": "rush", "possession_team_id": 1,
             "yard_line": 20, "yard_line_team": "AWY"},  # yl100=80
            # FG play missing down → no ep_before
            {"play_type": "field_goal", "possession_team_id": 1,
             "down": None, "distance": None,
             "yard_line": 20, "yard_line_team": "AWY",
             "fg_good": True},
            {"play_type": "kickoff", "possession_team_id": 1},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        # Play 0: FG has no ep_before, forward scan should find it as scoring → 3.0
        assert result.iloc[0]["ep_after"] == 3.0

    def test_defensive_td_ep_after_is_negative(self):
        """Pick-6 / fumble-return TD should give negative ep_after."""
        df = _game_df([
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 30, "yard_line_team": "HOM",
             "interception": True, "touchdown": True, "pat_good": True},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        assert result.iloc[0]["ep_after"] == -7.0

    def test_defensive_td_missed_pat(self):
        """Fumble-return TD with missed PAT → ep_after = -6."""
        df = _game_df([
            {"play_type": "rush", "possession_team_id": 1,
             "yard_line": 50, "yard_line_team": "HOM",
             "fumble_lost": True, "touchdown": True, "pat_good": False},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        assert result.iloc[0]["ep_after"] == -6.0

    def test_drive_telescoping(self):
        """Total EPA for a drive should equal drive_outcome - ep_before_first."""
        df = _game_df([
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 30, "yard_line_team": "HOM"},   # yl100=30
            {"play_type": "rush", "possession_team_id": 1,
             "yard_line": 45, "yard_line_team": "HOM"},   # yl100=45
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 40, "yard_line_team": "AWY",    # yl100=60
             "touchdown": True, "pat_good": True},
            # New drive
            {"play_type": "rush", "possession_team_id": 2,
             "possession_team": "AWY",
             "yard_line": 25, "yard_line_team": "AWY"},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        drive_plays = result.iloc[:3]
        total_epa = drive_plays["epa"].sum()
        expected = 7.0 - drive_plays.iloc[0]["ep_before"]  # TD(7) - first_ep
        assert total_epa == pytest.approx(expected, abs=0.01)

    def test_noscoring_drive_telescoping(self):
        """Non-scoring drive: total EPA = 0 - ep_before_first."""
        df = _game_df([
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 25, "yard_line_team": "HOM"},   # yl100=25
            {"play_type": "rush", "possession_team_id": 1,
             "yard_line": 35, "yard_line_team": "HOM"},   # yl100=35
            # Turnover — new drive
            {"play_type": "rush", "possession_team_id": 2,
             "possession_team": "AWY",
             "yard_line": 25, "yard_line_team": "AWY"},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        drive_plays = result.iloc[:2]
        total_epa = drive_plays["epa"].sum()
        expected = 0.0 - drive_plays.iloc[0]["ep_before"]
        assert total_epa == pytest.approx(expected, abs=0.01)

    def test_fg_drive_telescoping(self):
        """FG drive: total EPA = 3 - ep_before_first, even when FG play
        itself lacks ep_before."""
        df = _game_df([
            {"play_type": "rush", "possession_team_id": 1,
             "yard_line": 40, "yard_line_team": "HOM"},   # yl100=40
            {"play_type": "pass", "possession_team_id": 1,
             "yard_line": 25, "yard_line_team": "AWY"},   # yl100=75
            # FG play has no down/distance → no ep_before
            {"play_type": "field_goal", "possession_team_id": 1,
             "down": None, "distance": None,
             "yard_line": 25, "yard_line_team": "AWY",
             "fg_good": True},
        ])
        result = compute_epa_for_df(df, _ConstantEPModel(), era_specific=True, drive_model=True)
        # Only plays 0 and 1 have EPA (FG has no ep_before)
        valid_plays = result[result["epa"].notna()]
        total_epa = valid_plays["epa"].sum()
        expected = 3.0 - valid_plays.iloc[0]["ep_before"]
        assert total_epa == pytest.approx(expected, abs=0.01)
