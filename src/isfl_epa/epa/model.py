"""Expected Points model: train, predict, save/load.

Supports two model types:
- HistGradientBoostingClassifier (default, better for non-linear patterns)
- Multinomial logistic regression (simpler, more interpretable)

Both predict P(next_score_type | game_state), then EP = sum(P_i * points_i).
"""

from __future__ import annotations

import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler

from isfl_epa.epa.dataset import LABEL_POINT_VALUES

MODEL_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "models"
DEFAULT_MODEL_PATH = MODEL_DIR / "ep_model.joblib"
MODEL_2016_PATH = MODEL_DIR / "ep_model_2016.joblib"
MODEL_2022_PATH = MODEL_DIR / "ep_model_2022.joblib"


class EPModel:
    """Expected Points model wrapper."""

    def __init__(self, model=None, scaler=None, model_type: str = "hgb"):
        self.model = model
        self.scaler = scaler
        self.model_type = model_type
        self.classes_: np.ndarray | None = None

    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        model_type: str = "hgb",
        calibrate: bool = True,
        sample_weight: np.ndarray | None = None,
    ) -> dict:
        """Train the EP model.

        Args:
            X: Feature matrix.
            y: Next-score labels.
            model_type: "hgb" or "logistic".
            calibrate: Wrap in CalibratedClassifierCV for better probabilities.

        Returns:
            Dict with evaluation metrics.
        """
        self.model_type = model_type

        if model_type == "hgb_reg":
            from isfl_epa.config import (
                EPA_MODEL_LEARNING_RATE,
                EPA_MODEL_MAX_DEPTH,
                EPA_MODEL_MAX_ITER,
                EPA_MODEL_MIN_SAMPLES_LEAF,
            )

            self.scaler = None
            self.model = HistGradientBoostingRegressor(
                max_iter=EPA_MODEL_MAX_ITER,
                max_depth=EPA_MODEL_MAX_DEPTH,
                learning_rate=EPA_MODEL_LEARNING_RATE,
                min_samples_leaf=EPA_MODEL_MIN_SAMPLES_LEAF,
                random_state=42,
            )
            self.model.fit(X, y, sample_weight=sample_weight)
            self.classes_ = None
            preds = self.model.predict(X)
            return {
                "train_mae": mean_absolute_error(y, preds, sample_weight=sample_weight),
                "train_r2": r2_score(y, preds, sample_weight=sample_weight),
                "model_type": model_type,
            }

        if model_type == "hgb":
            from isfl_epa.config import (
                EPA_MODEL_LEARNING_RATE,
                EPA_MODEL_MAX_DEPTH,
                EPA_MODEL_MAX_ITER,
                EPA_MODEL_MIN_SAMPLES_LEAF,
            )

            base = HistGradientBoostingClassifier(
                max_iter=EPA_MODEL_MAX_ITER,
                max_depth=EPA_MODEL_MAX_DEPTH,
                learning_rate=EPA_MODEL_LEARNING_RATE,
                min_samples_leaf=EPA_MODEL_MIN_SAMPLES_LEAF,
                random_state=42,
            )
            self.scaler = None
        elif model_type == "logistic":
            self.scaler = StandardScaler()
            X = pd.DataFrame(
                self.scaler.fit_transform(X), columns=X.columns, index=X.index
            )
            base = LogisticRegression(
                solver="lbfgs",
                max_iter=1000,
                C=1.0,
                random_state=42,
            )
        else:
            raise ValueError(f"Unknown model_type: {model_type}")

        if calibrate:
            self.model = CalibratedClassifierCV(base, cv=5, method="isotonic")
        else:
            self.model = base

        self.model.fit(X, y)
        self.classes_ = self.model.classes_

        # Evaluate on training data (caller should also evaluate on test set)
        probs = self.model.predict_proba(X)
        train_log_loss = log_loss(y, probs, labels=self.classes_)

        return {"train_log_loss": train_log_loss, "model_type": model_type}

    def predict_ep(self, X: pd.DataFrame) -> np.ndarray:
        """Predict expected points for each row.

        For regression models: direct prediction.
        For classification: EP = sum(P(class_i) * point_value_i).
        """
        X_input = X
        if self.scaler is not None:
            X_input = pd.DataFrame(
                self.scaler.transform(X), columns=X.columns, index=X.index
            )

        if self.model_type == "hgb_reg":
            return self.model.predict(X_input)

        probs = self.model.predict_proba(X_input)
        point_values = np.array(
            [LABEL_POINT_VALUES[c] for c in self.classes_]
        )
        return probs @ point_values

    def predict_proba(self, X: pd.DataFrame) -> pd.DataFrame:
        """Return probability for each score type."""
        X_input = X
        if self.scaler is not None:
            X_input = pd.DataFrame(
                self.scaler.transform(X), columns=X.columns, index=X.index
            )
        probs = self.model.predict_proba(X_input)
        return pd.DataFrame(probs, columns=self.classes_, index=X.index)

    def evaluate(
        self, X: pd.DataFrame, y: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> dict:
        """Evaluate model on test data."""
        X_input = X
        if self.scaler is not None:
            X_input = pd.DataFrame(
                self.scaler.transform(X), columns=X.columns, index=X.index
            )

        if self.model_type == "hgb_reg":
            preds = self.model.predict(X_input)
            return {
                "mae": mean_absolute_error(y, preds, sample_weight=sample_weight),
                "r2": r2_score(y, preds, sample_weight=sample_weight),
                "n_samples": len(y),
            }

        probs = self.model.predict_proba(X_input)
        return {
            "log_loss": log_loss(y, probs, labels=self.classes_),
            "n_samples": len(y),
        }

    def save(self, path: Path = DEFAULT_MODEL_PATH) -> None:
        """Serialize model to disk."""
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "model": self.model,
                "scaler": self.scaler,
                "classes": self.classes_,
                "model_type": self.model_type,
            },
            path,
        )

    @classmethod
    def load(cls, path: Path = DEFAULT_MODEL_PATH) -> EPModel:
        """Load a previously trained model."""
        try:
            data = joblib.load(path)
        except Exception as exc:
            logger.error("Failed to load model from %s: %s", path, exc)
            raise ValueError(f"Corrupt or incompatible model file: {path}") from exc
        ep = cls(
            model=data["model"],
            scaler=data.get("scaler"),
            model_type=data.get("model_type", "hgb"),
        )
        ep.classes_ = data["classes"]
        return ep


class EPModelPair:
    """Holds two era-specific EP models and dispatches by season."""

    def __init__(
        self,
        model_2016: EPModel | None = None,
        model_2022: EPModel | None = None,
    ):
        self.model_2016 = model_2016
        self.model_2022 = model_2022

    def get_model(self, season: int) -> EPModel:
        from isfl_epa.config import ENGINE_CUTOFF_SEASON

        if season < ENGINE_CUTOFF_SEASON:
            if self.model_2016 is None:
                raise ValueError("No 2016-era model loaded")
            return self.model_2016
        else:
            if self.model_2022 is None:
                raise ValueError("No 2022-era model loaded")
            return self.model_2022

    def save(
        self,
        path_2016: Path = MODEL_2016_PATH,
        path_2022: Path = MODEL_2022_PATH,
    ) -> None:
        if self.model_2016:
            self.model_2016.save(path_2016)
        if self.model_2022:
            self.model_2022.save(path_2022)

    @classmethod
    def load(
        cls,
        path_2016: Path = MODEL_2016_PATH,
        path_2022: Path = MODEL_2022_PATH,
    ) -> EPModelPair:
        m2016 = EPModel.load(path_2016) if path_2016.exists() else None
        m2022 = EPModel.load(path_2022) if path_2022.exists() else None
        return cls(model_2016=m2016, model_2022=m2022)
