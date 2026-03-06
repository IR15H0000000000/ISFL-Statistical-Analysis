"""Expected Points Added (EPA) module.

Provides EP model training, EPA calculation per play, and aggregation.
"""

from isfl_epa.epa.calculator import compute_epa_for_df, compute_epa_for_season
from isfl_epa.epa.model import EPModel, EPModelPair
from isfl_epa.epa.score_reconstruct import reconstruct_game_scores

__all__ = [
    "EPModel",
    "EPModelPair",
    "compute_epa_for_df",
    "compute_epa_for_season",
    "reconstruct_game_scores",
]
