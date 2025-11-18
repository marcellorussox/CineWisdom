"""
Module: mab_manager

Implements an Advanced Multi-Armed Bandit (MAB) using optimized Thompson Sampling to
balance exploration and exploitation across multiple recommender models.

Classes
-------
- AdvancedThompsonSampling: Core TS logic with optimizations (temperature scheduling,
  batch updates, regularization, etc.)
- MABManager: Manager to integrate advanced TS with recommender models.
"""
from __future__ import annotations

from typing import List, Optional, Tuple
from .algorithms import AdvancedThompsonSampling, ThompsonSamplingConfig


# Keep backward compatibility alias
ThompsonSampling = AdvancedThompsonSampling


class MABManager:
    """
    Manager to orchestrate Advanced Thompson Sampling over multiple recommender models.

    This manager integrates the Advanced Thompson Sampling algorithm with:
    - Temperature scheduling for controlled exploration
    - Batch updates for improved performance
    - Optimistic initialization for better early behavior
    - Statistics tracking for analysis

    Parameters
    ----------
    recommender_models : List[str]
        List of model identifiers used as bandit arms
    config : Optional[ThompsonSamplingConfig]
        Configuration for the Thompson Sampling algorithm
    """

    def __init__(
        self,
        recommender_models: Optional[List[str]] = None,
        config: Optional[ThompsonSamplingConfig] = None,
    ) -> None:
        """Initialize the manager with advanced Thompson Sampling configuration.

        Parameters
        ----------
        recommender_models : List[str]
            List of model identifiers used as bandit arms
        config : Optional[ThompsonSamplingConfig]
            Advanced configuration with optimizations
        """
        if recommender_models is None:
            recommender_models = ["KBRS_Hybrid", "Popularity_Baseline"]
        if not isinstance(recommender_models, list) or len(recommender_models) == 0:
            raise ValueError("recommender_models must be a non-empty list of strings")

        self.recommender_models: List[str] = recommender_models
        # Use advanced Thompson Sampling with optimized defaults
        default_config = ThompsonSamplingConfig(
            prior_strength=2.0,  # Optimistic initialization
            initial_temperature=1.0,
            min_temperature=0.1,
            temperature_decay=0.995,
            min_exploration_rate=0.05,  # Force 5% minimum exploration
            batch_size=1,  # Single updates by default (can increase for batch learning)
            regularization=0.01,
        )
        self.config = config or default_config
        self.mab_instance: AdvancedThompsonSampling = AdvancedThompsonSampling(
            num_arms=len(self.recommender_models),
            config=self.config
        )

    def get_recommendations(self) -> Tuple[int, str]:
        """Select a recommender model via Advanced Thompson Sampling.

        Returns
        -------
        Tuple[int, str]
            A tuple of (chosen_arm_index, chosen_model_name).
        """
        idx = self.mab_instance.select_arm()
        model_name = self.recommender_models[idx]

        # DEBUG: Log arm selection
        arm_name = 'KBRS' if idx == 0 else 'Baseline'
        print(f"  [MAB] Selected {arm_name} (arm {idx})")

        return idx, model_name

    def register_feedback(self, chosen_arm_index: int, reward: int) -> None:
        """Register binary feedback for the previously chosen arm.

        Parameters
        ----------
        chosen_arm_index : int
            Index of the arm that produced the displayed recommendation(s).
        reward : int
            Binary reward in {0, 1} indicating user interaction outcome.
        """
        # DEBUG: Log reward to track MAB learning
        arm_name = 'KBRS' if chosen_arm_index == 0 else 'Baseline'
        print(f"  [MAB] Received reward={reward} for {arm_name} (arm {chosen_arm_index})")

        self.mab_instance.update(chosen_arm_index, reward)

    def get_statistics(self) -> dict:
        """
        Get comprehensive statistics about the current MAB state.

        Returns
        -------
        dict
            Dictionary with detailed statistics including means, confidence intervals,
            selection counts, success rates, and exploration metrics.
        """
        return self.mab_instance.get_statistics()

    def finalize(self) -> None:
        """Process any pending batch updates and finalize statistics."""
        self.mab_instance.finalize()
