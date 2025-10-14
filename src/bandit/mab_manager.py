"""
Module: mab_manager

Implements a simple Multi-Armed Bandit (MAB) using Thompson Sampling to
balance exploration and exploitation across multiple recommender models.

Classes
-------
- ThompsonSampling: Core TS logic with Beta posterior updates per arm.
- MABManager: Thin manager to integrate TS selection with a list of
  recommender models. In this initial version, it returns placeholders
  suitable for notebook orchestration.
"""
from __future__ import annotations

from typing import List, Optional, Tuple

import numpy as np


class ThompsonSampling:
    """
    Thompson Sampling for Bernoulli rewards using Beta priors.

    Each arm i maintains a Beta(alpha_i, beta_i) posterior, initialized
    to Beta(1, 1). On a binary reward, the posterior is updated as:
      - reward == 1: alpha_i += 1
      - reward == 0: beta_i += 1
    """

    def __init__(self, num_arms: int) -> None:
        """Initialize the Thompson Sampling model.

        Parameters
        ----------
        num_arms : int
            Number of available arms (candidate recommendation models).
        """
        if not isinstance(num_arms, int) or num_arms <= 0:
            raise ValueError("num_arms must be a positive integer")

        self.num_arms: int = num_arms
        # Beta(1, 1) priors for each arm
        self.alphas: np.ndarray = np.ones(self.num_arms, dtype=float)
        self.betas: np.ndarray = np.ones(self.num_arms, dtype=float)

    def select_arm(self) -> int:
        """Select an arm index via Thompson Sampling.

        For each arm i, sample theta_i ~ Beta(alpha_i, beta_i) and return the
        index of the arm with the maximum sampled value.

        Returns
        -------
        int
            The index of the selected arm.
        """
        # Vectorized sampling for performance
        thetas = np.random.beta(self.alphas, self.betas)
        chosen = int(np.argmax(thetas))
        return chosen

    def update_arm(self, chosen_arm: int, reward: int) -> None:
        """Update the Beta posterior for the chosen arm given a binary reward.

        Parameters
        ----------
        chosen_arm : int
            Index of the selected arm to update.
        reward : int
            Binary reward in {0, 1}. 1 = success, 0 = failure.
        """
        if not isinstance(chosen_arm, int) or not (0 <= chosen_arm < self.num_arms):
            raise IndexError("chosen_arm is out of range")
        if reward not in (0, 1):
            raise ValueError("reward must be binary in {0, 1}")

        if reward == 1:
            self.alphas[chosen_arm] += 1.0
        else:
            self.betas[chosen_arm] += 1.0


class MABManager:
    """
    Manager to orchestrate Thompson Sampling over multiple recommender models.

    In this placeholder implementation, `get_recommendations()` returns the
    chosen arm index and model name. The actual recommendation generation and
    reward collection should be handled externally (e.g., in a notebook) and
    fed back via `register_feedback`.
    """

    def __init__(self, recommender_models: Optional[List[str]] = None) -> None:
        """Initialize the manager with a list of recommender models.

        Parameters
        ----------
        recommender_models : Optional[List[str]]
            List of model identifiers used as bandit arms. If None, a default
            placeholder list is used.
        """
        if recommender_models is None:
            recommender_models = ["KBRS_Hybrid", "Popularity_Baseline"]
        if not isinstance(recommender_models, list) or len(recommender_models) == 0:
            raise ValueError("recommender_models must be a non-empty list of strings")

        self.recommender_models: List[str] = recommender_models
        self.mab_instance: ThompsonSampling = ThompsonSampling(len(self.recommender_models))

    def get_recommendations(self) -> Tuple[int, str]:
        """Select a recommender model via Thompson Sampling.

        Returns
        -------
        Tuple[int, str]
            A tuple of (chosen_arm_index, chosen_model_name).
        """
        idx = self.mab_instance.select_arm()
        model_name = self.recommender_models[idx]
        # Placeholder: in the notebook, use `model_name` to actually compute recommendations
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
        self.mab_instance.update_arm(chosen_arm_index, reward)
