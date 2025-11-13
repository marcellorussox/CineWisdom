"""
Advanced Reward System for MAB simulation.

This module provides:
- Positional rewards (NDCG, MRR, MAP)
- Dynamic calibration (R_G computed in real-time)
- Composite rewards (exploration + accuracy + novelty)
- Fair baseline rewards

Features:
- Reward normalization across different strategies
- Configurable reward weights
- Real-time performance tracking
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import List, Tuple, Dict, Optional, NamedTuple
from dataclasses import dataclass


@dataclass
class RewardConfig:
    """Configuration for reward computation."""
    # Exploration reward
    exploration_threshold: float = 4.0  # Rating threshold for positive rewards

    # Reward weights for composite scoring
    weight_exploration: float = 0.4  # R_A weight
    weight_accuracy: float = 0.3  # R_G weight
    weight_novelty: float = 0.2  # Novelty score
    weight_serendipity: float = 0.1  # Serendipity score

    # Position-based rewards
    use_ndcg: bool = True  # Use NDCG for positional rewards
    use_mrr: bool = False  # Use MRR
    use_map: bool = False  # Use MAP

    # Calibration
    update_calibration_every: int = 50  # Recompute R_G every N iterations

    # Novelty settings
    novelty_penalty: float = 0.1  # Penalize already-seen items


class RewardMetrics(NamedTuple):
    """Container for computed reward metrics."""
    exploration_reward: float  # R_A: 0 or 1
    accuracy_proxy: float  # R_G: 0 to 1
    novelty_score: float  # 0 to 1
    serendipity_score: float  # 0 to 1
    composite_reward: float  # Weighted combination


class AdvancedRewardSystem:
    """
    Advanced reward system for MAB with multiple reward strategies.

    Supported strategies:
    1. Binary Exploration (R_A): High prediction for unseen item
    2. Accuracy Proxy (R_G): Overall model accuracy
    3. Positional Rewards (NDCG/MRR/MAP): Quality of ranking
    4. Novelty/Surprise: Diversity in recommendations
    5. Composite Reward: Weighted combination of all strategies
    """

    def __init__(
        self,
        config: Optional[RewardConfig] = None,
        user_seen_movies: Optional[Dict[int, set]] = None,
        popularity_top_ids: Optional[List[int]] = None,
    ):
        self.config = config or RewardConfig()
        self.user_seen_movies = user_seen_movies or {}
        self.popularity_top_ids = popularity_top_ids or []

        # Calibration tracking
        self.kbrs_calls = 0
        self.kbrs_hits = 0
        self.general_accuracy = 0.0
        self._iterations_since_calibration = 0

        # Novelty tracking
        self.user_recommendation_history: Dict[int, List[int]] = {}

    def compute_reward(
        self,
        user_id: int,
        recommendations: List[Tuple[int, Optional[float]]],
        model_name: str,
    ) -> RewardMetrics:
        """
        Compute comprehensive reward for given recommendations.

        Args:
            user_id: User ID
            recommendations: List of (movie_id, predicted_rating) pairs
            model_name: Name of the model (KBRS_Hybrid or Popularity_Baseline)

        Returns:
            RewardMetrics container with all computed metrics
        """
        seen = self.user_seen_movies.get(user_id, set())

        # Compute exploration reward (R_A)
        exploration_reward = self._compute_exploration_reward(
            recommendations, seen, model_name
        )

        # Compute accuracy proxy (R_G)
        accuracy_proxy = self._compute_accuracy_proxy(recommendations, model_name)

        # Update R_G calibration
        if model_name == 'KBRS_Hybrid':
            self._update_calibration(recommendations)
            accuracy_proxy = self.general_accuracy

        # Compute novelty score
        novelty_score = self._compute_novelty_score(user_id, recommendations, seen)

        # Compute serendipity score
        serendipity_score = self._compute_serendipity_score(
            user_id, recommendations, seen
        )

        # Compute composite reward
        composite_reward = (
            self.config.weight_exploration * exploration_reward +
            self.config.weight_accuracy * accuracy_proxy +
            self.config.weight_novelty * novelty_score +
            self.config.weight_serendipity * serendipity_score
        )

        return RewardMetrics(
            exploration_reward=exploration_reward,
            accuracy_proxy=accuracy_proxy,
            novelty_score=novelty_score,
            serendipity_score=serendipity_score,
            composite_reward=composite_reward,
        )

    def _compute_exploration_reward(
        self,
        recommendations: List[Tuple[int, Optional[float]]],
        seen: set,
        model_name: str,
    ) -> float:
        """
        Compute exploration reward (R_A).

        Returns 1 if:
        - KBRS: Unseen movie with predicted_rating >= threshold
        - Baseline: At least one popular unseen movie
        """
        has_predictions = any(pred is not None for _, pred in recommendations)

        if has_predictions:
            # KBRS model: check for high-quality unseen items
            for movie_id, pred in recommendations:
                if movie_id not in seen and pred is not None and pred >= self.config.exploration_threshold:
                    return 1.0
            return 0.0
        else:
            # Baseline model: check for popular unseen items
            for movie_id, _ in recommendations:
                if movie_id not in seen and movie_id in self.popularity_top_ids:
                    return 1.0
            return 0.0

    def _compute_accuracy_proxy(
        self,
        recommendations: List[Tuple[int, Optional[float]]],
        model_name: str,
    ) -> float:
        """
        Compute accuracy proxy (R_G).

        For KBRS: Returns the general accuracy from calibration.
        For Baseline: Returns 0 (non-personalized).
        """
        if model_name == 'KBRS_Hybrid':
            return self.general_accuracy
        else:
            return 0.0

    def _compute_novelty_score(
        self,
        user_id: int,
        recommendations: List[Tuple[int, Optional[float]]],
        seen: set,
    ) -> float:
        """
        Compute novelty score (0 to 1).

        Based on:
        - Proportion of unseen items
        - Diversity from past recommendations
        - Popularity (less popular = more novel)
        """
        if not recommendations:
            return 0.0

        # Count unseen items
        unseen_count = sum(1 for movie_id, _ in recommendations if movie_id not in seen)
        unseen_ratio = unseen_count / len(recommendations)

        # Check diversity from history
        history = self.user_recommendation_history.get(user_id, [])
        if history:
            diversity = len(set([mid for mid, _ in recommendations]) & set(history))
            diversity_penalty = diversity / len(recommendations)
            unseen_ratio *= (1 - diversity_penalty)

        # Update history
        if user_id not in self.user_recommendation_history:
            self.user_recommendation_history[user_id] = []
        self.user_recommendation_history[user_id].extend([mid for mid, _ in recommendations])

        return min(1.0, unseen_ratio)

    def _compute_serendipity_score(
        self,
        user_id: int,
        recommendations: List[Tuple[int, Optional[float]]],
        seen: set,
    ) -> float:
        """
        Compute serendipity score (0 to 1).

        Based on unexpectedness + relevance:
        - Low predicted rating (unexpected) but still >= threshold (relevant)
        """
        if not recommendations:
            return 0.0

        serendipity_count = 0
        for movie_id, pred in recommendations:
            if movie_id not in seen and pred is not None:
                # Serendipity: moderate quality (3.5-4.0) which is good but unexpected
                if 3.5 <= pred < 4.0:
                    serendipity_count += 1

        return serendipity_count / len(recommendations)

    def _update_calibration(
        self,
        recommendations: List[Tuple[int, Optional[float]]],
    ) -> None:
        """Update general accuracy proxy (R_G) dynamically."""
        self.kbrs_calls += 1
        self._iterations_since_calibration += 1

        # Check if any prediction >= threshold (ignoring seen/unseen)
        if any(pred is not None and pred >= self.config.exploration_threshold for _, pred in recommendations):
            self.kbrs_hits += 1

        # Update R_G if enough iterations have passed
        if self._iterations_since_calibration >= self.config.update_calibration_every:
            if self.kbrs_calls > 0:
                self.general_accuracy = self.kbrs_hits / self.kbrs_calls
            self._iterations_since_calibration = 0

    def compute_position_based_reward(
        self,
        recommendations: List[Tuple[int, Optional[float]]],
        user_id: int,
    ) -> float:
        """
        Compute position-based reward (NDCG, MRR, or MAP).

        Args:
            recommendations: List of (movie_id, predicted_rating) pairs
            user_id: User ID

        Returns:
            Position-based reward score
        """
        if not recommendations:
            return 0.0

        # Get relevance scores (binary: >= threshold = 1, else 0)
        seen = self.user_seen_movies.get(user_id, set())
        relevance_scores = [
            1 if mid not in seen and pred is not None and pred >= self.config.exploration_threshold else 0
            for mid, pred in recommendations
        ]

        if self.config.use_ndcg:
            return self._compute_ndcg(relevance_scores)
        elif self.config.use_mrr:
            return self._compute_mrr(relevance_scores)
        elif self.config.use_map:
            return self._compute_map(relevance_scores)
        else:
            # Fallback to simple cumulative reward
            return sum(relevance_scores) / len(relevance_scores)

    def _compute_ndcg(self, relevance_scores: List[int], k: Optional[int] = None) -> float:
        """Compute Normalized Discounted Cumulative Gain."""
        k = k or len(relevance_scores)
        k = min(k, len(relevance_scores))

        # DCG
        dcg = sum(
            (2**rel - 1) / np.log2(i + 2)
            for i, rel in enumerate(relevance_scores[:k])
        )

        # IDCG (ideal DCG)
        ideal_relevance = sorted(relevance_scores[:k], reverse=True)
        idcg = sum(
            (2**rel - 1) / np.log2(i + 2)
            for i, rel in enumerate(ideal_relevance)
        )

        return dcg / idcg if idcg > 0 else 0.0

    def _compute_mrr(self, relevance_scores: List[int]) -> float:
        """Compute Mean Reciprocal Rank."""
        for i, rel in enumerate(relevance_scores):
            if rel > 0:
                return 1.0 / (i + 1)
        return 0.0

    def _compute_map(self, relevance_scores: List[int]) -> float:
        """Compute Mean Average Precision."""
        if not relevance_scores or sum(relevance_scores) == 0:
            return 0.0

        precision_at_k = []
        relevant_count = 0

        for i, rel in enumerate(relevance_scores):
            if rel > 0:
                relevant_count += 1
                precision_at_k.append(relevant_count / (i + 1))

        return sum(precision_at_k) / sum(relevance_scores)

    def get_calibration_metrics(self) -> Dict:
        """Get current calibration metrics."""
        return {
            'general_accuracy': self.general_accuracy,
            'kbrs_calls': self.kbrs_calls,
            'kbrs_hits': self.kbrs_hits,
            'iterations_since_calibration': self._iterations_since_calibration,
        }

    def reset_calibration(self) -> None:
        """Reset calibration metrics."""
        self.kbrs_calls = 0
        self.kbrs_hits = 0
        self.general_accuracy = 0.0
        self._iterations_since_calibration = 0

    def __repr__(self) -> str:
        return (f"AdvancedRewardSystem(accuracy={self.general_accuracy:.3f}, "
                f"calls={self.kbrs_calls}, hits={self.kbrs_hits})")
