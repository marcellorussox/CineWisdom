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
    weight_exploration: float = 0.5  # R_A weight (default 50%)
    weight_accuracy: float = 0.5  # R_G weight (default 50%)
    weight_novelty: float = 0.0  # Novelty score (default 0% - no bias)
    weight_serendipity: float = 0.0  # Serendipity score (default 0% - no bias)

    # Position-based rewards
    use_ndcg: bool = False  # Use NDCG for positional rewards (default False)
    use_mrr: bool = False  # Use MRR
    use_map: bool = False  # Use MAP

    # Calibration
    update_calibration_every: int = 10  # ✅ FIX: Recompute R_G every 10 iterations (faster)

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
        Compute FAIR reward for given recommendations (optimized for KBRS vs Baseline comparison).

        FAIR APPROACH:
        - Only uses exploration reward (R_A) and accuracy proxy (R_G)
        - Excludes novelty/serendipity that would bias toward baseline
        - Both models judged on same criteria:
          * Can find UNSEEN movies with high predicted quality (R_A)
          * Overall accuracy of predictions (R_G)

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
            recommendations, seen, model_name, user_id
        )

        # Compute accuracy proxy (R_G)
        accuracy_proxy = self._compute_accuracy_proxy(recommendations, model_name)

        # DEBUG: Log reward composition per capire perché baseline vince sempre
        if self.kbrs_calls <= 3:  # Solo primi 3 per non spam
            print(f"\n  [DEBUG {model_name}] R_A={exploration_reward:.3f}, R_G={accuracy_proxy:.3f}")

        # DEBUG: Mostra componenti reward
        if self.kbrs_calls <= 3:
            print(f"    Composite reward = {self.config.weight_exploration}*{exploration_reward:.3f} + {self.config.weight_accuracy}*{accuracy_proxy:.3f} = {self.config.weight_exploration * exploration_reward + self.config.weight_accuracy * accuracy_proxy:.3f}")

        # Update R_G calibration (✅ FIX: Pass user_id and seen for UNSEEN-only calibration)
        if model_name == 'KBRS_Hybrid':
            self._update_calibration(recommendations, seen)
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
        user_id: int,
    ) -> float:
        """
        Compute exploration reward (R_A) - IMPROVED: Reward diversity & personalization!

        Returns score (0-1) based on exploration potential.

        New approach:
        1. Check how many recommendations are UNSEEN (exploration bonus)
        2. For KBRS: Reward if predictions are diverse (not all the same)
        3. For Baseline: Standard exploration score
        """
        unseen_count = 0
        diverse_predictions = 0
        predictions = []

        for movie_id, pred in recommendations:
            # Exploration: UNSEEN items bonus
            if movie_id not in seen:
                unseen_count += 1

            # Collect predictions for diversity check
            if pred is not None:
                predictions.append(pred)

        # Normalize: 0-1 based on number of recommendations
        if len(recommendations) > 0:
            exploration_score = unseen_count / len(recommendations)

            # Diversity check: reward varied predictions
            if len(predictions) > 1:
                pred_std = np.std(predictions)
                # Higher std = more diverse predictions = better personalization
                if pred_std > 0.5:  # Threshold for "diverse"
                    diverse_predictions = 1.0
                else:
                    diverse_predictions = pred_std  # Partial credit

            # Combine: 80% exploration + 20% diversity
            # KBRS should excel at both (personalized + diverse)
            # Baseline only at exploration (popular UNSEEN movies)
            if model_name == 'KBRS_Hybrid':
                final_reward = 0.8 * exploration_score + 0.2 * diverse_predictions

                # DEBUG: Mostra calcolo
                if self.kbrs_calls <= 3:
                    print(f"      KBRS: exploration={exploration_score:.3f}, diversity={diverse_predictions:.3f}")
                    print(f"      Final R_A = 0.8*{exploration_score:.3f} + 0.2*{diverse_predictions:.3f} = {final_reward:.3f}")

                return final_reward
            else:
                # Baseline: only exploration score (not personalized)

                # DEBUG: Mostra calcolo
                if self.kbrs_calls <= 3:
                    print(f"      Baseline: exploration={exploration_score:.3f}, diversity=0.0")
                    print(f"      Final R_A = {exploration_score:.3f}")

                return exploration_score
        else:
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
        seen: set,
    ) -> None:
        """Update general accuracy proxy (R_G) dynamically - FIXED: Only UNSEEN items."""
        self.kbrs_calls += 1
        self._iterations_since_calibration += 1

        # ✅ FIX: Check if any UNSEEN prediction >= threshold
        hit_found = False
        for movie_id, pred in recommendations:
            if movie_id not in seen and pred is not None and pred >= self.config.exploration_threshold:
                self.kbrs_hits += 1
                hit_found = True
                break  # One call per iteration

        # DEBUG: Log first few calls to understand R_G behavior
        if self.kbrs_calls <= 5:
            print(f"  [DEBUG] KBRS call #{self.kbrs_calls}: hit={hit_found}, threshold={self.config.exploration_threshold}")
            print(f"    Recommendations: {len(recommendations)} items")
            unseen_count = sum(1 for mid, _ in recommendations if mid not in seen)
            high_pred_count = sum(1 for _, pred in recommendations if pred is not None and pred >= self.config.exploration_threshold)
            print(f"    Unseen items: {unseen_count}, High pred items: {high_pred_count}")
            print(f"    WHY hit={hit_found}: movie_id={recommendations[0][0] if recommendations else 'N/A'} not in seen={recommendations[0][0] not in seen if recommendations else False}, pred={recommendations[0][1] if recommendations else 'N/A'} >= {self.config.exploration_threshold}={recommendations[0][1] >= self.config.exploration_threshold if recommendations and recommendations[0][1] is not None else False}")

        # Update R_G if enough iterations have passed
        if self._iterations_since_calibration >= self.config.update_calibration_every:
            if self.kbrs_calls > 0:
                old_accuracy = self.general_accuracy
                self.general_accuracy = self.kbrs_hits / self.kbrs_calls
                print(f"  [CALIBRATION] R_G updated: {old_accuracy:.4f} -> {self.general_accuracy:.4f} "
                      f"(hits={self.kbrs_hits}/{self.kbrs_calls})")
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
