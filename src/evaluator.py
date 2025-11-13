"""
Recommender system evaluation utilities.

Module: src/evaluator.py

This module defines the `RecommenderEvaluator` class to compute standard offline
ranking metrics (Precision@K, Recall@K) given ground-truth positives derived from
user ratings and a mapping of recommended items per user.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Literal

import numpy as np
import pandas as pd


@dataclass
class UserEvalResult:
    """Container for per-user evaluation results."""
    user_id: int
    k: int
    tp: int
    positives_count: int
    precision_at_k: float
    recall_at_k: float
    f1_at_k: float
    hitrate_at_k: float
    mrr_at_k: float
    map_at_k: float
    ndcg_at_k: float


class RecommenderEvaluator:
    """
    Compute ranking metrics for recommender outputs.

    Parameters
    - ratings_df: pandas DataFrame with at least columns `userId`, `movieId`, `rating`.
    - success_threshold: float, inclusive threshold to mark a movie as a positive (default 4.0).

    Ground truth definition
    - For each user, any movie with rating >= success_threshold is considered a positive.
      These positives form the test set used for offline evaluation.
    """

    def __init__(self, ratings_df: pd.DataFrame, success_threshold: float = 4.0) -> None:
        required_cols = {"userId", "movieId", "rating"}
        missing = required_cols - set(ratings_df.columns)
        if missing:
            raise ValueError(f"ratings_df missing required columns: {sorted(missing)}")

        if not isinstance(success_threshold, (int, float)):
            raise TypeError("success_threshold must be a number")

        self.ratings_df: pd.DataFrame = ratings_df
        self.success_threshold: float = float(success_threshold)

        positives = (
            ratings_df.loc[ratings_df["rating"] >= self.success_threshold, ["userId", "movieId"]]
            .groupby("userId")["movieId"].apply(set)
        )
        # Users without positives will be absent; store empty sets for consistency on demand
        self.user_test_sets: Dict[int, Set[int]] = positives.to_dict()

    # ============================
    # Internal helpers (binary)
    # ============================
    @staticmethod
    def _dcg(relevance: Sequence[int]) -> float:
        # relevance: binary gains ordered by rank 1..K
        if not relevance:
            return 0.0
        rel = np.asarray(relevance, dtype=float)
        denom = np.log2(np.arange(2, len(rel) + 2))
        return float(np.sum(rel / denom))

    @staticmethod
    def _ndcg(relevance: Sequence[int]) -> float:
        if not relevance:
            return 0.0
        dcg = RecommenderEvaluator._dcg(relevance)
        ideal = sorted(relevance, reverse=True)
        idcg = RecommenderEvaluator._dcg(ideal)
        return float(dcg / idcg) if idcg > 0 else 0.0

    @staticmethod
    def _average_precision(relevance: Sequence[int]) -> float:
        # AP = mean precision at each relevant position
        if not relevance:
            return 0.0
        ap_sum = 0.0
        hit_count = 0
        for i, rel in enumerate(relevance, start=1):
            if rel:
                hit_count += 1
                ap_sum += hit_count / i
        return ap_sum / hit_count if hit_count > 0 else 0.0

    @staticmethod
    def _mrr(relevance: Sequence[int]) -> float:
        for i, rel in enumerate(relevance, start=1):
            if rel:
                return 1.0 / i
        return 0.0

    def _get_user_positives(self, user_id: int) -> Set[int]:
        """Return the set of positive movieIds for the given user (may be empty)."""
        return self.user_test_sets.get(int(user_id), set())

    def evaluate_ranking_metrics(
        self,
        recommended_list: Sequence[int],
        user_id: int,
        k: Optional[int] = None,
    ) -> UserEvalResult:
        """
        Compute Precision@K and Recall@K for a single user.

        Args
        - recommended_list: ordered list of recommended movieIds for the user.
        - user_id: target user id.
        - k: top-K cutoff. If None, uses len(recommended_list).

        Returns
        - UserEvalResult with precision_at_k and recall_at_k.

        Notes
        - If the user has no positives (|test set| == 0), recall is defined as 0.0.
          This avoids NaNs and keeps aggregation simple. Adjust as needed.
        """
        if recommended_list is None:
            recommended_list = []
        if k is None:
            k = len(recommended_list)
        if k < 0:
            raise ValueError("k must be non-negative")

        # Truncate to top-k
        topk = list(recommended_list)[:k]
        topk_set = set(topk)

        positives = self._get_user_positives(user_id)
        tp = len(topk_set & positives)

        denom_p = max(k, 1)  # avoid divide-by-zero if k == 0
        precision = tp / denom_p

        pos_count = len(positives)
        recall = (tp / pos_count) if pos_count > 0 else 0.0

        # Build binary relevance vector in rank order
        rel = [1 if mid in positives else 0 for mid in topk]
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
        hitrate = 1.0 if tp > 0 else 0.0
        mrr = self._mrr(rel)
        ap = self._average_precision(rel)
        ndcg = self._ndcg(rel)

        return UserEvalResult(
            user_id=int(user_id),
            k=k,
            tp=tp,
            positives_count=pos_count,
            precision_at_k=precision,
            recall_at_k=recall,
            f1_at_k=f1,
            hitrate_at_k=hitrate,
            mrr_at_k=mrr,
            map_at_k=ap,
            ndcg_at_k=ndcg,
        )

    def evaluate_all_users(
        self,
        user_recommendations_map: Dict[int, Sequence[int]],
        k: Optional[int] = None,
        *,
        metrics: Optional[Set[str]] = None,
        exclude_users_without_positives: bool = True,
        min_positives_per_user: int = 1,
        user_filter: Optional[Set[int]] = None,
        aggregate: Literal["macro", "micro", "both"] = "both",
        bootstrap_ci: bool = False,
        n_bootstrap: int = 1000,
        ci_level: float = 0.95,
    ) -> pd.DataFrame:
        """
        Evaluate Precision@K and Recall@K for a batch of users and aggregate.

        Args
        - user_recommendations_map: dict mapping user_id -> list of recommended movieIds.
        - k: top-K cutoff to apply to each user's list. If None, uses len(list) per user.
        - metrics: subset to include (default: all available).
        - exclude_users_without_positives: drop users with zero positives.
        - min_positives_per_user: require at least this many positives.
        - user_filter: evaluate only a subset of users.
        - aggregate: include macro and/or micro aggregates.
        - bootstrap_ci: if True, compute bootstrap CI for macro precision/recall.

        Returns
        - pandas DataFrame with per-user metrics and aggregate rows.
        """
        # Determine users to evaluate
        candidates = user_recommendations_map.keys()
        if user_filter is not None:
            candidates = [u for u in candidates if u in user_filter]

        # Apply positivity filters
        filtered_users: List[int] = []
        for u in candidates:
            pos = self._get_user_positives(u)
            if exclude_users_without_positives and len(pos) == 0:
                continue
            if len(pos) < min_positives_per_user:
                continue
            filtered_users.append(u)

        results: List[UserEvalResult] = []
        # 🚀 OPTIMIZED: Batch processing - processa utenti in batch invece di uno per uno
        batch_size = 100  # Process 100 users at a time
        for i in range(0, len(filtered_users), batch_size):
            batch_users = filtered_users[i:i + batch_size]
            # 🚀 OPTIMIZED: Parallel processing for evaluation (se disponibile)
            # from concurrent.futures import ThreadPoolExecutor
            # with ThreadPoolExecutor(max_workers=4) as executor:
            #     batch_results = list(executor.map(
            #         lambda uid: self.evaluate_ranking_metrics(
            #             recommended_list=user_recommendations_map.get(uid, []),
            #             user_id=uid, k=k
            #         ), batch_users
            #     ))

            # Sequential for now (easier to debug)
            for user_id in batch_users:
                recs = user_recommendations_map.get(user_id, [])
                res = self.evaluate_ranking_metrics(recommended_list=recs, user_id=user_id, k=k)
                results.append(res)

        if not results:
            cols = [
                "user_id", "k", "tp", "positives_count",
                "precision_at_k", "recall_at_k", "f1_at_k",
                "hitrate_at_k", "mrr_at_k", "map_at_k", "ndcg_at_k",
            ]
            return pd.DataFrame(columns=cols)

        df = pd.DataFrame([r.__dict__ for r in results])

        # Select metrics if requested
        all_metrics = {"precision_at_k", "recall_at_k", "f1_at_k", "hitrate_at_k", "mrr_at_k", "map_at_k", "ndcg_at_k"}
        selected = all_metrics if metrics is None else (all_metrics & set(metrics))
        base_cols = ["user_id", "k", "tp", "positives_count"]
        df = df[base_cols + sorted(selected)]

        aggregates: List[pd.DataFrame] = []

        if aggregate in ("macro", "both"):
            macro = df[list(sorted(selected))].mean().to_frame().T
            macro.insert(0, "user_id", "__overall_macro__")
            macro.insert(1, "k", k if k is not None else None)
            macro.insert(2, "tp", df["tp"].sum())
            macro.insert(3, "positives_count", df["positives_count"].sum())

            # Optional bootstrap CI for precision/recall
            if bootstrap_ci and len(df) > 1:
                rng = np.random.default_rng(42)
                b_p, b_r = [], []
                vals_p = df["precision_at_k"].to_numpy(dtype=float)
                vals_r = df["recall_at_k"].to_numpy(dtype=float)
                n = len(vals_p)
                for _ in range(n_bootstrap):
                    idx = rng.integers(0, n, size=n)
                    b_p.append(float(np.mean(vals_p[idx])))
                    b_r.append(float(np.mean(vals_r[idx])))
                lo_q = (1 - ci_level) / 2
                hi_q = 1 - lo_q
                macro["precision_at_k_ci"] = [(np.quantile(b_p, lo_q), np.quantile(b_p, hi_q))]
                macro["recall_at_k_ci"] = [(np.quantile(b_r, lo_q), np.quantile(b_r, hi_q))]

            aggregates.append(macro)

        if aggregate in ("micro", "both"):
            denom_p = df["k"].replace(0, np.nan).sum()
            micro_precision = (df["tp"].sum() / denom_p) if denom_p and not np.isnan(denom_p) else 0.0
            micro_recall = (
                df["tp"].sum() / df["positives_count"].sum()
                if df["positives_count"].sum() > 0 else 0.0
            )
            micro = pd.DataFrame({
                "user_id": ["__overall_micro__"],
                "k": [k if k is not None else None],
                "tp": [df["tp"].sum()],
                "positives_count": [df["positives_count"].sum()],
                "precision_at_k": [micro_precision],
                "recall_at_k": [micro_recall],
            })
            # For micro, other metrics are less standard; leave NaN
            for m in selected - {"precision_at_k", "recall_at_k"}:
                micro[m] = np.nan
            aggregates.append(micro)

        df_out = pd.concat([df] + aggregates, ignore_index=True) if aggregates else df
        return df_out

    # ============================
    # Optional helpers
    # ============================
    def evaluate_from_history(
        self,
        history_df: pd.DataFrame,
        *,
        user_id_col: str = "user_id",
        recs_col: str = "recommended_movie_ids",
        k: Optional[int] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Convenience wrapper to evaluate from a DataFrame that contains, per user,
        a list column with recommended movieIds.

        The DataFrame is expected to have one row per user (or pre-aggregated)
        and a column `recs_col` with a list/sequence of movieIds. If your history
        is at interaction level, pre-aggregate to a user->list mapping first.
        """
        if recs_col not in history_df.columns or user_id_col not in history_df.columns:
            raise ValueError(f"history_df must contain columns: {user_id_col}, {recs_col}")
        user_map: Dict[int, Sequence[int]] = (
            history_df[[user_id_col, recs_col]]
            .dropna()
            .set_index(user_id_col)[recs_col]
            .to_dict()
        )
        return self.evaluate_all_users(user_map, k=k, **kwargs)
