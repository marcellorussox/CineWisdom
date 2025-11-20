"""
Evaluation metrics for CineWisdom recommender systems.
"""

from __future__ import annotations

import numpy as np
from typing import List, Dict, Any


def RMSE(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Root Mean Square Error.

    Args:
        y_true: True ratings
        y_pred: Predicted ratings

    Returns:
        RMSE value
    """
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def MAE(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Mean Absolute Error.

    Args:
        y_true: True ratings
        y_pred: Predicted ratings

    Returns:
        MAE value
    """
    return float(np.mean(np.abs(y_true - y_pred)))


def PrecisionAtK(y_true: np.ndarray, y_pred: np.ndarray, k: int = 10, threshold: float = 4.0) -> float:
    """
    Precision@K - Fraction of recommended items in top-k that are relevant.

    Args:
        y_true: True ratings
        y_pred: Predicted ratings
        k: Number of top recommendations to consider
        threshold: Threshold for relevance

    Returns:
        Precision@K value
    """
    # Convert to binary relevance
    y_true_binary = (y_true >= threshold).astype(int)
    y_pred_binary = (y_pred >= threshold).astype(int)

    # Get top-k predictions
    top_k_indices = np.argsort(y_pred)[-k:]
    precision = np.mean([y_true_binary[i] for i in top_k_indices])

    return float(precision)


def NDCGAtK(y_true: np.ndarray, y_pred: np.ndarray, k: int = 10) -> float:
    """
    Normalized Discounted Cumulative Gain at K.

    Args:
        y_true: True ratings
        y_pred: Predicted ratings
        k: Number of top recommendations to consider

    Returns:
        NDCG@K value
    """
    # Get top-k indices
    top_k_indices = np.argsort(y_pred)[-k:][::-1]

    # Calculate DCG
    dcg = 0.0
    for i, idx in enumerate(top_k_indices):
        dcg += (2 ** y_true[idx] - 1) / np.log2(i + 2)

    # Calculate IDCG
    y_true_sorted = np.sort(y_true)[::-1]
    idcg = 0.0
    for i in range(min(k, len(y_true_sorted))):
        idcg += (2 ** y_true_sorted[i] - 1) / np.log2(i + 2)

    # Return NDCG
    if idcg == 0:
        return 0.0
    return float(dcg / idcg)


def evaluate_predictions(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    metrics: List[str] = None
) -> Dict[str, float]:
    """
    Evaluate predictions using multiple metrics.

    Args:
        y_true: True ratings
        y_pred: Predicted ratings
        metrics: List of metric names to compute

    Returns:
        Dictionary of metric values
    """
    if metrics is None:
        metrics = ['rmse', 'mae', 'precision@10', 'ndcg@10']

    results = {}

    for metric in metrics:
        metric_lower = metric.lower()
        if metric_lower == 'rmse':
            results[metric] = RMSE(y_true, y_pred)
        elif metric_lower == 'mae':
            results[metric] = MAE(y_true, y_pred)
        elif metric_lower.startswith('precision@'):
            k = int(metric.split('@')[1])
            results[metric] = PrecisionAtK(y_true, y_pred, k=k)
        elif metric_lower.startswith('ndcg@'):
            k = int(metric.split('@')[1])
            results[metric] = NDCGAtK(y_true, y_pred, k=k)
        else:
            raise ValueError(f"Unknown metric: {metric}")

    return results
