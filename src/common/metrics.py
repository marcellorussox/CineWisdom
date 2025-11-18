"""
Metrics - Wrapper per metriche di valutazione

Fornisce wrapper semplificati per le metriche di valutazione
dei sistemi di raccomandazione.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
import math


def compute_rmse(ground_truth: List[float], predictions: List[float]) -> float:
    """
    Calcola Root Mean Square Error (RMSE).

    Args:
        ground_truth: Lista dei rating reali
        predictions: Lista delle predizioni

    Returns:
        RMSE value (più basso è meglio)
    """
    from src.evaluation.traditional_evaluator import TraditionalEvaluator

    evaluator = TraditionalEvaluator()
    rmse = evaluator.rmse(ground_truth, predictions)

    return rmse


def compute_mae(ground_truth: List[float], predictions: List[float]) -> float:
    """
    Calcola Mean Absolute Error (MAE).

    Args:
        ground_truth: Lista dei rating reali
        predictions: Lista delle predizioni

    Returns:
        MAE value (più basso è meglio)
    """
    from src.evaluation.traditional_evaluator import TraditionalEvaluator

    evaluator = TraditionalEvaluator()
    mae = evaluator.mae(ground_truth, predictions)

    return mae


def compute_precision_at_k(ground_truth: List[float],
                          predictions: List[float],
                          k: int = 10,
                          threshold: float = 3.5) -> float:
    """
    Calcola Precision@K.

    Args:
        ground_truth: Lista dei rating reali
        predictions: Lista delle predizioni
        k: Numero di top item da considerare
        threshold: Soglia per considerare un item "rilevante"

    Returns:
        Precision@K value
    """
    from src.evaluation.traditional_evaluator import TraditionalEvaluator

    evaluator = TraditionalEvaluator()
    precision = evaluator.precision_at_k(ground_truth, predictions, k=k, threshold=threshold)

    return precision


def compute_ndcg_at_k(ground_truth: List[float],
                     predictions: List[float],
                     k: int = 10) -> float:
    """
    Calcola Normalized Discounted Cumulative Gain (NDCG@K).

    Args:
        ground_truth: Lista dei rating reali
        predictions: Lista delle predizioni
        k: Numero di top item da considerare

    Returns:
        NDCG@K value
    """
    from src.evaluation.traditional_evaluator import TraditionalEvaluator

    evaluator = TraditionalEvaluator()
    ndcg = evaluator.ndcg_at_k(ground_truth, predictions, k=k)

    return ndcg


def compute_all_metrics(ground_truth: List[float],
                       predictions: List[float],
                       k_values: List[int] = [5, 10, 20]) -> Dict[str, float]:
    """
    Calcola tutte le metriche di valutazione.

    Args:
        ground_truth: Lista dei rating reali
        predictions: Lista delle predizioni
        k_values: Lista di valori K per metriche ranking

    Returns:
        Dict con tutte le metriche
    """
    print("🔄 Calcolo metriche di valutazione...")

    metrics = {
        'rmse': compute_rmse(ground_truth, predictions),
        'mae': compute_mae(ground_truth, predictions),
        'num_predictions': len(predictions)
    }

    # Aggiungi metriche ranking
    for k in k_values:
        metrics[f'precision@{k}'] = compute_precision_at_k(ground_truth, predictions, k=k)
        metrics[f'ndcg@{k}'] = compute_ndcg_at_k(ground_truth, predictions, k=k)

    print("✅ Calcolo metriche completato")

    return metrics


def print_metrics_summary(metrics: Dict[str, float]):
    """
    Stampa un riepilogo formattato delle metriche.

    Args:
        metrics: Dict con metriche
    """
    print("\n" + "="*70)
    print("METRICHE DI VALUTAZIONE - RIEPILOGO")
    print("="*70)

    print(f"\n📊 Rating Prediction:")
    print(f"   RMSE:   {metrics['rmse']:.4f}")
    print(f"   MAE:    {metrics['mae']:.4f}")
    print(f"   N° predizioni: {metrics['num_predictions']}")

    # Metriche ranking
    print(f"\n📈 Ranking Metrics:")
    for key, value in sorted(metrics.items()):
        if '@' in key:
            print(f"   {key:12s}: {value:.4f}")

    print("="*70 + "\n")


def evaluate_baseline_vs_kbrs(kbrs_predictions: List[float],
                             baseline_predictions: List[float],
                             ground_truth: List[float]) -> Dict:
    """
    Confronto tra baseline e KBRS.

    Args:
        kbrs_predictions: Predizioni KBRS
        baseline_predictions: Predizioni baseline
        ground_truth: Rating reali

    Returns:
        Dict con confronto
    """
    print("🔄 Confronto KBRS vs Baseline...")

    # Metriche KBRS
    kbrs_metrics = compute_all_metrics(ground_truth, kbrs_predictions)

    # Metriche Baseline
    baseline_metrics = compute_all_metrics(ground_truth, baseline_predictions)

    # Confronto
    comparison = {
        'KBRS': kbrs_metrics,
        'Baseline': baseline_metrics,
        'Improvement': {}
    }

    # Calcola miglioramenti
    for metric in kbrs_metrics.keys():
        if metric in baseline_metrics:
            improvement = ((baseline_metrics[metric] - kbrs_metrics[metric])
                          / baseline_metrics[metric] * 100) if baseline_metrics[metric] > 0 else 0
            comparison['Improvement'][metric] = improvement

    print("✅ Confronto completato")

    return comparison


def plot_metrics_comparison(comparison: Dict, save_path: str = None):
    """
    Crea un grafico di confronto metriche.

    Args:
        comparison: Dict con confronto tra modelli
        save_path: Percorso per salvare il grafico
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("⚠️ matplotlib non installato, skipping plot")
        return

    kbrs_metrics = comparison['KBRS']
    baseline_metrics = comparison['Baseline']

    # Metriche da plottare
    metric_names = ['rmse', 'mae', 'precision@10', 'ndcg@10']
    kbrs_values = [kbrs_metrics[m] for m in metric_names]
    baseline_values = [baseline_metrics[m] for m in metric_names]

    # Crea grafico
    x = np.arange(len(metric_names))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    bars1 = ax.bar(x - width/2, kbrs_values, width, label='KBRS', color='#2E86AB')
    bars2 = ax.bar(x + width/2, baseline_values, width, label='Baseline', color='#A23B72')

    ax.set_xlabel('Metriche')
    ax.set_ylabel('Valore')
    ax.set_title('Confronto KBRS vs Baseline')
    ax.set_xticks(x)
    ax.set_xticklabels([m.upper() for m in metric_names])
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Aggiungi valori sulle barre
    def add_value_labels(bars):
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.3f}',
                       xy=(bar.get_x() + bar.get_width() / 2, height),
                       xytext=(0, 3),
                       textcoords="offset points",
                       ha='center', va='bottom', fontsize=8)

    add_value_labels(bars1)
    add_value_labels(bars2)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150)
        print(f"✅ Grafico salvato in {save_path}")

    plt.show()
