"""
Comparison Package - Utility modules for comparing Traditional vs MAB approaches

ATTENZIONE: Questo package NON confronta RMSE con Cumulative Reward!
Le due metodologie sono PARALLELE e rispondono a domande diverse.

Moduli disponibili:
- comparative_analyzer: Confronto tra algoritmi (KBRS vs Popularity, LinUCB vs EpsilonGreedy)
- visualization: Genera visualizzazioni comparative
- config: Configurazioni centralizzate
"""

from .config import ComparisonConfig, PipelineConfig
from .comparative_analyzer import (
    KBRSVsPopularityComparator,
    MABAlgorithmComparator,
    DualEvaluationReporter
)
from .visualization import ComparativeVisualizer

__all__ = [
    'ComparisonConfig',
    'PipelineConfig',
    'KBRSVsPopularityComparator',
    'MABAlgorithmComparator',
    'DualEvaluationReporter',
    'ComparativeVisualizer'
]
