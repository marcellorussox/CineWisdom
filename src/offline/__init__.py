"""
Offline Module - Pipeline tradizionale per valutazione accuratezza KBRS

Questo modulo gestisce:
- Gestione split train/val/test
- Valutazione offline con metriche standard
- Pipeline completa per valutazione accuratezza
"""

from .split_manager_wrapper import *
from .offline_evaluator import *
from .offline_pipeline import *

__all__ = [
    'create_train_val_test_split',
    'save_splits',
    'OfflineEvaluator',
    'OfflinePipeline',
]
