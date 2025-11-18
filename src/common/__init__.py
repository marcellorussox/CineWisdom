"""
Common Module - Funzioni condivise per CineWisdom

Questo modulo contiene:
- Data loading e preprocessing
- KBRS wrapper
- Metriche di valutazione
- Utilità comuni
"""

from .data_loader import *
from .kbrs_wrapper import *
from .metrics import *
from .utils import *

__all__ = [
    # Data loader
    'load_movielens_data',
    'enrich_with_dbpedia',
    'normalize_catalog',
    'compress_features',
    'create_user_item_matrix',
    'join_dataframes',
    'drop_columns',
    'clean_partial_rows',

    # KBRS wrapper
    'KBRSWrapper',

    # Metrics
    'compute_rmse',
    'compute_mae',
    'compute_precision_at_k',
    'compute_ndcg_at_k',

    # Utils
    'filter_users_by_rating_count',
    'get_timestamp_sorted_ratings',
    'compute_popularity_baseline',
    'compute_global_statistics',
    'get_user_history',
    'create_data_splits',
    'save_splits',
    'get_split_statistics',
    'print_split_statistics',
    'save_results',
    'load_results',
    'validate_data',
]
