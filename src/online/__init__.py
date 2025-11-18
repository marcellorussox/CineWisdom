"""
Online Module - Pipeline online per valutazione efficacia MAB

Questo modulo gestisce:
- Algoritmi MAB (LinUCB, Thompson Sampling)
- Simulazione online con replay evaluation
- Pipeline completa per valutazione efficacia MAB
"""

from .linucb import *
from .online_simulator import *
from .online_pipeline import *

__all__ = [
    'LinUCB',
    'OnlineSimulator',
    'OnlinePipeline',
]
