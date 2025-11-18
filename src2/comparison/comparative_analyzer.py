# src/comparison/comparative_analyzer.py
"""
Comparative Analyzer - Confronto tra metodologie diverse.

ATTENZIONE: Non confronta RMSE con Cumulative Reward!
Ogni metodologia risponde a domande diverse.

Questo modulo contiene funzioni per confrontare:
1. KBRS vs Baseline (usando metriche Traditional)
2. LinUCB vs EpsilonGreedy (usando metriche MAB)
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, List, Any

# Import per confronti Traditional
from src.evaluation.traditional_evaluator import TraditionalEvaluator

# Import per confronti MAB
# (Da implementare se necessario)


class KBRSVsPopularityComparator:
    """
    Confronto tra KBRS e baseline Popularity.
    USATO DA traditional_pipeline.py per valutare se KBRS è migliore di una baseline semplice.
    """

    def __init__(self):
        self.evaluator = TraditionalEvaluator()

    def compare(self, kbrs_predictions: List[float], popularity_predictions: List[float],
                ground_truth: List[float]) -> Dict[str, Any]:
        """
        Confronto RMSE e MAE tra KBRS e Popularity.

        Args:
            kbrs_predictions: Predizioni KBRS
            popularity_predictions: Predizioni baseline Popularity
            ground_truth: Rating reali

        Returns:
            Dict con confronto dei risultati
        """
        # Calcola metriche per KBRS
        kbrs_rmse = self.evaluator.rmse(ground_truth, kbrs_predictions)
        kbrs_mae = self.evaluator.mae(ground_truth, kbrs_predictions)

        # Calcola metriche per Popularity
        pop_rmse = self.evaluator.rmse(ground_truth, popularity_predictions)
        pop_mae = self.evaluator.mae(ground_truth, popularity_predictions)

        # Confronto
        comparison = {
            'KBRS': {
                'RMSE': kbrs_rmse,
                'MAE': kbrs_mae
            },
            'Popularity_Baseline': {
                'RMSE': pop_rmse,
                'MAE': pop_mae
            },
            'Winner': 'KBRS' if kbrs_rmse < pop_rmse else 'Popularity_Baseline',
            'Improvement': {
                'RMSE': ((pop_rmse - kbrs_rmse) / pop_rmse * 100) if pop_rmse > 0 else 0,
                'MAE': ((pop_mae - kbrs_mae) / pop_mae * 100) if pop_mae > 0 else 0
            }
        }

        return comparison


class MABAlgorithmComparator:
    """
    Confronto tra diversi algoritmi MAB (LinUCB, EpsilonGreedy, etc.).
    USATO DA mab_experiment.py per valutare quale algoritmo MAB impara meglio.
    """

    def __init__(self):
        pass

    def compare_algorithms(self, mab_results_dict: Dict[str, Dict]) -> pd.DataFrame:
        """
        Confronto tra risultati di diversi algoritmi MAB.

        Args:
            mab_results_dict: Dict con structure:
                {
                    'LinUCB': {'cumulative_reward': [...], 'final_ctr': 0.45},
                    'EpsilonGreedy': {'cumulative_reward': [...], 'final_ctr': 0.40},
                    ...
                }

        Returns:
            DataFrame con confronto
        """
        comparison_results = []

        for algorithm_name, results in mab_results_dict.items():
            comparison_results.append({
                'Algorithm': algorithm_name,
                'Final_Cumulative_Reward': results['final_cumulative_reward'],
                'Final_CTR': results['final_ctr'],
                'Learning_Speed': self._calculate_learning_speed(results['cumulative_reward'])
            })

        comparison_df = pd.DataFrame(comparison_results)
        comparison_df = comparison_df.sort_values('Final_Cumulative_Reward', ascending=False)

        return comparison_df

    def _calculate_learning_speed(self, cumulative_rewards: List[float], window: int = 100) -> float:
        """
        Calcola la velocità di apprendimento (slope del cumulative reward).

        Args:
            cumulative_rewards: Lista dei cumulative rewards nel tempo
            window: Finestra per calcolare la slope

        Returns:
            Slope media (reward per step)
        """
        if len(cumulative_rewards) < window:
            return cumulative_rewards[-1] / len(cumulative_rewards) if cumulative_rewards else 0

        # Calcola slope media nelle ultime 'window' osservazioni
        slopes = []
        for i in range(window, len(cumulative_rewards)):
            slope = (cumulative_rewards[i] - cumulative_rewards[i-window]) / window
            slopes.append(slope)

        return np.mean(slopes) if slopes else 0


class DualEvaluationReporter:
    """
    Reporter per documentare che le due metodologie sono PARALLELE e INDEPENDENTI.
    NON confronta RMSE con Cumulative Reward.
    """

    def __init__(self, output_dir: str = 'results/comparison'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def create_comparison_report(self, traditional_results: Dict, mab_results: Dict) -> Dict:
        """
        Crea un report che spiega che le due metodologie sono diverse e complementari.

        Args:
            traditional_results: Risultati valutazione offline (RMSE, MAE, etc.)
            mab_results: Risultati valutazione online (Cumulative Reward, CTR, etc.)

        Returns:
            Dict con report
        """
        report = {
            'methodologies': {
                'Traditional_Offline': {
                    'objective': 'Misurare accuratezza predittiva del KBRS',
                    'question': 'Quanto è accurato il KBRS nel predire rating a freddo?',
                    'approach': 'Split train/val/test statico',
                    'metrics': ['RMSE', 'MAE', 'Precision@K', 'NDCG@K'],
                    'results': {
                        'RMSE': traditional_results.get('rmse', 'N/A'),
                        'MAE': traditional_results.get('mae', 'N/A'),
                        'num_predictions': traditional_results.get('num_predictions', 'N/A')
                    },
                    'interpretation': self._interpret_traditional_results(traditional_results)
                },
                'MAB_Online': {
                    'objective': 'Misurare efficacia della strategia MAB',
                    'question': 'Quanto è efficace il MAB nell\'imparare nel tempo?',
                    'approach': 'Replay sequenziale ordinato per timestamp',
                    'metrics': ['Cumulative Reward', 'CTR', 'Learning Curves'],
                    'results': {
                        'Cumulative_Reward': mab_results.get('final_cumulative_reward', 'N/A'),
                        'CTR': mab_results.get('final_ctr', 'N/A'),
                        'total_events': mab_results.get('total_events', 'N/A')
                    },
                    'interpretation': self._interpret_mab_results(mab_results)
                }
            },
            'conclusion': {
                'dual_evaluation': True,
                'parallel_not_sequential': True,
                'complementary': True,
                'message': (
                    "Le due metodologie rispondono a domande DIVERSE e COMPLEMENTARI. "
                    "Non confrontano RMSE con Cumulative Reward. "
                    "Traditional misura 'accuratezza', MAB misura 'adattività'."
                )
            }
        }

        # Salva report
        with open(self.output_dir / 'comparison_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)

        return report

    def _interpret_traditional_results(self, results: Dict) -> str:
        """Interpreta i risultati della valutazione offline."""
        rmse = results.get('rmse', 5.0)
        if rmse < 0.8:
            return "KBRS ha ottima accuratezza (RMSE basso)"
        elif rmse < 1.2:
            return "KBRS ha buona accuratezza"
        elif rmse < 1.5:
            return "KBRS ha accuracy moderata"
        else:
            return "KBRS ha accuracy scarsa (potrebbero servire miglioramenti)"

    def _interpret_mab_results(self, results: Dict) -> str:
        """Interpreta i risultati della valutazione online."""
        ctr = results.get('final_ctr', 0)
        if ctr > 0.5:
            return "MAB ha alta efficacia (CTR elevato)"
        elif ctr > 0.3:
            return "MAB ha buona efficacia"
        elif ctr > 0.15:
            return "MAB ha efficacia moderata"
        else:
            return "MAB ha efficacia bassa (impara lentamente)"
