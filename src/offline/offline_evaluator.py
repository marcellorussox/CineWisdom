"""
Offline Evaluator - Valutazione offline KBRS

Classe per valutare la accuratezza del KBRS usando metriche tradizionali.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from pathlib import Path


class OfflineEvaluator:
    """
    Evaluator per pipeline offline.

    Gestisce:
    - Predizione rating su test set
    - Calcolo metriche di accuratezza
    - Confronto con baseline
    """

    def __init__(self):
        """Inizializza l'evaluator."""
        print("🔄 Inizializzazione Offline Evaluator...")

        # Componenti
        from src.common.kbrs_wrapper import KBRSWrapper
        from src.common.metrics import compute_all_metrics
        from src.common.metrics import compute_global_statistics

        self.kbrs_wrapper_class = KBRSWrapper
        self.compute_metrics_func = compute_all_metrics
        self.compute_stats_func = compute_global_statistics

        print("✅ Offline Evaluator inizializzato")

    def predict_all_ratings(self, kbrs_wrapper, test_df: pd.DataFrame,
                           train_df: pd.DataFrame) -> Tuple[List[float], List[float]]:
        """
        Predice tutti i rating del test set.

        Args:
            kbrs_wrapper: KBRSWrapper inizializzato
            test_df: DataFrame test
            train_df: DataFrame train

        Returns:
            Tuple (predictions, ground_truth)
        """
        print("🔄 Predizione rating su test set...")

        predictions = []
        ground_truth = []

        test_users = test_df['userId'].unique()
        total_predictions = len(test_df)

        print(f"   Utenti da valutare: {len(test_users)}")
        print(f"   Predizioni totali: {total_predictions}")

        for user_id in test_users:
            # Storia utente (solo training)
            user_history = train_df[train_df['userId'] == user_id]

            # Costruisci profilo
            user_profile = kbrs_wrapper.build_user_profile(user_history)

            # Film valutati nel test
            user_test_items = test_df[test_df['userId'] == user_id]

            for _, row in user_test_items.iterrows():
                movie_id = row['movieId']
                true_rating = row['rating']

                # Predici
                predicted_rating = kbrs_wrapper.predict_rating(user_profile, movie_id)

                predictions.append(predicted_rating)
                ground_truth.append(true_rating)

            # Progress
            if len(predictions) % 1000 == 0:
                progress = len(predictions) / total_predictions * 100
                print(f"   Progresso: {progress:.1f}% ({len(predictions)}/{total_predictions})")

        print(f"✅ Predizioni completate: {len(predictions)}")

        return predictions, ground_truth

    def evaluate_model(self, predictions: List[float],
                      ground_truth: List[float],
                      model_name: str = "KBRS") -> Dict:
        """
        Valuta il modello calcolando tutte le metriche.

        Args:
            predictions: Lista predizioni
            ground_truth: Lista rating reali
            model_name: Nome del modello

        Returns:
            Dict con metriche di valutazione
        """
        print(f"🔄 Valutazione modello: {model_name}...")

        # Calcola metriche
        metrics = self.compute_metrics_func(ground_truth, predictions)

        # Aggiungi metadati
        metrics['model_name'] = model_name
        metrics['evaluation_type'] = 'offline'
        metrics['timestamp'] = pd.Timestamp.now().isoformat()

        print(f"✅ Valutazione completata")

        return metrics

    def compare_with_baseline(self, predictions: List[float],
                             baseline_predictions: List[float],
                             ground_truth: List[float],
                             model_name: str = "KBRS",
                             baseline_name: str = "Popularity") -> Dict:
        """
        Confronta KBRS con una baseline.

        Args:
            predictions: Predizioni KBRS
            baseline_predictions: Predizioni baseline
            ground_truth: Rating reali
            model_name: Nome KBRS
            baseline_name: Nome baseline

        Returns:
            Dict con confronto dettagliato
        """
        print(f"🔄 Confronto {model_name} vs {baseline_name}...")

        # Valuta KBRS
        kbrs_metrics = self.evaluate_model(predictions, ground_truth, model_name)

        # Valuta baseline
        baseline_metrics = self.evaluate_model(baseline_predictions, ground_truth, baseline_name)

        # Calcola miglioramenti
        improvements = {}
        for metric in kbrs_metrics.keys():
            if metric in baseline_metrics and metric != 'rmse':  # Per RMSE, migliore è più basso
                improvement = ((baseline_metrics[metric] - kbrs_metrics[metric])
                              / baseline_metrics[metric] * 100)
                improvements[f'{metric}_improvement'] = improvement
            elif metric == 'rmse':
                improvement = ((baseline_metrics[metric] - kbrs_metrics[metric])
                              / baseline_metrics[metric] * 100)
                improvements[f'{metric}_improvement'] = improvement

        comparison = {
            'model': kbrs_metrics,
            'baseline': baseline_metrics,
            'improvements': improvements,
            'winner': model_name if improvements.get('rmse_improvement', 0) > 0 else baseline_name
        }

        print(f"✅ Confronto completato - Vincitore: {comparison['winner']}")

        return comparison

    def generate_report(self, results: Dict, output_dir: str = "results/offline"):
        """
        Genera report completo dei risultati.

        Args:
            results: Dict con risultati valutazione
            output_dir: Directory di output
        """
        from src.common.utils import save_results

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Salva JSON
        save_results(results, output_path / "evaluation_results.json")

        # Salva report testo
        report_path = output_path / "evaluation_report.txt"
        with open(report_path, 'w') as f:
            f.write("="*80 + "\n")
            f.write("OFFLINE EVALUATION REPORT\n")
            f.write("="*80 + "\n\n")

            f.write(f"Model: {results.get('model_name', 'N/A')}\n")
            f.write(f"Type: {results.get('evaluation_type', 'N/A')}\n")
            f.write(f"Timestamp: {results.get('timestamp', 'N/A')}\n\n")

            # Metriche
            f.write("METRICHE:\n")
            f.write("-"*80 + "\n")
            for key, value in results.items():
                if key not in ['model_name', 'evaluation_type', 'timestamp']:
                    f.write(f"{key:30s}: {value}\n")

        print(f"✅ Report salvato in {output_path}")

    def plot_results(self, comparison: Dict, save_path: str = "results/offline/metrics_comparison.png"):
        """
        Crea visualizzazioni dei risultati.

        Args:
            comparison: Dict con confronto
            save_path: Percorso per salvare il grafico
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            print("⚠️ matplotlib non installato, skipping plot")
            return

        # Estrai metriche
        model_metrics = comparison['model']
        baseline_metrics = comparison['baseline']

        metric_names = ['rmse', 'mae', 'precision@10', 'ndcg@10']
        model_values = [model_metrics[m] for m in metric_names]
        baseline_values = [baseline_metrics[m] for m in metric_names]

        # Crea grafico
        x = np.arange(len(metric_names))
        width = 0.35

        fig, ax = plt.subplots(figsize=(12, 6))
        bars1 = ax.bar(x - width/2, model_values, width,
                      label=comparison['model'].get('model_name', 'KBRS'),
                      color='#2E86AB', alpha=0.8)
        bars2 = ax.bar(x + width/2, baseline_values, width,
                      label=comparison['baseline'].get('model_name', 'Baseline'),
                      color='#A23B72', alpha=0.8)

        ax.set_xlabel('Metriche')
        ax.set_ylabel('Valore')
        ax.set_title('Confronto Offline Evaluation: KBRS vs Baseline')
        ax.set_xticks(x)
        ax.set_xticklabels([m.upper() for m in metric_names])
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Aggiungi valori
        def add_value_labels(bars):
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f'{height:.3f}',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3),
                           textcoords="offset points",
                           ha='center', va='bottom', fontsize=9)

        add_value_labels(bars1)
        add_value_labels(bars2)

        plt.tight_layout()
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.show()

        print(f"✅ Grafico salvato in {save_path}")
