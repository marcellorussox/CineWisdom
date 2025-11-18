"""
Traditional Evaluator - Metriche per Recommender Systems

Implementa le metriche di valutazione standard:
- RMSE (Root Mean Square Error)
- MAE (Mean Absolute Error)
- Precision@K
- NDCG (Normalized Discounted Cumulative Gain)
- Recall@K
- MAP (Mean Average Precision)
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from sklearn.metrics import mean_squared_error, mean_absolute_error
import math


class TraditionalEvaluator:
    """Evaluator per sistemi di raccomandazione tradizionali."""

    def __init__(self, k_values: List[int] = [5, 10, 20]):
        """
        Inizializza l'evaluator.

        Args:
            k_values: Lista di valori K per Precision@K, Recall@K, NDCG
        """
        self.k_values = k_values

    def calculate_rmse(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """
        Calcola RMSE (Root Mean Square Error).

        Args:
            y_true: Rating reali
            y_pred: Rating predetti

        Returns:
            RMSE value
        """
        return np.sqrt(mean_squared_error(y_true, y_pred))

    def calculate_mae(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """
        Calcola MAE (Mean Absolute Error).

        Args:
            y_true: Rating reali
            y_pred: Rating predetti

        Returns:
            MAE value
        """
        return mean_absolute_error(y_true, y_pred)

    def get_user_relevant_items(self, user_ratings: pd.DataFrame, threshold: float = 3.5) -> set:
        """
        Estrae gli item rilevanti per un utente.

        Args:
            user_ratings: Ratings di un utente
            threshold: Soglia per considerare un item rilevante

        Returns:
            Set di movieId rilevanti
        """
        relevant = user_ratings[user_ratings['rating'] >= threshold]['movieId'].unique()
        return set(relevant)

    def calculate_precision_at_k(self, recommended_items: List[int], relevant_items: set, k: int) -> float:
        """
        Calcola Precision@K.

        Args:
            recommended_items: Lista di item raccomandati (ordinati per rilevanza)
            relevant_items: Set di item rilevanti
            k: Numero di raccomandazioni da considerare

        Returns:
            Precision@K value
        """
        if k == 0 or len(recommended_items) == 0:
            return 0.0

        top_k_recommendations = recommended_items[:k]
        relevant_in_top_k = len([item for item in top_k_recommendations if item in relevant_items])

        return relevant_in_top_k / k

    def calculate_recall_at_k(self, recommended_items: List[int], relevant_items: set, k: int) -> float:
        """
        Calcola Recall@K.

        Args:
            recommended_items: Lista di item raccomandati (ordinati per rilevanza)
            relevant_items: Set di item rilevanti
            k: Numero di raccomandazioni da considerare

        Returns:
            Recall@K value
        """
        if len(relevant_items) == 0 or len(recommended_items) == 0:
            return 0.0

        top_k_recommendations = recommended_items[:k]
        relevant_in_top_k = len([item for item in top_k_recommendations if item in relevant_items])

        return relevant_in_top_k / len(relevant_items)

    def calculate_dcg_at_k(self, recommendations: List[int], relevance_scores: Dict[int, float], k: int) -> float:
        """
        Calcola DCG@K (Discounted Cumulative Gain).

        Args:
            recommendations: Lista di item raccomandati
            relevance_scores: Dizionario movieId -> relevance score
            k: Numero di posizioni da considerare

        Returns:
            DCG@K value
        """
        dcg = 0.0
        for i, item in enumerate(recommendations[:k]):
            if item in relevance_scores:
                relevance = relevance_scores[item]
                dcg += (2**relevance - 1) / math.log2(i + 2)
        return dcg

    def calculate_ndcg_at_k(self, recommendations: List[int], relevance_scores: Dict[int, float], k: int) -> float:
        """
        Calcola NDCG@K (Normalized Discounted Cumulative Gain).

        Args:
            recommendations: Lista di item raccomandati (ordinati per rilevanza)
            relevance_scores: Dizionario movieId -> relevance score
            k: Numero di posizioni da considerare

        Returns:
            NDCG@K value
        """
        dcg = self.calculate_dcg_at_k(recommendations, relevance_scores, k)

        # Calcola IDCG (ideal DCG) - ranking perfetto
        ideal_relevance = sorted(relevance_scores.values(), reverse=True)[:k]
        idcg = sum((2**rel - 1) / math.log2(i + 2) for i, rel in enumerate(ideal_relevance))

        if idcg == 0:
            return 0.0

        return dcg / idcg

    def calculate_ap(self, recommended_items: List[int], relevant_items: set) -> float:
        """
        Calcola Average Precision per un utente.

        Args:
            recommended_items: Lista di item raccomandati
            relevant_items: Set di item rilevanti

        Returns:
            Average Precision value
        """
        if len(relevant_items) == 0 or len(recommended_items) == 0:
            return 0.0

        precision_sum = 0.0
        relevant_found = 0

        for i, item in enumerate(recommended_items):
            if item in relevant_items:
                relevant_found += 1
                precision_at_i = relevant_found / (i + 1)
                precision_sum += precision_at_i

        return precision_sum / len(relevant_items)

    def evaluate_ratings(self, y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
        """
        Valuta le predizioni numeriche (RMSE, MAE).

        Args:
            y_true: Rating reali
            y_pred: Rating predetti

        Returns:
            Dict con metriche di rating
        """
        results = {
            "rmse": self.calculate_rmse(y_true, y_pred),
            "mae": self.calculate_mae(y_true, y_pred)
        }
        return results

    def evaluate_rankings(
        self,
        recommendations: Dict[int, List[int]],
        ground_truth: Dict[int, List[int]],
        ground_truth_ratings: pd.DataFrame,
        threshold: float = 3.5
    ) -> Dict[str, float]:
        """
        Valuta i ranking per tutti gli utenti.

        Args:
            recommendations: Dict userId -> list di movieId raccomandati
            ground_truth: Dict userId -> list di movieId nel test set
            ground_truth_ratings: DataFrame con ratings di test
            threshold: Soglia per item rilevanti

        Returns:
            Dict con tutte le metriche di ranking
        """
        print("\n" + "="*60)
        print("EVALUATING RANKINGS")
        print("="*60)

        results = {}
        n_users = len(recommendations)

        print(f"Evaluating {n_users} users...")

        # Inizializza accumulatori per ogni K
        precision_accumulator = {k: [] for k in self.k_values}
        recall_accumulator = {k: [] for k in self.k_values}
        ndcg_accumulator = {k: [] for k in self.k_values}
        ap_accumulator = []

        for user_id in recommendations:
            if user_id not in ground_truth:
                continue

            # Get relevant items for this user
            user_ratings = ground_truth_ratings[
                ground_truth_ratings['userId'] == user_id
            ]
            relevant_items = self.get_user_relevant_items(user_ratings, threshold)

            if len(relevant_items) == 0:
                continue

            # Recommended items for this user
            rec_items = recommendations.get(user_id, [])

            # Calculate relevance scores (use actual ratings, normalized)
            relevance_scores = {}
            user_ratings_dict = dict(zip(user_ratings['movieId'], user_ratings['rating']))
            for movie_id, rating in user_ratings_dict.items():
                # Normalize rating to 0-1 scale
                relevance_scores[movie_id] = (rating - 1) / 4  # ratings are 1-5

            # Calculate metrics for each K
            for k in self.k_values:
                precision_k = self.calculate_precision_at_k(rec_items, relevant_items, k)
                recall_k = self.calculate_recall_at_k(rec_items, relevant_items, k)
                ndcg_k = self.calculate_ndcg_at_k(rec_items, relevance_scores, k)

                precision_accumulator[k].append(precision_k)
                recall_accumulator[k].append(recall_k)
                ndcg_accumulator[k].append(ndcg_k)

            # Calculate Average Precision
            ap = self.calculate_ap(rec_items, relevant_items)
            ap_accumulator.append(ap)

        # Calcola medie
        for k in self.k_values:
            results[f"precision@{k}"] = np.mean(precision_accumulator[k]) if precision_accumulator[k] else 0.0
            results[f"recall@{k}"] = np.mean(recall_accumulator[k]) if recall_accumulator[k] else 0.0
            results[f"ndcg@{k}"] = np.mean(ndcg_accumulator[k]) if ndcg_accumulator[k] else 0.0

        results["map"] = np.mean(ap_accumulator) if ap_accumulator else 0.0

        return results

    def evaluate_model(
        self,
        predictions: pd.DataFrame,
        test_data: pd.DataFrame,
        recommended_items: Dict[int, List[int]],
        model_name: str = "Model"
    ) -> Dict:
        """
        Valutazione completa del modello.

        Args:
            predictions: DataFrame con colonne ['userId', 'movieId', 'rating', 'prediction']
            test_data: DataFrame con dati di test
            recommended_items: Dict userId -> movieId raccomandati
            model_name: Nome del modello

        Returns:
            Dict completo con tutte le metriche
        """
        print("\n" + "="*70)
        print(f"EVALUATING MODEL: {model_name}")
        print("="*70)

        results = {"model_name": model_name}

        # Rating-based metrics (RMSE, MAE)
        rating_metrics = self.evaluate_ratings(
            predictions['rating'].values,
            predictions['prediction'].values
        )
        results.update(rating_metrics)

        print(f"\nRating Metrics:")
        print(f"  RMSE: {rating_metrics['rmse']:.4f}")
        print(f"  MAE:  {rating_metrics['mae']:.4f}")

        # Ranking-based metrics
        # Prepara ground truth per ranking
        test_by_user = test_data.groupby('userId')['movieId'].apply(list).to_dict()
        test_ratings_by_user = {}

        for user_id, group in test_data.groupby('userId'):
            test_ratings_by_user[user_id] = group

        ranking_metrics = self.evaluate_rankings(
            recommendations=recommended_items,
            ground_truth=test_by_user,
            ground_truth_ratings=test_data,
            threshold=3.5
        )

        results.update(ranking_metrics)

        print(f"\nRanking Metrics:")
        for k in self.k_values:
            print(f"  Precision@{k}: {ranking_metrics[f'precision@{k}']:.4f}")
            print(f"  Recall@{k}:    {ranking_metrics[f'recall@{k}']:.4f}")
            print(f"  NDCG@{k}:      {ranking_metrics[f'ndcg@{k}']:.4f}")
        print(f"  MAP:            {ranking_metrics['map']:.4f}")

        return results

    def print_results(self, results: Dict):
        """
        Stampa i risultati in formato tabellare.

        Args:
            results: Risultati dell'evaluazione
        """
        print("\n" + "="*70)
        print(f"EVALUATION RESULTS - {results.get('model_name', 'Model')}")
        print("="*70)

        # Rating metrics
        if 'rmse' in results and 'mae' in results:
            print("\n[ RATING PREDICTION ]")
            print(f"  RMSE: {results['rmse']:.4f}")
            print(f"  MAE:  {results['mae']:.4f}")

        # Ranking metrics
        print("\n[ RANKING ]")
        for k in self.k_values:
            if f'precision@{k}' in results:
                print(f"  Precision@{k:2d}: {results[f'precision@{k}']:.4f}")
        print()
        for k in self.k_values:
            if f'recall@{k}' in results:
                print(f"  Recall@{k:2d}:    {results[f'recall@{k}']:.4f}")
        print()
        for k in self.k_values:
            if f'ndcg@{k}' in results:
                print(f"  NDCG@{k:2d}:      {results[f'ndcg@{k}']:.4f}")

        if 'map' in results:
            print(f"\n  MAP:            {results['map']:.4f}")

        print("="*70)


def evaluate_traditional_model(predictions, test_data, recommended_items, model_name="KBRS"):
    """
    Funzione di convenienza per valutare un modello tradizionale.

    Args:
        predictions: DataFrame con ['userId', 'movieId', 'rating', 'prediction']
        test_data: DataFrame con dati di test
        recommended_items: Dict userId -> movieId raccomandati
        model_name: Nome del modello

    Returns:
        Dict con risultati dell'evaluazione
    """
    evaluator = TraditionalEvaluator(k_values=[5, 10, 20])
    results = evaluator.evaluate_model(
        predictions=predictions,
        test_data=test_data,
        recommended_items=recommended_items,
        model_name=model_name
    )
    return results


if __name__ == "__main__":
    # Test con dati di esempio
    print("Traditional Evaluator - Test Mode")

    # Mock data
    np.random.seed(42)
    n_ratings = 1000

    test_predictions = pd.DataFrame({
        'userId': np.random.randint(1, 101, n_ratings),
        'movieId': np.random.randint(1, 1001, n_ratings),
        'rating': np.random.uniform(1, 5, n_ratings),
        'prediction': np.random.uniform(1, 5, n_ratings)
    })

    test_data = test_predictions[['userId', 'movieId', 'rating']].copy()

    # Mock recommendations
    recommended_items = {
        user_id: list(np.random.choice(range(1, 1001), 20, replace=False))
        for user_id in range(1, 21)
    }

    evaluator = TraditionalEvaluator()
    results = evaluator.evaluate_model(
        predictions=test_predictions,
        test_data=test_data,
        recommended_items=recommended_items,
        model_name="TestModel"
    )

    evaluator.print_results(results)
