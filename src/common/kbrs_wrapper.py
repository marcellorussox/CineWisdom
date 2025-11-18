"""
KBRS Wrapper - Wrapper user-friendly per KBRSEngine

Fornisce un'interfaccia semplificata per utilizzare KBRSEngine
nei notebook e nelle pipeline.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import warnings


class KBRSWrapper:
    """
    Wrapper per KBRSEngine con interfaccia semplificata.

    Gestisce automaticamente:
    - Costruzione profili utente
    - Predizione rating
    - Generazione raccomandazioni
    """

    def __init__(self, movies_df: pd.DataFrame):
        """
        Inizializza il wrapper KBRS.

        Args:
            movies_df: DataFrame con film arricchiti (movieId + feature numeriche)
        """
        from src.recommender.kbrs import KBRSEngine

        print("🔄 Inizializzazione KBRS Engine...")
        self.kbrs_engine = KBRSEngine(movies_df)
        self.n_features = len(self.kbrs_engine.feature_columns)

        print(f"✅ KBRS inizializzato:")
        print(f"   - Film: {len(movies_df)}")
        print(f"   - Feature: {self.n_features}")

    def build_user_profile(self, user_ratings: pd.DataFrame) -> np.ndarray:
        """
        Costruisce il profilo utente dai suoi rating.

        Args:
            user_ratings: DataFrame con [userId, movieId, rating]

        Returns:
            Vettore profilo utente (shape: n_features,)
        """
        if user_ratings.empty:
            return np.zeros(self.n_features)

        profile = self.kbrs_engine.build_user_profile(user_ratings)
        return profile

    def predict_rating(self, user_profile: np.ndarray, movie_id: int) -> float:
        """
        Predice il rating per un utente e un film.

        Args:
            user_profile: Vettore profilo utente
            movie_id: ID del film

        Returns:
            Rating predetto (0-5)
        """
        movie_features = self.kbrs_engine.get_item_features(movie_id)
        predicted_rating = self.kbrs_engine.predict_score(user_profile, movie_features)
        return predicted_rating

    def predict_ratings_batch(self, user_profile: np.ndarray,
                             movie_ids: List[int]) -> Dict[int, float]:
        """
        Predice rating per una lista di film.

        Args:
            user_profile: Vettore profilo utente
            movie_ids: Lista di ID film

        Returns:
            Dict {movie_id: predicted_rating}
        """
        predictions = {}

        print(f"🔄 Predizione batch per {len(movie_ids)} film...")

        for movie_id in movie_ids:
            predictions[movie_id] = self.predict_rating(user_profile, movie_id)

        print(f"✅ Predizioni completate")

        return predictions

    def get_recommendations(self, user_ratings: pd.DataFrame,
                           candidate_movies_df: pd.DataFrame,
                           top_k: int = 10) -> pd.DataFrame:
        """
        Genera top-K raccomandazioni per un utente.

        Args:
            user_ratings: DataFrame rating utente
            candidate_movies_df: DataFrame film candidati
            top_k: Numero di raccomandazioni

        Returns:
            DataFrame con top-K raccomandazioni (movieId, predicted_rating, title)
        """
        # Costruisci profilo
        user_profile = self.build_user_profile(user_ratings)

        # Genera raccomandazioni
        recommendations = self.kbrs_engine.get_top_k_recommendations(
            user_profile_vec=user_profile,
            candidate_movies_df=candidate_movies_df,
            k=top_k
        )

        print(f"✅ Generate {len(recommendations)} raccomandazioni")

        return recommendations

    def compute_user_similarity(self, user_profile_1: np.ndarray,
                               user_profile_2: np.ndarray) -> float:
        """
        Calcola similarità tra due profili utente.

        Args:
            user_profile_1: Primo profilo
            user_profile_2: Secondo profilo

        Returns:
            Similarità coseno
        """
        from sklearn.metrics.pairwise import cosine_similarity

        if np.linalg.norm(user_profile_1) == 0 or np.linalg.norm(user_profile_2) == 0:
            return 0.0

        similarity = cosine_similarity(
            user_profile_1.reshape(1, -1),
            user_profile_2.reshape(1, -1)
        )[0][0]

        return float(similarity)

    def get_feature_importance(self, user_profile: np.ndarray) -> pd.DataFrame:
        """
        Analizza l'importanza delle feature nel profilo.

        Args:
            user_profile: Vettore profilo utente

        Returns:
            DataFrame con feature e importanza
        """
        feature_names = self.kbrs_engine.feature_columns
        importance = np.abs(user_profile)

        feature_df = pd.DataFrame({
            'feature': feature_names,
            'importance': importance
        }).sort_values('importance', ascending=False)

        return feature_df

    def get_user_stats(self, user_ratings: pd.DataFrame) -> Dict:
        """
        Calcola statistiche dell'utente.

        Args:
            user_ratings: DataFrame rating utente

        Returns:
            Dict con statistiche
        """
        if user_ratings.empty:
            return {
                'num_ratings': 0,
                'avg_rating': 0.0,
                'min_rating': 0.0,
                'max_rating': 0.0,
                'std_rating': 0.0
            }

        stats = {
            'num_ratings': len(user_ratings),
            'avg_rating': user_ratings['rating'].mean(),
            'min_rating': user_ratings['rating'].min(),
            'max_rating': user_ratings['rating'].max(),
            'std_rating': user_ratings['rating'].std()
        }

        return stats
