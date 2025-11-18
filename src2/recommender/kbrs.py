# src/recommender/kbrs.py
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from typing import Optional, Dict, Any

class KBRSEngine:
    """
    Motore KBRS agnostico.

    Non sa nulla di train/test/MAB.
    Riceve un DataFrame di film arricchiti e fornisce
    utilità per costruire profili utente e calcolare similarità.
    """

    def __init__(self, movies_df: pd.DataFrame):
        """
        Inizializza il motore KBRS.

        Args:
            movies_df: DataFrame dei film con feature già arricchite da DBpedia.
                       Deve avere colonne: movieId, title, e feature numeriche.
        """
        self.movies_df = movies_df.copy()

        # Prepara le feature per il calcolo di similarità
        # Exclude non-numeric columns
        self.feature_columns = [
            col for col in movies_df.columns
            if col not in ['movieId', 'title'] and
            pd.api.types.is_numeric_dtype(movies_df[col])
        ]

        self.movie_features = self.movies_df[self.feature_columns].values
        self.movie_id_to_idx = {mid: idx for idx, mid in enumerate(self.movies_df['movieId'])}

        print(f"KBRSEngine inizializzato con {len(self.movies_df)} film e {len(self.feature_columns)} feature")

    def build_user_profile(self, user_ratings_df: pd.DataFrame) -> np.ndarray:
        """
        Costruisce il profilo utente dai suoi rating passati.

        Args:
            user_ratings_df: DataFrame con colonne [userId, movieId, rating]
                             Deve contenere SOLO i rating PASSATI dell'utente
                             (per Pipeline 1: train_df; per Pipeline 2: cronologici precedenti)

        Returns:
            np.array: Vettore profilo utente (media pesata delle feature dei film che ha valutato)
        """
        if user_ratings_df.empty:
            # Utente nuovo: profilo nullo
            return np.zeros(len(self.feature_columns))

        # Merge con le feature dei film
        user_movies = user_ratings_df.merge(
            self.movies_df[['movieId'] + self.feature_columns],
            on='movieId',
            how='inner'
        )

        if user_movies.empty:
            return np.zeros(len(self.feature_columns))

        # Normalizza i rating (da 0-5 a 0-1)
        normalized_ratings = (user_movies['rating'] - 1) / 4.0

        # Calcola profilo come media pesata
        weights = normalized_ratings.values.reshape(-1, 1)
        features_matrix = user_movies[self.feature_columns].values

        # Media pesata
        weighted_features = features_matrix * weights
        user_profile = weighted_features.sum(axis=0) / (weights.sum() + 1e-10)

        return user_profile

    def get_item_features(self, movie_id: int) -> np.ndarray:
        """
        Ottiene il vettore di feature di un film.

        Args:
            movie_id: ID del film

        Returns:
            np.array: Vettore di feature del film
        """
        if movie_id not in self.movie_id_to_idx:
            return np.zeros(len(self.feature_columns))

        idx = self.movie_id_to_idx[movie_id]
        return self.movie_features[idx]

    def predict_score(self, user_profile_vec: np.ndarray, item_features_vec: np.ndarray) -> float:
        """
        Predice il punteggio per un utente e un film usando similarità coseno.

        Args:
            user_profile_vec: Vettore profilo utente
            item_features_vec: Vettore feature film

        Returns:
            float: Punteggio predetto (0-5, dove 0 è negativo, 5 è molto positivo)
        """
        # Calcola similarità coseno
        if np.linalg.norm(user_profile_vec) == 0 or np.linalg.norm(item_features_vec) == 0:
            return 2.5  # Rating neutro per profili/vettori nulli

        # Similarità normalizzata tra -1 e 1
        similarity = cosine_similarity(
            user_profile_vec.reshape(1, -1),
            item_features_vec.reshape(1, -1)
        )[0][0]

        # Mappa da [-1, 1] a [0, 5]
        # similarity = -1 -> rating = 1.0
        # similarity = 0 -> rating = 3.0
        # similarity = 1 -> rating = 5.0
        predicted_rating = 3.0 + similarity * 2.0

        # Clamp tra 0 e 5
        return np.clip(predicted_rating, 0, 5)

    def get_top_k_recommendations(
        self,
        user_profile_vec: np.ndarray,
        candidate_movies_df: pd.DataFrame,
        k: int = 10
    ) -> pd.DataFrame:
        """
        Ottiene i top-K film candidati per un utente.

        Args:
            user_profile_vec: Vettore profilo utente
            candidate_movies_df: DataFrame dei film candidati
            k: Numero di raccomandazioni

        Returns:
            DataFrame: Top-K film con punteggi predetti
        """
        results = []

        for _, movie in candidate_movies_df.iterrows():
            movie_id = movie['movieId']
            item_vec = self.get_item_features(movie_id)
            score = self.predict_score(user_profile_vec, item_vec)
            results.append({
                'movieId': movie_id,
                'predicted_rating': score,
                'title': movie.get('title', '')
            })

        results_df = pd.DataFrame(results)
        return results_df.nlargest(k, 'predicted_rating')
