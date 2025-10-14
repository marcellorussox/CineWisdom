import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class KBRS:
    """
    Knowledge-Based Recommender System (KBRS) implementation with hybrid recommendation capabilities.

    This class combines content-based and collaborative filtering approaches to provide
    movie recommendations based on:
    - Movie attributes and content similarity
    - User rating patterns

    Attributes:
        unique_movie_catalog (pd.DataFrame): DataFrame containing movie metadata
    """

    def __init__(self, unique_movie_catalog: pd.DataFrame) -> None:
        """
        Initialize the KBRS recommender with movie catalog data.

        Args:
            unique_movie_catalog: DataFrame containing at minimum columns:
                - movieId: Unique movie identifier
                - title: Movie title
                Additional metadata columns can be included
        """
        self.unique_movie_catalog = unique_movie_catalog

    def create_user_profile(self, user_id, ratings_df):
        """
        Crea un profilo utente basato sui rating forniti dall'utente.

        Args:
            user_id (int): L'ID dell'utente.
            ratings_df (pd.DataFrame): DataFrame con i dati di rating.

        Returns:
            pd.DataFrame: DataFrame con i film visti dall'utente e i loro rating.
        """
        user_ratings = ratings_df[ratings_df['userId'] == user_id].copy()
        user_profile = pd.merge(user_ratings, self.unique_movie_catalog[['movieId', 'title']], on='movieId')
        return user_profile

    @staticmethod
    def predict_rating(user_id, movie_id, ratings_df, cosine_sim_matrix, movie_ids):
        """
        Prevede il rating di un utente per un film non ancora visto,
        basato sulla similarità del coseno e i rating dell'utente.

        Args:
            user_id (int): L'ID dell'utente.
            movie_id (int): L'ID del film per cui prevedere il rating.
            ratings_df (pd.DataFrame): DataFrame con i dati di rating.
            cosine_sim_matrix (np.ndarray): La matrice di similarità del coseno.
            movie_ids (pd.Series): Una Series contenente gli ID dei film nell'ordine della matrice di similarità.

        Returns:
            float: Il rating previsto per il film, o None se non ci sono film visti con similarità.
        """
        user_ratings = ratings_df[ratings_df['userId'] == user_id]

        # Filtra i film che l'utente ha visto
        seen_movies = user_ratings['movieId'].tolist()

        # Ottieni l'indice del film per cui si vuole prevedere il rating
        try:
            target_movie_index = movie_ids[movie_ids == movie_id].index[0]
        except IndexError:
            print(f"Film con ID {movie_id} non trovato nel catalogo compresso.")
            return None

        # Inizializza variabili per il calcolo del rating previsto
        weighted_sum = 0
        similarity_sum = 0

        # Itera sui film visti dall'utente
        for seen_movie_id, rating in user_ratings[['movieId', 'rating']].values:
            try:
                seen_movie_index = movie_ids[movie_ids == seen_movie_id].index[0]
            except IndexError:
                # Salta i film visti che non sono nel catalogo compresso (dovrebbe essere raro)
                continue

            # Ottieni la similarità tra il film target e il film visto
            similarity = cosine_sim_matrix[target_movie_index, seen_movie_index]

            # Pesa la similarità con il rating dell'utente
            weighted_sum += similarity * rating
            similarity_sum += abs(similarity)  # Usa il valore assoluto per evitare similarità negative

        # Calcola il rating previsto
        if similarity_sum > 0:
            predicted_rating = weighted_sum / similarity_sum
            return predicted_rating
        else:
            # Nessun film visto dall'utente ha similarità con il film target
            return None

    @staticmethod
    def recommend_movies_hybrid(user_id, ratings_df, cosine_sim_matrix, movie_ids, cleaned_df,
                                          num_recommendations=10):
        """
        Genera raccomandazioni per un utente utilizzando un approccio ibrido ottimizzato.

        Args:
            user_id (int): L'ID dell'utente.
            ratings_df (pd.DataFrame): DataFrame con i dati di rating.
            cosine_sim_matrix (np.ndarray): La matrice di similarità del coseno.
            movie_ids (pd.Series): Una Series contenente gli ID dei film nell'ordine della matrice di similarità.
            cleaned_df (pd.DataFrame): DataFrame con il catalogo unico dei film.
            num_recommendations (int): Il numero di raccomandazioni da generare.

        Returns:
            list: Una lista di tuple (movie_id, predicted_rating) dei film raccomandati.
        """
        user_ratings = ratings_df[ratings_df['userId'] == user_id]
        seen_movies_ids = user_ratings['movieId'].tolist()

        # Initialize dictionary to store aggregated scores for unseen movies
        aggregated_scores = {}
        similarity_sums = {}  # To normalize the scores later

        # Get all movie IDs from the cleaned catalog
        all_movie_ids = cleaned_df['movieId'].tolist()

        # Iterate over the movies the user has rated
        for index, row in user_ratings.iterrows():
            seen_movie_id = row['movieId']
            rating = row['rating']

            try:
                # Get the index of the seen movie in the similarity matrix
                seen_movie_index = movie_ids[movie_ids == seen_movie_id].index[0]
            except IndexError:
                # Skip if the seen movie is not in the compressed catalog
                continue

            # Get similarity scores for the seen movie with all other movies
            similarity_scores = cosine_sim_matrix[seen_movie_index]

            # Iterate through all movies to aggregate scores
            for i, movie_id in enumerate(movie_ids):
                # Skip movies the user has already seen
                if movie_id in seen_movies_ids:
                    continue

                similarity = similarity_scores[i]

                # Aggregate the weighted score
                if movie_id not in aggregated_scores:
                    aggregated_scores[movie_id] = 0
                    similarity_sums[movie_id] = 0

                aggregated_scores[movie_id] += similarity * rating
                similarity_sums[movie_id] += abs(similarity)  # Use absolute similarity for normalization

        # Normalize the aggregated scores to get predicted ratings
        predicted_ratings = []
        for movie_id, score_sum in aggregated_scores.items():
            if similarity_sums[movie_id] > 0:
                predicted_rating = score_sum / similarity_sums[movie_id]
                predicted_ratings.append((movie_id, predicted_rating))

        # Sort movies by predicted rating in descending order
        predicted_ratings = sorted(predicted_ratings, key=lambda x: x[1], reverse=True)

        # Return the top recommendations
        return predicted_ratings[:num_recommendations]
