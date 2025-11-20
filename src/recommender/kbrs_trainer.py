"""
KBRS Trainer - Offline Training and Evaluation for Knowledge-Based Recommender System.

This module provides functionality to train, validate, and test the KBRS model
using traditional train/validation/test split methodology.
"""

from __future__ import annotations

import os
import pickle
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.preprocessing import StandardScaler

from src.recommender.kbrs import KBRS


class KBRSTrainer:
    """
    Trainer for KBRS model with offline evaluation capabilities.

    This class handles:
    - Training KBRS on train set
    - Validation on validation set for hyperparameter tuning
    - Testing on test set for final evaluation
    - Metrics calculation (RMSE, MAE, Precision@K, NDCG@K)
    """

    def __init__(
        self,
        movie_catalog: pd.DataFrame,
        cosine_sim_matrix: np.ndarray,
        movie_ids: pd.Series
    ):
        """
        Initialize trainer with movie catalog and similarity matrix.

        Args:
            movie_catalog: DataFrame with movie metadata
            cosine_sim_matrix: Precomputed cosine similarity matrix
            movie_ids: Series with movie IDs matching similarity matrix indices
        """
        self.movie_catalog = movie_catalog
        self.cosine_sim_matrix = cosine_sim_matrix
        self.movie_ids = movie_ids.reset_index(drop=True)
        self.kbrs = KBRS(movie_catalog)
        self.user_profiles: Dict[int, set] = {}
        self.train_df: Optional[pd.DataFrame] = None

        # Scaler for normalizing predictions if needed
        self.scaler = StandardScaler()

    def _build_user_profiles(self, train_df: pd.DataFrame) -> None:
        """
        Build user profiles from training data.

        Args:
            train_df: DataFrame with columns ['userId', 'movieId', 'rating']
        """
        print("Building user profiles from training data...")

        # For each user, collect movies they rated >= 4.0 (positive feedback)
        for user_id in train_df['userId'].unique():
            user_ratings = train_df[train_df['userId'] == user_id]
            # Movies the user liked (rating >= 4.0)
            liked_movies = set(
                user_ratings[user_ratings['rating'] >= 4.0]['movieId'].values
            )
            self.user_profiles[user_id] = liked_movies

        print(f"Built profiles for {len(self.user_profiles)} users")

    def train(
        self,
        train_df: pd.DataFrame,
        k_similar: int = 500
    ) -> None:
        """
        Train KBRS model on training set.

        Args:
            train_df: DataFrame with columns ['userId', 'movieId', 'rating']
            k_similar: Number of similar movies to consider (default 500)
        """
        print(f"\n=== Training KBRS ===")
        print(f"k_similar: {k_similar}")
        print(f"Training set size: {len(train_df)}")

        # Save training data for predictions
        self.train_df = train_df

        # Set similarity matrix in KBRS
        self.kbrs.k_similar = k_similar

        # Build user profiles
        self._build_user_profiles(train_df)

        print("Training completed!")

    def predict(self, user_id: int, movie_id: int) -> float:
        """
        Predict rating for a user-movie pair.

        Args:
            user_id: User ID
            movie_id: Movie ID

        Returns:
            Predicted rating (1-5 scale)
        """
        if self.train_df is None:
            raise ValueError("Model must be trained before making predictions. Call train() first.")

        if user_id not in self.user_profiles:
            return 3.0  # Default prediction for unseen users

        # Get movies seen by this user from training data
        user_seen_movies = self.train_df[self.train_df['userId'] == user_id][['userId', 'movieId', 'rating']]

        # If user has no ratings in training set, return default
        if len(user_seen_movies) == 0:
            return 3.0

        # Get prediction from KBRS
        prediction = self.kbrs.predict_rating(
            user_id=user_id,
            movie_id=movie_id,
            ratings_df=user_seen_movies,
            cosine_sim_matrix=self.cosine_sim_matrix,
            movie_ids=self.movie_ids
        )

        return prediction

    def get_user_recommendations(
        self,
        user_id: int,
        ratings_df: pd.DataFrame,
        n_recommendations: int = 10
    ) -> List[Tuple[int, float]]:
        """
        Get top-N movie recommendations for a user.

        Args:
            user_id: User ID
            ratings_df: DataFrame with user ratings
            n_recommendations: Number of recommendations to return

        Returns:
            List of (movie_id, predicted_rating) tuples
        """
        print(f"\n[DEBUG] Getting recommendations for user {user_id}")
        print(f"  User in user_profiles? {user_id in self.user_profiles}")
        print(f"  Total users in profiles: {len(self.user_profiles)}")

        if user_id not in self.user_profiles:
            print(f"  WARNING: User {user_id} not in user_profiles!")
            return []

        # Get seen movies for this user
        seen_movies = set(ratings_df[ratings_df['userId'] == user_id]['movieId'].values)
        print(f"  User has {len(seen_movies)} seen movies")

        # Get recommendations from KBRS
        print(f"  Calling KBRS recommend_movies_hybrid...")
        recommendations = self.kbrs.recommend_movies_hybrid(
            user_id=user_id,
            ratings_df=ratings_df,
            cosine_sim_matrix=self.cosine_sim_matrix,
            movie_ids=self.movie_ids,
            cleaned_df=self.movie_catalog,
            top_k_similar=self.kbrs.k_similar
        )

        print(f"  KBRS returned {len(recommendations)} recommendations")

        # Filter out seen movies and take top N
        filtered_recs = [
            (movie_id, pred) for movie_id, pred in recommendations
            if movie_id not in seen_movies
        ][:n_recommendations]

        print(f"  After filtering seen movies: {len(filtered_recs)} recommendations")

        return filtered_recs

    def validate(
        self,
        val_df: pd.DataFrame
    ) -> Dict[str, float]:
        """
        Validate model on validation set.

        Args:
            val_df: DataFrame with columns ['userId', 'movieId', 'rating']

        Returns:
            Dictionary with validation metrics
        """
        print(f"\n=== Validation ===")
        print(f"Validation set size: {len(val_df)}")

        # DEBUG: Check if movies in validation are in compressed catalog
        compressed_movie_ids = set(self.movie_ids)
        val_movie_ids = set(val_df['movieId'].unique())
        missing_in_compressed = val_movie_ids - compressed_movie_ids

        print(f"\nDEBUG:")
        print(f"  Movies in compressed catalog: {len(compressed_movie_ids)}")
        print(f"  Movies in validation set: {len(val_movie_ids)}")
        print(f"  Movies missing from compressed: {len(missing_in_compressed)}")
        if len(missing_in_compressed) > 0:
            print(f"  Missing movie IDs (first 10): {sorted(list(missing_in_compressed))[:10]}")

        # Filter validation to only include movies that are in compressed catalog
        val_df_filtered = val_df[val_df['movieId'].isin(compressed_movie_ids)]
        print(f"  Validation set after filtering: {len(val_df_filtered)}")

        if len(val_df_filtered) == 0:
            print("ERROR: No movies in validation set are in compressed catalog!")
            return {
                'rmse': float('inf'),
                'mae': float('inf'),
                'num_predictions': 0
            }

        # Get unique users in validation
        val_users = val_df_filtered['userId'].unique()
        predictions = []
        actual_ratings = []

        # For each user in validation, get recommendations
        for user_id in val_users:
            user_data = val_df_filtered[val_df_filtered['userId'] == user_id]

            # Get predictions for movies in validation set
            for _, row in user_data.iterrows():
                pred = self.predict(user_id, row['movieId'])
                predictions.append(pred)
                actual_ratings.append(row['rating'])

        # Calculate metrics
        rmse = np.sqrt(mean_squared_error(actual_ratings, predictions))
        mae = mean_absolute_error(actual_ratings, predictions)

        metrics = {
            'rmse': rmse,
            'mae': mae,
            'num_predictions': len(predictions)
        }

        print(f"RMSE: {rmse:.4f}")
        print(f"MAE: {mae:.4f}")
        print(f"Predictions: {len(predictions)}")

        return metrics

    def test(
        self,
        test_df: pd.DataFrame
    ) -> Dict[str, float]:
        """
        Test model on test set.

        Args:
            test_df: DataFrame with columns ['userId', 'movieId', 'rating']

        Returns:
            Dictionary with test metrics
        """
        print(f"\n=== Test ===")
        print(f"Test set size: {len(test_df)}")

        # Filter test to only include movies that are in compressed catalog
        compressed_movie_ids = set(self.movie_ids)
        test_df_filtered = test_df[test_df['movieId'].isin(compressed_movie_ids)]
        print(f"Test set after filtering: {len(test_df_filtered)}")

        if len(test_df_filtered) == 0:
            print("ERROR: No movies in test set are in compressed catalog!")
            return {
                'rmse': float('inf'),
                'mae': float('inf'),
                'num_predictions': 0
            }

        # Get unique users in test
        test_users = test_df_filtered['userId'].unique()
        predictions = []
        actual_ratings = []

        # For each user in test, get recommendations
        for user_id in test_users:
            user_data = test_df_filtered[test_df_filtered['userId'] == user_id]

            # Get predictions for movies in test set
            for _, row in user_data.iterrows():
                pred = self.predict(user_id, row['movieId'])
                predictions.append(pred)
                actual_ratings.append(row['rating'])

        # Calculate metrics
        rmse = np.sqrt(mean_squared_error(actual_ratings, predictions))
        mae = mean_absolute_error(actual_ratings, predictions)

        metrics = {
            'rmse': rmse,
            'mae': mae,
            'num_predictions': len(predictions)
        }

        print(f"RMSE: {rmse:.4f}")
        print(f"MAE: {mae:.4f}")
        print(f"Predictions: {len(predictions)}")

        return metrics

    def save_model(self, filepath: str) -> None:
        """
        Save trained model to disk.

        Args:
            filepath: Path to save the model
        """
        model_data = {
            'user_profiles': self.user_profiles,
            'k_similar': self.kbrs.k_similar,
            'movie_catalog': self.movie_catalog,
            'cosine_sim_matrix': self.cosine_sim_matrix,
            'movie_ids': self.movie_ids
        }

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)

        print(f"Model saved to {filepath}")

    def load_model(self, filepath: str) -> None:
        """
        Load trained model from disk.

        Args:
            filepath: Path to the saved model
        """
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)

        self.user_profiles = model_data['user_profiles']
        self.kbrs.k_similar = model_data['k_similar']
        self.movie_catalog = model_data['movie_catalog']
        self.cosine_sim_matrix = model_data['cosine_sim_matrix']
        self.movie_ids = model_data['movie_ids']

        print(f"Model loaded from {filepath}")


def run_offline_evaluation(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    movie_catalog: pd.DataFrame,
    cosine_sim_matrix: np.ndarray,
    movie_ids: pd.Series,
    k_similar_values: List[int] = [100, 500, 1000]
) -> Dict[int, Dict[str, float]]:
    """
    Run offline evaluation with different k_similar values.

    Args:
        train_df: Training DataFrame
        val_df: Validation DataFrame
        test_df: Test DataFrame
        movie_catalog: Movie catalog DataFrame
        cosine_sim_matrix: Cosine similarity matrix
        movie_ids: Movie IDs Series
        k_similar_values: List of k_similar values to try

    Returns:
        Dictionary mapping k_similar to metrics
    """
    print("\n" + "="*70)
    print("OFFLINE EVALUATION: Finding optimal k_similar")
    print("="*70)

    results = {}

    for k in k_similar_values:
        print(f"\n--- Testing k_similar = {k} ---")

        # Train
        trainer = KBRSTrainer(movie_catalog, cosine_sim_matrix, movie_ids)
        trainer.train(train_df, k_similar=k)

        # Validate
        val_metrics = trainer.validate(val_df)

        # Test
        test_metrics = trainer.test(test_df)

        results[k] = {
            'validation': val_metrics,
            'test': test_metrics
        }

        print(f"Best k_similar so far: {k}")

    return results


def find_best_k_similar(results: Dict[int, Dict[str, float]]) -> int:
    """
    Find best k_similar based on validation RMSE.

    Args:
        results: Results from run_offline_evaluation

    Returns:
        Best k_similar value
    """
    best_k = None
    best_rmse = float('inf')

    for k, metrics in results.items():
        val_rmse = metrics['validation']['rmse']
        if val_rmse < best_rmse:
            best_rmse = val_rmse
            best_k = k

    return best_k
