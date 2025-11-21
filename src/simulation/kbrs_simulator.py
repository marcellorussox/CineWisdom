"""
Online KBRS Simulator with Multi-Armed Bandit

This module implements online learning for KBRS using Thompson Sampling
to choose between Exploration and Exploitation strategies dynamically.

The MAB learns which strategy works best for each user based on real-time feedback.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple
from tqdm import tqdm


class ThompsonSamplingKBRS:
    """
    Thompson Sampling for KBRS strategy selection.
    
    Arms:
        0: Director
        1: Cast
        2: Genre
        3: Exploration
    """
    
    def __init__(self, n_arms: int = 2):
        """
        Initialize Thompson Sampling with Beta distributions.
        
        Args:
            n_arms: Number of arms (2 for exploration/exploitation)
        """
        self.n_arms = n_arms
        # Beta distribution parameters: (successes + 1, failures + 1)
        self.alpha = np.ones(n_arms)  # Successes
        self.beta = np.ones(n_arms)   # Failures
        
        # Statistics
        self.pulls = np.zeros(n_arms)
        self.rewards = np.zeros(n_arms)
    
    def select_arm(self) -> int:
        """
        Select an arm using Thompson Sampling.
        
        Returns:
            Selected arm index (0 or 1)
        """
        # Sample from Beta distribution for each arm
        theta_samples = [
            np.random.beta(self.alpha[i], self.beta[i])
            for i in range(self.n_arms)
        ]
        
        # Select arm with highest sample
        return int(np.argmax(theta_samples))
    
    def update(self, arm: int, reward: float):
        """
        Update Beta distribution based on reward.
        
        Args:
            arm: Selected arm
            reward: Reward received (0-1 range, 1 = best)
        """
        self.pulls[arm] += 1
        self.rewards[arm] += reward
        
        # Update Beta parameters
        # Treat reward as probability of success
        self.alpha[arm] += reward
        self.beta[arm] += (1 - reward)
    
    def get_statistics(self) -> Dict:
        """Get current statistics."""
        return {
            'pulls': self.pulls.copy(),
            'total_rewards': self.rewards.copy(),
            'avg_rewards': self.rewards / np.maximum(self.pulls, 1),
            'alpha': self.alpha.copy(),
            'beta': self.beta.copy(),
            'expected_values': self.alpha / (self.alpha + self.beta)
        }


class OnlineKBRSSimulator:
    """
    Simulate online learning for KBRS with MAB strategy selection.
    
    The simulator:
    1. Uses offline-trained KBRS with cosine similarity matrix
    2. For each online user interaction:
       - MAB selects strategy (exploration or exploitation)
       - KBRS recommends movie using selected strategy
       - Observes actual rating
       - Computes reward (1 - normalized error)
       - Updates MAB
    3. Learns which strategy works best over time
    """
    
    def __init__(
        self,
        kbrs_model,
        cosine_sim_matrix: np.ndarray,
        movie_ids: pd.Series,
        movies_catalog: pd.DataFrame,
        rating_scale: Tuple[float, float] = (0.5, 5.0)
    ):
        """
        Initialize online simulator.
        
        Args:
            kbrs_model: Trained KBRS instance
            cosine_sim_matrix: Precomputed cosine similarity matrix
            movie_ids: Movie IDs corresponding to similarity matrix
            movies_catalog: Full movie catalog
            rating_scale: (min, max) rating scale for normalization
        """
        self.kbrs = kbrs_model
        self.cosine_sim_matrix = cosine_sim_matrix
        self.movie_ids = movie_ids
        self.movies_catalog = movies_catalog
        self.rating_scale = rating_scale
        
        # Initialize MAB with 4 arms for Semantic Strategies
        # 0: Director (Semantic Exploitation)
        # 1: Cast (Semantic Exploitation)
        # 2: Genre (Baseline Exploitation)
        # 3: Exploration (Diversity)
        self.mab = ThompsonSamplingKBRS(n_arms=4)
        
        # Strategy names
        self.strategies = ['director', 'cast', 'genre', 'exploration']
        
        # History
        self.history = []
    
    def _normalize_error(self, error: float) -> float:
        """
        Normalize error to 0-1 range for reward.
        
        Args:
            error: Absolute error in rating prediction
        
        Returns:
            Normalized error (0 = perfect, 1 = worst possible)
        """
        max_error = self.rating_scale[1] - self.rating_scale[0]
        return min(error / max_error, 1.0)
    
    def _compute_reward(self, predicted: float, actual: float) -> float:
        """
        Compute reward from prediction.
        
        Args:
            predicted: Predicted rating
            actual: Actual rating
        
        Returns:
            Reward in [0, 1] (1 = best, 0 = worst)
        """
        error = abs(predicted - actual)
        normalized_error = self._normalize_error(error)
        reward = 1.0 - normalized_error
        return reward
    
    def simulate_online(
        self,
        online_df: pd.DataFrame,
        offline_ratings_df: pd.DataFrame,
        n_recommendations: int = 10,
        verbose: bool = True
    ) -> Dict:
        """
        Run online simulation with MAB strategy selection.
        
        Args:
            online_df: Online interactions (user, movie, rating, timestamp)
            offline_ratings_df: Historical ratings for KBRS predictions
            n_recommendations: Number of recommendations per user
            verbose: Show progress bar
        
        Returns:
            Dictionary with simulation results
        """
        # Sort by timestamp for realistic simulation
        online_df = online_df.sort_values('timestamp').reset_index(drop=True)
        
        # Combine offline + accumulated online for predictions
        current_ratings = offline_ratings_df.copy()
        
        # Simulation loop
        iterator = tqdm(online_df.iterrows(), total=len(online_df), desc="Online Simulation") if verbose else online_df.iterrows()
        
        for idx, interaction in iterator:
            user_id = interaction['userId']
            true_movie_id = interaction['movieId']
            true_rating = interaction['rating']
            
            # 1. MAB selects strategy
            arm = self.mab.select_arm()
            strategy = self.strategies[arm]
            
            # 2. KBRS recommends using selected strategy
            try:
                recommendations = self.kbrs.recommend_movies_hybrid(
                    user_id=user_id,
                    ratings_df=current_ratings,
                    cosine_sim_matrix=self.cosine_sim_matrix,
                    movie_ids=self.movie_ids,
                    cleaned_df=self.movies_catalog,
                    num_recommendations=n_recommendations,
                    strategy=strategy
                )
                
                # Check if true movie is in recommendations
                recommended_ids = [rec[0] for rec in recommendations]
                
                # Predict rating for the actual movie
                predicted_rating = self.kbrs.predict_rating(
                    user_id=user_id,
                    movie_id=true_movie_id,
                    ratings_df=current_ratings,
                    cosine_sim_matrix=self.cosine_sim_matrix,
                    movie_ids=self.movie_ids
                )
                
                # If prediction fails, use fallback
                if predicted_rating is None:
                    # Fallback: use mean rating
                    user_ratings = current_ratings[current_ratings['userId'] == user_id]
                    if len(user_ratings) > 0:
                        predicted_rating = user_ratings['rating'].mean()
                    else:
                        predicted_rating = 3.0  # Global fallback
                
                # 3. Compute reward
                reward = self._compute_reward(predicted_rating, true_rating)
                
                # 4. Update MAB
                self.mab.update(arm, reward)
                
                # Record interaction
                self.history.append({
                    'step': idx,
                    'user_id': user_id,
                    'movie_id': true_movie_id,
                    'true_rating': true_rating,
                    'predicted_rating': predicted_rating,
                    'strategy': strategy,
                    'arm': arm,
                    'reward': reward,
                    'in_recommendations': true_movie_id in recommended_ids,
                    'n_recommendations': len(recommendations)
                })
                
            except Exception as e:
                # Handle edge cases gracefully
                print(f"❌ Error at step {idx}: {e}")
                import traceback
                traceback.print_exc()
                continue
            
            # 5. Add interaction to history (for next predictions)
            new_rating = pd.DataFrame([{
                'userId': user_id,
                'movieId': true_movie_id,
                'rating': true_rating,
                'timestamp': interaction['timestamp']
            }])
            current_ratings = pd.concat([current_ratings, new_rating], ignore_index=True)
        
        # Compute summary statistics
        history_df = pd.DataFrame(self.history)
        
        if history_df.empty:
            print("❌ CRITICAL: Simulation history is empty! All iterations failed.")
            return {
                'history': pd.DataFrame(columns=['step', 'user_id', 'movie_id', 'true_rating', 'predicted_rating', 'strategy', 'arm', 'reward', 'in_recommendations', 'n_recommendations']),
                'mab_stats': self.mab.get_statistics(),
                'summary': {
                    'total_interactions': 0,
                    'mean_reward': 0.0,
                    'final_rmse': 0.0,
                    'final_mae': 0.0,
                    'exploration_rate': 0.0,
                    'exploitation_rate': 0.0,
                }
            }

        results = {
            'history': history_df,
            'mab_stats': self.mab.get_statistics(),
            'summary': {
                'total_interactions': len(history_df),
                'mean_reward': history_df['reward'].mean(),
                'final_rmse': np.sqrt(((history_df['predicted_rating'] - history_df['true_rating']) ** 2).mean()),
                'final_mae': (history_df['predicted_rating'] - history_df['true_rating']).abs().mean(),
                'exploration_rate': (history_df['arm'] == 0).mean(),
                'exploitation_rate': (history_df['arm'] == 1).mean(),
            }
        }
        
        return results
