"""
Online MAB Simulator for CineWisdom.

This module simulates an online learning environment where a Multi-Armed Bandit
chooses between different recommendation strategies (Arms) to maximize reward.

In this simulation (Replay on Offline Data):
- The "Environment" is the stream of user interactions from the Online Dataset.
- The "Arms" are different predictors/recommenders:
    0. NCF Model (Personalized)
    1. Global Average Baseline (Safe fallback)
- The "Reward" is based on the accuracy of the prediction for the specific item
  the user actually interacted with.
"""

import numpy as np
import pandas as pd
import torch
from tqdm import tqdm
from typing import List, Dict, Any, Tuple

from src.bandit.policy import BanditPolicy
from src.models.ncf import NCF


class OnlineSimulator:
    """
    Simulates online learning using a Replay strategy on historical data.
    """

    def __init__(
        self,
        model: NCF,
        bandit_policy: BanditPolicy,
        online_df: pd.DataFrame,
        train_df: pd.DataFrame  # Needed for baseline stats
    ):
        """
        Initialize Simulator.

        Args:
            model: Pre-trained NCF model
            bandit_policy: Policy to select between arms
            online_df: DataFrame containing the stream of "future" interactions
            train_df: DataFrame used for training (to compute baselines)
        """
        self.model = model
        self.policy = bandit_policy
        self.online_df = online_df.copy()
        
        # Compute baselines from training data
        self.global_mean = train_df['rating'].mean()
        
        # Pre-compute user/movie means for a slightly better baseline than global mean
        # (Optional, but makes the baseline arm more competitive)
        self.user_means = train_df.groupby('userId')['rating'].mean().to_dict()
        self.movie_means = train_df.groupby('movieId')['rating'].mean().to_dict()
        
        # Arms definition
        self.arm_names = {
            0: 'NCF',
            1: 'Baseline'
        }
        
        # Statistics
        self.history = []
        self.cumulative_reward = 0.0

    def _get_baseline_prediction(self, user_id: int, movie_id: int) -> float:
        """Get prediction from baseline (User Mean + Movie Mean - Global Mean)."""
        u_mean = self.user_means.get(user_id, self.global_mean)
        m_mean = self.movie_means.get(movie_id, self.global_mean)
        return (u_mean + m_mean) / 2.0

    def _get_ncf_prediction(self, user_id: int, movie_id: int, feature_row: Any = None) -> float:
        """Get prediction from NCF model."""
        # Note: This assumes model is on CPU for single-item inference speed in loop
        # For batch processing, we would batch these.
        # Here we simulate one-by-one to mimic online stream.
        
        # If we had features, we would pass them. For now, assuming no features or handling inside model.
        # The NCF.predict method handles int inputs.
        try:
            pred = self.model.predict(user_id, movie_id)
            return float(pred[0])
        except Exception:
            return self.global_mean

    def _calculate_reward(self, prediction: float, actual: float) -> float:
        """
        Calculate reward based on accuracy.
        Reward = 1.0 - (Normalized Absolute Error)
        Clipped between 0 and 1.
        """
        error = abs(prediction - actual)
        # Normalize error: max error is usually 4.5 (5.0 - 0.5)
        # Let's say error < 0.5 is "perfect" (reward 1), error > 2.0 is "bad" (reward 0)
        
        # Simple linear reward:
        # Reward = 1 if error=0, Reward=0 if error=2.0
        reward = max(0.0, 1.0 - (error / 2.0))
        return reward

    def run(self, limit: int = None) -> pd.DataFrame:
        """
        Run the simulation.

        Args:
            limit: Max number of interactions to process

        Returns:
            DataFrame with simulation history
        """
        print(f"🚀 Starting Online Simulation with {len(self.online_df)} interactions...")
        
        interactions = self.online_df
        if limit:
            interactions = interactions.head(limit)
            
        # Ensure model is in eval mode and on CPU for simple loop
        self.model.eval()
        self.model.to('cpu')
        
        for idx, row in tqdm(interactions.iterrows(), total=len(interactions)):
            user_id = int(row['userId'])
            movie_id = int(row['movieId'])
            actual_rating = float(row['rating'])
            
            # 1. Bandit chooses arm
            arm = self.policy.select_arm()
            
            # 2. Make prediction based on arm
            if arm == 0:  # NCF
                prediction = self._get_ncf_prediction(user_id, movie_id)
            else:  # Baseline
                prediction = self._get_baseline_prediction(user_id, movie_id)
                
            # 3. Observe Reward (Accuracy)
            reward = self._calculate_reward(prediction, actual_rating)
            
            # 4. Update Bandit
            self.policy.update(arm, reward)
            
            # 5. Log
            self.cumulative_reward += reward
            self.history.append({
                'step': idx,
                'user_id': user_id,
                'movie_id': movie_id,
                'actual_rating': actual_rating,
                'arm': arm,
                'arm_name': self.arm_names[arm],
                'prediction': prediction,
                'reward': reward,
                'cumulative_reward': self.cumulative_reward
            })
            
        return pd.DataFrame(self.history)
