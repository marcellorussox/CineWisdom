import numpy as np
import pandas as pd
from tqdm import tqdm
from src.mab_manager import MABManager
from src.kbrs import KBRS

class MABSimulator:
    def __init__(self, mab_manager: MABManager, ratings_df: pd.DataFrame, kbrs: KBRS, 
                 cosine_sim_matrix: np.ndarray, movie_ids_series: pd.Series,
                 cleaned_df: pd.DataFrame, popularity_n_recommendations: int):
        self.mab_manager = mab_manager
        self.kbrs = kbrs
        self.cosine_sim_matrix = cosine_sim_matrix
        self.movie_ids_series = movie_ids_series
        self.cleaned_df = cleaned_df
        self.popularity_n = popularity_n_recommendations
        self.ratings_df = ratings_df
        
        # Precompute user preferred movies (rating >= 4.0)
        self.user_preferred_movies = ratings_df[ratings_df['rating'] >= 4.0]\
            .groupby('userId')['movieId'].apply(set).to_dict()

    def _get_popularity_recommendations(self) -> list:
        """Get top-n popular movies based on rating counts"""
        top_movies = (
            self.ratings_df['movieId']
            .value_counts()
            .head(self.popularity_n)
            .index
            .tolist()
        )
        # Popularity baseline doesn't predict ratings; keep None for compatibility
        return [(movie_id, None) for movie_id in top_movies]

    def _get_recommendations_for_model(self, model_name: str, user_id: int) -> list:
        """Get recommendations for specified model"""
        if model_name == 'KBRS_Hybrid':
            return self.kbrs.recommend_movies_hybrid(
                user_id,
                self.ratings_df,
                self.cosine_sim_matrix,
                self.movie_ids_series,
                self.cleaned_df,
                num_recommendations=self.popularity_n,
            )
        elif model_name == 'Popularity_Baseline':
            return self._get_popularity_recommendations()
        else:
            raise ValueError(f"Unknown model: {model_name}")

    def _simulate_reward(self, user_id: int, recommended_movie_ids: set) -> int:
        """Simulate binary reward based on overlap with user's preferred movies (rating >= 4)."""
        preferred = self.user_preferred_movies.get(user_id, set())
        return 1 if preferred & recommended_movie_ids else 0

    def run_simulation(self, n_iterations: int) -> pd.DataFrame:
        """Run full simulation loop"""
        history = []
        user_ids = list(self.user_preferred_movies.keys())
        
        for i in tqdm(range(n_iterations)):
            # 1. Select random user
            user_id = np.random.choice(user_ids)
            
            # 2. Get MAB recommendation (arm selection)
            chosen_idx, model_name = self.mab_manager.get_recommendations()
            
            # 3. Generate recommendations
            recommendations = self._get_recommendations_for_model(model_name, user_id)
            rec_movie_ids = {movie_id for movie_id, _ in recommendations}
            
            # 4. Simulate reward
            reward = self._simulate_reward(user_id, rec_movie_ids)
            
            # 5. Register feedback to MAB
            self.mab_manager.register_feedback(chosen_idx, reward)
            
            # 6. Record history
            alphas = self.mab_manager.mab_instance.alphas.copy()
            betas = self.mab_manager.mab_instance.betas.copy()
            history.append({
                'iteration': i,
                'user_id': user_id,
                'chosen_arm_index': chosen_idx,
                'model_name': model_name,
                'reward': reward,
                'alphas': alphas,
                'betas': betas,
            })
            
        return pd.DataFrame(history)
