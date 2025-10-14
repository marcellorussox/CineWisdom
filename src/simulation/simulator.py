import numpy as np
import pandas as pd
from typing import Optional, Sequence, List, Dict, Set, Tuple
from tqdm import tqdm
from src.bandit.mab_manager import MABManager
from src.recommender.kbrs import KBRS

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
        self.user_preferred_movies: Dict[int, Set[int]] = (
            ratings_df[ratings_df['rating'] >= 4.0]
            .groupby('userId')['movieId'].apply(set).to_dict()
        )

        # Precompute user seen movies (all rated items regardless of rating)
        # This is used to avoid rewarding recommendations of already seen movies
        self.user_seen_movies: Dict[int, Set[int]] = ratings_df.groupby('userId')['movieId'].apply(set).to_dict()

        # Cache numpy views and popularity baseline for speed
        self.movie_ids_np = movie_ids_series.to_numpy(copy=False)
        # Precompute global popularity top-N once
        self.popularity_top_ids: List[int] = (
            self.ratings_df['movieId']
            .value_counts()
            .head(self.popularity_n)
            .index
            .tolist()
        )

    def _get_popularity_recommendations(self) -> list:
        """Get top-n popular movies based on rating counts"""
        # Use cached list (computed in __init__) to avoid repeated computations
        return [(movie_id, None) for movie_id in self.popularity_top_ids]
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

    def _simulate_reward(self, user_id: int, recommendations: List[Tuple[int, Optional[float]]]) -> int:
        """
        Simulate binary reward guided by predicted ratings.

        New policy:
        - If recommendations include predicted ratings (KBRS_Hybrid):
          reward = 1 if there exists at least one UNSEEN item with predicted_rating >= 4.0.
        - If recommendations do NOT include predicted ratings (Popularity_Baseline):
          reward = 0 (non-personalized baseline does not earn positive reward).

        Args:
            user_id: The user for whom the recommendation was generated.
            recommendations: List of (movieId, predicted_rating) where predicted_rating may be None (for baseline).
        Returns:
            1 for success (KBRS predicts a high-value unseen item), else 0.
        """
        seen = self.user_seen_movies.get(user_id, set())

        # Detect if this is the KBRS arm by checking for any non-None predicted rating
        has_predictions = any((pred is not None) for _, pred in recommendations)

        if not has_predictions:
            # Popularity_Baseline arm: always 0 to serve as comparison
            return 0

        for movie_id, pred in recommendations:
            if movie_id in seen:
                continue
            if pred is not None and pred >= 4.0:
                return 1
        return 0

    def run_simulation(
        self,
        n_iterations: int,
        *,
        eligible_users: Optional[Sequence[int]] = None,
        min_positives_per_user: int = 1,
        use_tqdm: bool = True,
        eval_every: int = 0,
        early_stop_patience: int = 0,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Run full simulation loop with optional speed-oriented controls.

        Args
        - n_iterations: total iterations to simulate.
        - eligible_users: optional list of users to sample from.
        - min_positives_per_user: require at least this many positives for eligibility.
        - use_tqdm: show progress bar if True.
        - eval_every: if >0, compute moving average reward every eval_every steps.
        - early_stop_patience: if >0, stop when moving average doesn't improve for this many checks.
        - verbose: print basic logs when early stopping checks run.
        """
        history: List[Dict] = []

        # Build eligible pool once
        if eligible_users is None:
            user_ids = [u for u, pos in self.user_preferred_movies.items() if len(pos) >= min_positives_per_user]
        else:
            user_ids = [int(u) for u in eligible_users if len(self.user_preferred_movies.get(int(u), set())) >= min_positives_per_user]

        if not user_ids:
            raise ValueError("No eligible users found for simulation. Adjust filters.")

        iterator = range(n_iterations)
        if use_tqdm:
            iterator = tqdm(iterator)

        # Early stopping trackers
        best_ma = -1.0
        no_improve = 0
        rewards_window: List[int] = []

        for i in iterator:
            # 1. Select random user
            user_id = np.random.choice(user_ids)
            
            # 2. Get MAB recommendation (arm selection)
            chosen_idx, model_name = self.mab_manager.get_recommendations()
            
            # 3. Generate recommendations
            recommendations = self._get_recommendations_for_model(model_name, user_id)
            
            # 4. Simulate reward
            reward = self._simulate_reward(user_id, recommendations)
            
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

            # 7. Early stopping check
            if eval_every and (i + 1) % eval_every == 0:
                rewards_window.append(reward)
                # Compute moving average on recent eval window
                window = history[-eval_every:]
                ma = float(np.mean([h['reward'] for h in window])) if window else 0.0
                if verbose:
                    print(f"Iter {i+1}: moving avg reward over last {eval_every} = {ma:.4f}")
                if ma > best_ma + 1e-9:
                    best_ma = ma
                    no_improve = 0
                else:
                    no_improve += 1
                    if early_stop_patience and no_improve >= early_stop_patience:
                        if verbose:
                            print(f"Early stopping at iter {i+1}: no improvement for {no_improve} checks.")
                        break
            
        return pd.DataFrame(history)
