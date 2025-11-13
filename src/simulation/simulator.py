import numpy as np
import pandas as pd
from typing import Optional, Sequence, List, Dict, Set, Tuple
from tqdm.auto import tqdm
from src.bandit.mab_manager import MABManager
from src.recommender.kbrs import KBRS
from .reward_system import AdvancedRewardSystem, RewardConfig

class MABSimulator:
    """Advanced MAB Simulator with optimized reward system and calibration."""

    def __init__(
        self,
        mab_manager: MABManager,
        ratings_df: pd.DataFrame,
        kbrs: KBRS,
        cosine_sim_matrix: np.ndarray,
        movie_ids_series: pd.Series,
        cleaned_df: pd.DataFrame,
        popularity_n_recommendations: int,
        precompute_kbrs: bool = True,
        reward_config: Optional[RewardConfig] = None,
    ):
        """Initialize MAB simulator with reward system.

        Parameters
        ----------
        mab_manager : MABManager
            Manager with advanced Thompson Sampling
        ratings_df : pd.DataFrame
            User ratings data
        kbrs : KBRS
            Knowledge-based recommender
        cosine_sim_matrix : np.ndarray
            Movie similarity matrix
        movie_ids_series : pd.Series
            Movie IDs series
        cleaned_df : pd.DataFrame
            Cleaned movie metadata
        popularity_n_recommendations : int
            Number of recommendations to generate
        precompute_kbrs : bool
            Whether to precompute KBRS recommendations
        reward_config : Optional[RewardConfig]
            Configuration for reward system (defaults to 50/50 exploration/accuracy)
        """
        self.mab_manager = mab_manager
        self.kbrs = kbrs
        self.cosine_sim_matrix = cosine_sim_matrix
        self.movie_ids_series = movie_ids_series
        self.cleaned_df = cleaned_df
        self.popularity_n = popularity_n_recommendations
        self.ratings_df = ratings_df
        self.precompute_kbrs = precompute_kbrs

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

        # Precompute user preferred movies (rating >= 4.0)
        self.user_preferred_movies: Dict[int, Set[int]] = (
            ratings_df[ratings_df['rating'] >= 4.0]
            .groupby('userId')['movieId'].apply(set).to_dict()
        )

        # Precompute user seen movies (all rated items regardless of rating)
        # This is used to avoid rewarding recommendations of already seen movies
        self.user_seen_movies: Dict[int, Set[int]] = ratings_df.groupby('userId')['movieId'].apply(set).to_dict()

        # Initialize reward system (✅ FIXED: Unified, correct calibration)
        # Default: 50% exploration + 50% accuracy, update every 10 iterations
        default_reward_config = RewardConfig(
            weight_exploration=0.5,
            weight_accuracy=0.5,
            weight_novelty=0.0,  # No bias
            weight_serendipity=0.0,  # No bias
            use_ndcg=False,
            update_calibration_every=10,  # ✅ Faster calibration
        )
        self.reward_config = reward_config or default_reward_config
        self.reward_system = AdvancedRewardSystem(
            config=self.reward_config,
            user_seen_movies=self.user_seen_movies,
            popularity_top_ids=self.popularity_top_ids,
        )

        # KBRS per-user cache (filled in run_simulation based on eligible users)
        self._kbrs_cache: Dict[int, List[Tuple[int, Optional[float]]]] = {}

    def _get_popularity_recommendations(self) -> list:
        """Get top-n popular movies based on rating counts"""
        # Use cached list (computed in __init__) to avoid repeated computations
        return [(movie_id, None) for movie_id in self.popularity_top_ids]

    def _get_recommendations_for_model(self, model_name: str, user_id: int) -> list:
        """Get recommendations for specified model"""
        if model_name == 'KBRS_Hybrid':
            # Serve from cache if available
            cached = self._kbrs_cache.get(user_id)
            if cached is not None:
                return cached
            # Fallback to on-the-fly computation
            # 🚀 OPTIMIZED: Use top_k_similar parameter for faster computation
            return self.kbrs.recommend_movies_hybrid(
                user_id,
                self.ratings_df,
                self.cosine_sim_matrix,
                self.movie_ids_series,
                self.cleaned_df,
                num_recommendations=self.popularity_n,
                top_k_similar=100,  # 🚀 OPTIMIZED: Only consider top-100 similar movies
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
            # Popularity_Baseline arm: reward based on presence of popular unseen movies
            # This allows the MAB to learn that the baseline has value for users who haven't
            # seen popular movies yet
            for movie_id, _ in recommendations:
                if movie_id not in seen and movie_id in self.popularity_top_ids:
                    return 1
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

        # Optionally precompute KBRS recommendations for all eligible users once
        if self.precompute_kbrs:
            iterable = user_ids
            if use_tqdm:
                iterable = tqdm(user_ids, desc="Precomputing KBRS")
            for uid in iterable:
                # 🚀 OPTIMIZED: Compute and store top-N KBRS recommendations per user
                recs = self.kbrs.recommend_movies_hybrid(
                    uid,
                    self.ratings_df,
                    self.cosine_sim_matrix,
                    self.movie_ids_series,
                    self.cleaned_df,
                    num_recommendations=self.popularity_n,
                    top_k_similar=100,  # 🚀 OPTIMIZED: Only consider top-100 similar movies
                )
                self._kbrs_cache[uid] = recs

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

            # DEBUG: Mostra scelta MAB per primi 10
            if i < 10:
                mab_stats = self.mab_manager.get_statistics()
                print(f"  [MAB CHOICE #{i}] {model_name} (arm={chosen_idx}) - selections so far: KBRS={mab_stats['selections'][0]}, Baseline={mab_stats['selections'][1]}")

            # 3. Generate recommendations
            recommendations = self._get_recommendations_for_model(model_name, user_id)

            # 4. Compute advanced reward using reward system
            reward_metrics = self.reward_system.compute_reward(
                user_id, recommendations, model_name
            )

            # Extract binary reward for MAB update (use composite reward)
            reward = reward_metrics.composite_reward

            # 5. Register feedback to MAB (use round to get binary 0/1)
            # FIX: round(0.5) in Python va a 0 (banker's rounding), usiamo int() per >=0.5
            binary_reward = 1 if reward >= 0.5 else 0

            # DEBUG: Stampa reward per debug
            if i < 10:  # Solo primi 10 per non spam
                print(f"  [MAB FEEDBACK] {model_name}: reward={reward:.3f} -> binary={binary_reward}")

            self.mab_manager.register_feedback(chosen_idx, binary_reward)
            
            # 6. Record history with advanced metrics
            mab_stats = self.mab_manager.get_statistics()
            history.append({
                'iteration': i,
                'user_id': user_id,
                'chosen_arm_index': chosen_idx,
                'model_name': model_name,
                'reward': reward,
                # Advanced reward metrics
                'exploration_reward': reward_metrics.exploration_reward,
                'accuracy_proxy': reward_metrics.accuracy_proxy,
                'novelty_score': reward_metrics.novelty_score,
                'serendipity_score': reward_metrics.serendipity_score,
                # MAB statistics
                'alphas': mab_stats['alphas'],
                'betas': mab_stats['betas'],
                'means': mab_stats['means'],
                'success_rates': mab_stats['success_rates'],
                'temperature': mab_stats['temperature'],
                'exploration_rate': mab_stats['exploration_rate'],
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

        # Finalize MAB statistics
        self.mab_manager.finalize()

        # Output advanced simulation results
        if verbose:
            reward_metrics = self.reward_system.get_calibration_metrics()
            mab_stats = self.mab_manager.get_statistics()

            print("\n" + "=" * 70)
            print("ADVANCED MAB SIMULATION RESULTS")
            print("=" * 70)
            print(f"\nReward System Metrics:")
            print(f"  • R_G (General Accuracy): {reward_metrics['general_accuracy']:.4f}")
            print(f"  • KBRS calls: {reward_metrics['kbrs_calls']}")
            print(f"  • KBRS hits: {reward_metrics['kbrs_hits']}")

            print(f"\nMAB Statistics:")
            print(f"  • Temperature: {mab_stats['temperature']:.4f}")
            print(f"  • Exploration Rate: {mab_stats['exploration_rate']:.4f}")
            print(f"  • KBRS Mean Estimate: {mab_stats['means'][0]:.4f}")
            print(f"  • Baseline Mean Estimate: {mab_stats['means'][1]:.4f}")
            print("=" * 70)

        return pd.DataFrame(history)

    def get_kbrs_general_accuracy(self) -> float:
        """Get KBRS general accuracy from reward system."""
        return self.reward_system.general_accuracy

    def get_kbrs_calls(self) -> int:
        """Get number of KBRS calls from reward system."""
        return self.reward_system.kbrs_calls

    def get_kbrs_rg_hits(self) -> int:
        """Get number of KBRS R_G hits from reward system."""
        return self.reward_system.kbrs_hits

    def get_simulation_summary(self) -> dict:
        """Get comprehensive simulation summary."""
        reward_metrics = self.reward_system.get_calibration_metrics()
        mab_stats = self.mab_manager.get_statistics()

        return {
            'reward_metrics': reward_metrics,
            'mab_statistics': mab_stats,
            'config': {
                'reward_config': self.reward_config.__dict__,
                'mab_config': self.config.__dict__ if hasattr(self, 'config') else None,
            }
        }
