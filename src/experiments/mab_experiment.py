"""
MAB Experiment API - High-level API for running MAB experiments.

Questo modulo fornisce un'API di alto livello per eseguire simulazioni MAB
senza dover gestire manualmente simulator, manager, etc.

Esempio d'uso:
```python
from src.experiments.mab_experiment import MABExperiment

# Configurazione esperimento
config = {
    'n_iterations': 300,
    'min_positives_per_user': 3,
    'top_k_similar': 100,  # Parametro KBRS
    'precompute_kbrs': True,
}

# Esegui esperimento
experiment = MABExperiment(data, config)
results = experiment.run()

# Confronta configurazioni
configs = [
    {'name': 'Fast', 'top_k_similar': 100},
    {'name': 'Balanced', 'top_k_similar': 500},
    {'name': 'Accurate', 'top_k_similar': 8000},
]
comparison = experiment.compare_configurations(configs)
```
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from src.bandit.mab_manager import MABManager
from src.recommender.kbrs import KBRS
from src.simulation.simulator import MABSimulator
from src.simulation.reward_system import RewardConfig


@dataclass
class ExperimentConfig:
    """Configuration for MAB experiment."""
    n_iterations: int = 300
    min_positives_per_user: int = 3
    top_k_similar: int = 100
    precompute_kbrs: bool = True
    popularity_n_recommendations: int = 10
    verbose: bool = True
    use_tqdm: bool = True

    # Reward configuration
    exploration_threshold: float = 4.0
    weight_exploration: float = 0.5
    weight_accuracy: float = 0.5
    update_calibration_every: int = 10


class MABExperiment:
    """
    High-level API for running MAB experiments.

    Gestisce automaticamente:
    - Inizializzazione sistema
    - Esecuzione simulazioni
    - Raccolta risultati
    - Confronto configurazioni
    """

    def __init__(
        self,
        data: Dict[str, pd.DataFrame],
        config: Optional[ExperimentConfig] = None,
    ):
        """
        Initialize MAB experiment.

        Args:
            data: Dictionary con keys:
                - 'ratings_df': DataFrame ratings
                - 'movies_df': DataFrame movies
                - 'links_df': DataFrame links
                - 'cleaned_df': DataFrame film puliti
                - 'cosine_sim_matrix': Matrice similarità
                - 'movie_ids_series': Series movie IDs
                - 'unique_movie_catalog': Catalogo film con metadata
            config: ExperimentConfig (se None, usa default)
        """
        self.data = data
        self.config = config or ExperimentConfig()

        # Initialize components
        self._initialize_components()

    def _initialize_components(self) -> None:
        """Initialize KBRS, MAB Manager, and Simulator."""
        # Initialize KBRS
        self.kbrs = self.data['unique_movie_catalog']

        # Initialize MAB Manager
        self.mab_manager = MABManager(
            recommender_models=["KBRS_Hybrid", "Popularity_Baseline"]
        )

        # Initialize Simulator
        self.simulator = MABSimulator(
            mab_manager=self.mab_manager,
            ratings_df=self.data['ratings_df'],
            kbrs=self.kbrs,
            cosine_sim_matrix=self.data['cosine_sim_matrix'],
            movie_ids_series=self.data['movie_ids_series'],
            cleaned_df=self.data['cleaned_df'],
            popularity_n_recommendations=self.config.popularity_n_recommendations,
            precompute_kbrs=self.config.precompute_kbrs,
        )

    def run(self) -> Dict[str, Any]:
        """
        Run single MAB experiment with current configuration.

        Returns:
            Dictionary with keys:
                - 'history_df': DataFrame simulazione
                - 'selection_counts': Series selection counts
                - 'avg_reward': float average reward
                - 'kbrs_rg': float KBRS general accuracy
                - 'config': dict configurazione usata
        """
        # Run simulation
        history_df = self.simulator.run_simulation(
            n_iterations=self.config.n_iterations,
            min_positives_per_user=self.config.min_positives_per_user,
            use_tqdm=self.config.use_tqdm,
            verbose=self.config.verbose,
        )

        # Collect results
        selection_counts = history_df['model_name'].value_counts(
            normalize=True
        ).mul(100).rename('Selection Rate (%)')

        avg_reward = history_df['reward'].mean()
        kbrs_rg = self.simulator.get_kbrs_general_accuracy()

        results = {
            'history_df': history_df,
            'selection_counts': selection_counts,
            'avg_reward': avg_reward,
            'kbrs_rg': kbrs_rg,
            'config': self.config.__dict__,
        }

        return results

    def compare_configurations(
        self,
        configs: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Run multiple experiments with different configurations.

        Args:
            configs: List of configuration dicts, each with:
                - 'name': str nome configurazione
                - 'top_k_similar': int (or other parameters to override)

        Returns:
            DataFrame with comparison results
        """
        comparison_results = []

        for config_dict in configs:
            # Create config
            config = ExperimentConfig(
                **self.config.__dict__,
                **{k: v for k, v in config_dict.items() if k != 'name'}
            )

            # Update simulator with new parameters
            self.config = config
            self._initialize_components()

            # Run experiment
            results = self.run()

            # Store results
            comparison_results.append({
                'Simulazione': config_dict['name'],
                'KBRS Selection (%)': results['selection_counts'].get('KBRS_Hybrid', 0),
                'Baseline Selection (%)': results['selection_counts'].get('Popularity_Baseline', 0),
                'Average Reward': results['avg_reward'],
                'KBRS R_G': results['kbrs_rg'],
            })

        return pd.DataFrame(comparison_results)

    def get_preprocessing_info(self) -> Dict[str, Any]:
        """Get information about preprocessed data."""
        return {
            'n_movies': len(self.data['cleaned_df']),
            'n_users': self.data['ratings_df']['userId'].nunique(),
            'n_ratings': len(self.data['ratings_df']),
            'cosine_sim_shape': self.data['cosine_sim_matrix'].shape,
            'precompute_kbrs': self.config.precompute_kbrs,
        }

    def __repr__(self) -> str:
        return (f"MABExperiment(n_iterations={self.config.n_iterations}, "
                f"top_k_similar={self.config.top_k_similar})")
