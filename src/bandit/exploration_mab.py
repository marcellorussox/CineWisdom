"""
Exploration-Exploitation MAB Manager - Opzione 3.

Questo modulo implementa un MAB per scegliere la strategia per ogni utente:
- Arm 0: EXPLORATION (raccomanda film diversi/diversi)
- Arm 1: EXPLOITATION (raccomanda film simili a quelli visti)

Ogni utente ha il proprio MAB Thompson Sampling per imparare le proprie preferenze.
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple
from dataclasses import dataclass
import numpy as np

from .algorithms import AdvancedThompsonSampling, ThompsonSamplingConfig


@dataclass
class ExplorationExploitationConfig:
    """Configuration for Exploration-Exploitation MAB."""
    # Thompson Sampling configuration
    prior_strength: float = 2.0
    initial_temperature: float = 1.0
    min_temperature: float = 0.1
    temperature_decay: float = 0.995
    min_exploration_rate: float = 0.05
    regularization: float = 0.01

    # Strategy thresholds
    exploration_similarity_threshold: float = 0.3  # Film con similarità < 0.3
    exploitation_similarity_threshold: float = 0.7  # Film con similarità > 0.7


class ExplorationExploitationMAB:
    """
    MAB per scegliere exploration vs exploitation per ogni utente.

    Architettura:
    - Ogni utente ha un AdvancedThompsonSampling dedicato
    - 2 arms: 0=EXPLORATION, 1=EXPLOITATION
    - Il MAB impara se l'utente preferisce esplorare o sfruttare

    Attributes
    ----------
    user_mabs : Dict[int, AdvancedThompsonSampling]
        MAB per ogni utente (lazy initialization)
    config : ExplorationExploitationConfig
        Configurazione del MAB
    user_stats : Dict[int, dict]
        Statistiche per utente (exploration_rate, exploitation_rate, etc.)
    """

    def __init__(self, config: Optional[ExplorationExploitationConfig] = None):
        """
        Inizializza l'Exploration-Exploitation MAB.

        Args:
            config: Configurazione (se None, usa default)
        """
        self.config = config or ExplorationExploitationConfig()
        self.user_mabs: Dict[int, AdvancedThompsonSampling] = {}
        self.user_stats: Dict[int, dict] = {}
        self._strategy_names = ['exploration', 'exploitation']

    def _get_or_create_user_mab(self, user_id: int) -> AdvancedThompsonSampling:
        """
        Lazy initialization del MAB per un utente.

        Args:
            user_id: ID dell'utente

        Returns:
            AdvancedThompsonSampling per l'utente
        """
        if user_id not in self.user_mabs:
            # Crea Thompson Sampling config per questo utente
            ts_config = ThompsonSamplingConfig(
                prior_strength=self.config.prior_strength,
                initial_temperature=self.config.initial_temperature,
                min_temperature=self.config.min_temperature,
                temperature_decay=self.config.temperature_decay,
                min_exploration_rate=self.config.min_exploration_rate,
                regularization=self.config.regularization
            )

            # Crea MAB per l'utente (2 arms: exploration, exploitation)
            self.user_mabs[user_id] = AdvancedThompsonSampling(
                num_arms=2,
                config=ts_config
            )

            # Inizializza statistiche per l'utente
            self.user_stats[user_id] = {
                'exploration_count': 0,
                'exploitation_count': 0,
                'exploration_reward_sum': 0.0,
                'exploitation_reward_sum': 0.0,
                'total_rewards': 0.0,
                'total_selections': 0
            }

        return self.user_mabs[user_id]

    def select_strategy(self, user_id: int) -> str:
        """
        Seleziona la strategia per un utente (exploration vs exploitation).

        Args:
            user_id: ID dell'utente

        Returns:
            'exploration' o 'exploitation'
        """
        mab = self._get_or_create_user_mab(user_id)
        selected_arm = mab.select_arm()
        strategy = self._strategy_names[selected_arm]

        # Aggiorna statistiche
        self.user_stats[user_id]['total_selections'] += 1
        if strategy == 'exploration':
            self.user_stats[user_id]['exploration_count'] += 1
        else:
            self.user_stats[user_id]['exploitation_count'] += 1

        return strategy

    def register_feedback(
        self,
        user_id: int,
        strategy: str,
        reward: float
    ) -> None:
        """
        Registra il feedback per la strategia scelta.

        Args:
            user_id: ID dell'utente
            strategy: 'exploration' o 'exploitation'
            reward: Ricompensa normalizzata (0.0 - 1.0)
        """
        if strategy not in self._strategy_names:
            raise ValueError(f"Strategy must be 'exploration' or 'exploitation', got {strategy}")

        mab = self._get_or_create_user_mab(user_id)
        arm_index = self._strategy_names.index(strategy)

        # Aggiorna MAB Thompson Sampling
        mab.update(arm_index, reward)

        # Aggiorna statistiche
        stats = self.user_stats[user_id]
        stats['total_rewards'] += reward

        if strategy == 'exploration':
            stats['exploration_reward_sum'] += reward
        else:
            stats['exploitation_reward_sum'] += reward

    def get_user_stats(self, user_id: int) -> dict:
        """
        Ottiene statistiche dettagliate per un utente.

        Args:
            user_id: ID dell'utente

        Returns:
            Dict con statistiche:
                - exploration_count, exploitation_count
                - exploration_rate, exploitation_rate
                - exploration_avg_reward, exploitation_avg_reward
                - total_selections, total_rewards
        """
        if user_id not in self.user_stats:
            return {
                'exploration_count': 0,
                'exploitation_count': 0,
                'exploration_rate': 0.0,
                'exploitation_rate': 0.0,
                'exploration_avg_reward': 0.0,
                'exploitation_avg_reward': 0.0,
                'total_selections': 0,
                'total_rewards': 0.0
            }

        stats = self.user_stats[user_id]
        total_selections = stats['total_selections']

        if total_selections == 0:
            return {
                'exploration_count': 0,
                'exploitation_count': 0,
                'exploration_rate': 0.0,
                'exploitation_rate': 0.0,
                'exploration_avg_reward': 0.0,
                'exploitation_avg_reward': 0.0,
                'total_selections': 0,
                'total_rewards': 0.0
            }

        # Calcola statistiche derivate
        exploration_count = stats['exploration_count']
        exploitation_count = stats['exploitation_count']

        exploration_rate = exploration_count / total_selections
        exploitation_rate = exploitation_count / total_selections

        exploration_avg_reward = (
            stats['exploration_reward_sum'] / exploration_count
            if exploration_count > 0 else 0.0
        )

        exploitation_avg_reward = (
            stats['exploitation_reward_sum'] / exploitation_count
            if exploitation_count > 0 else 0.0
        )

        return {
            'exploration_count': exploration_count,
            'exploitation_count': exploitation_count,
            'exploration_rate': exploration_rate,
            'exploitation_rate': exploitation_rate,
            'exploration_avg_reward': exploration_avg_reward,
            'exploitation_avg_reward': exploitation_avg_reward,
            'total_selections': total_selections,
            'total_rewards': stats['total_rewards']
        }

    def get_all_user_stats(self) -> Dict[int, dict]:
        """
        Ottiene statistiche per tutti gli utenti.

        Returns:
            Dict mapping user_id -> stats dict
        """
        return {user_id: self.get_user_stats(user_id)
                for user_id in self.user_stats.keys()}

    def get_mab_statistics(self, user_id: int) -> dict:
        """
        Ottiene statistiche complete del MAB per un utente.

        Args:
            user_id: ID dell'utente

        Returns:
            Dict con statistiche complete del Thompson Sampling
        """
        if user_id not in self.user_mabs:
            return {}

        return self.user_mabs[user_id].get_statistics()

    def get_preferred_strategy(self, user_id: int) -> Optional[str]:
        """
        Determina la strategia preferita dall'utente basandosi sui reward medi.

        Args:
            user_id: ID dell'utente

        Returns:
            'exploration', 'exploitation', o None se dati insufficienti
        """
        stats = self.get_user_stats(user_id)

        exploration_avg = stats['exploration_avg_reward']
        exploitation_avg = stats['exploitation_avg_reward']

        if exploration_avg == 0.0 and exploitation_avg == 0.0:
            return None

        if exploration_avg > exploitation_avg:
            return 'exploration'
        elif exploitation_avg > exploration_avg:
            return 'exploitation'
        else:
            return None  # Pari

    def reset_user(self, user_id: int) -> None:
        """
        Reset il MAB per un utente specifico.

        Args:
            user_id: ID dell'utente da resettare
        """
        if user_id in self.user_mabs:
            del self.user_mabs[user_id]
        if user_id in self.user_stats:
            del self.user_stats[user_id]

    def get_summary_statistics(self) -> dict:
        """
        Ottiene statistiche aggregate su tutti gli utenti.

        Returns:
            Dict con statistiche aggregate:
                - total_users
                - avg_exploration_rate
                - avg_exploitation_rate
                - most_preferred_strategy (globale)
        """
        if not self.user_stats:
            return {
                'total_users': 0,
                'avg_exploration_rate': 0.0,
                'avg_exploitation_rate': 0.0,
                'most_preferred_strategy': None
            }

        all_exploration_rates = []
        all_exploitation_rates = []
        exploration_preferrers = 0
        exploitation_preferrers = 0

        for user_id in self.user_stats.keys():
            stats = self.get_user_stats(user_id)
            all_exploration_rates.append(stats['exploration_rate'])
            all_exploitation_rates.append(stats['exploitation_rate'])

            preferred = self.get_preferred_strategy(user_id)
            if preferred == 'exploration':
                exploration_preferrers += 1
            elif preferred == 'exploitation':
                exploitation_preferrers += 1

        avg_exploration_rate = np.mean(all_exploration_rates)
        avg_exploitation_rate = np.mean(all_exploitation_rates)

        most_preferred = None
        if exploration_preferrers > exploitation_preferrers:
            most_preferred = 'exploration'
        elif exploitation_preferrers > exploration_preferrers:
            most_preferred = 'exploitation'

        return {
            'total_users': len(self.user_stats),
            'avg_exploration_rate': avg_exploration_rate,
            'avg_exploitation_rate': avg_exploitation_rate,
            'exploration_preferrers': exploration_preferrers,
            'exploitation_preferrers': exploitation_preferrers,
            'most_preferred_strategy': most_preferred
        }
