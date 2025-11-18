"""
Online Simulator - Wrapper per simulazione MAB online

Wrapper semplificato per MABSimulator esistente.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from pathlib import Path


class OnlineSimulator:
    """
    Simulator per valutazione MAB online.

    Gestisce:
    - Replay evaluation sequenziale
    - Tracking cumulative reward
    - Calcolo CTR
    - Comparazione algoritmi MAB
    """

    def __init__(self):
        """Inizializza l'online simulator."""
        print("🔄 Inizializzazione Online Simulator...")

        # Componenti esistenti
        from src.simulation.simulator import MABSimulator
        from src.simulation.reward_system import AdvancedRewardSystem, RewardConfig

        self.mab_simulator_class = MABSimulator
        self.reward_system_class = AdvancedRewardSystem
        self.reward_config_class = RewardConfig

        # Simulator instance
        self.simulator = None

        # MAB instance
        self.mab = None

        print("✅ Online Simulator inizializzato")

    def initialize_mab(self, mab_type: str = 'LinUCB',
                      n_features: int = 100,
                      alpha: float = 1.0,
                      **kwargs) -> object:
        """
        Inizializza l'algoritmo MAB.

        Args:
            mab_type: Tipo di MAB ('LinUCB', 'ThompsonSampling')
            n_features: Dimensione feature
            alpha: Parametro esplorazione
            **kwargs: Parametri aggiuntivi

        Returns:
            Istanza MAB inizializzata
        """
        print(f"🔄 Inizializzazione MAB: {mab_type}...")

        if mab_type == 'LinUCB':
            from src.online.linucb import LinUCB
            self.mab = LinUCB(n_features=n_features, alpha=alpha, **kwargs)
            print(f"✅ LinUCB inizializzato: features={n_features}, alpha={alpha}")

        elif mab_type == 'ThompsonSampling':
            # Usa Thompson Sampling esistente
            from src.bandit.mab_manager import MABManager
            self.mab = MABManager(['dummy'], **kwargs)
            print(f"✅ Thompson Sampling inizializzato")

        else:
            raise ValueError(f"Tipo MAB non supportato: {mab_type}")

        return self.mab

    def initialize_reward_system(self, weight_exploration: float = 0.5,
                                weight_accuracy: float = 0.5,
                                threshold: float = 4.0) -> object:
        """
        Inizializza il sistema di reward.

        Args:
            weight_exploration: Peso esplorazione
            weight_accuracy: Peso accuratezza
            threshold: Soglia per reward

        Returns:
            Istanza reward system
        """
        print(f"🔄 Inizializzazione reward system...")

        reward_config = self.reward_config_class(
            weight_exploration=weight_exploration,
            weight_accuracy=weight_accuracy,
            use_ndcg=False,
            update_calibration_every=10
        )

        reward_system = self.reward_system_class(
            config=reward_config,
            user_seen_movies={},
            popularity_top_ids=[]
        )

        print(f"✅ Reward system inizializzato")

        return reward_system

    def run_temporal_simulation(self, ratings_sorted_df: pd.DataFrame,
                               kbrs_wrapper,
                               mab,
                               reward_system,
                               max_iterations: Optional[int] = None) -> Dict:
        """
        Esegue simulazione temporale (replay evaluation).

        Args:
            ratings_sorted_df: DataFrame rating ordinato per timestamp
            kbrs_wrapper: KBRSWrapper instance
            mab: MAB instance (LinUCB, Thompson, etc.)
            reward_system: RewardSystem instance
            max_iterations: Max iterazioni (None = tutti gli eventi)

        Returns:
            Dict con risultati simulazione
        """
        print("🔄 Esecuzione simulazione temporale...")

        # Usa il simulator esistente (semplificato)
        # Nota: Il simulator originale è complesso, qui facciamo una versione semplificata

        if max_iterations:
            ratings_sorted_df = ratings_sorted_df.head(max_iterations)
            print(f"   Limitato a {max_iterations} iterazioni")

        print(f"   Eventi totali: {len(ratings_sorted_df)}")

        # Tracking
        cumulative_rewards = []
        ctr_history = []
        current_reward = 0
        current_ctr = 0

        # Lista candidati (tutti i film)
        candidate_movies = kbrs_wrapper.kbrs_engine.movies_df['movieId'].tolist()

        # Simula eventi sequenzialmente
        for idx, event in ratings_sorted_df.iterrows():
            user_id = event['userId']
            movie_id = event['movieId']
            true_rating = event['rating']
            timestamp = event['timestamp']

            # Costruisci contesto
            user_history = ratings_sorted_df[
                (ratings_sorted_df.index < idx) &
                (ratings_sorted_df['userId'] == user_id)
            ]

            user_profile = kbrs_wrapper.build_user_profile(user_history)
            movie_features = kbrs_wrapper.kbrs_engine.get_item_features(movie_id)

            # Combina per creare contesto
            context = np.concatenate([user_profile, movie_features])

            # MAB sceglie film
            try:
                recommended_movie = mab.choose_arm(context, candidate_movies)
            except Exception as e:
                # Fallback: scegli a caso
                recommended_movie = np.random.choice(candidate_movies)

            # Valuta reward
            if recommended_movie == movie_id:
                # Hit!
                reward = 1.0 if true_rating >= 4.0 else 0.0
                current_reward += reward

                # Aggiorna MAB
                try:
                    mab.update(context, reward, recommended_movie)
                except Exception as e:
                    logger.warning(f"Update failed: {e}")

            # Update tracking
            current_ctr = current_reward / (idx + 1)
            cumulative_rewards.append(current_reward)
            ctr_history.append(current_ctr)

            # Progress
            if (idx + 1) % 1000 == 0:
                print(f"   Progresso: {idx + 1}/{len(ratings_sorted_df)} "
                      f"(CTR: {current_ctr:.4f})")

        # Risultati finali
        results = {
            'cumulative_rewards': cumulative_rewards,
            'ctr_history': ctr_history,
            'final_cumulative_reward': current_reward,
            'final_ctr': current_ctr,
            'total_events': len(ratings_sorted_df),
            'n_hits': int(current_reward),
            'mab_type': type(mab).__name__
        }

        print(f"✅ Simulazione completata:")
        print(f"   - Cumulative Reward: {current_reward}")
        print(f"   - CTR finale: {current_ctr:.4f}")
        print(f"   - Hit rate: {current_ctr:.1%}")

        return results

    def compare_algorithms(self, results_dict: Dict[str, Dict]) -> pd.DataFrame:
        """
        Confronta risultati di diversi algoritmi MAB.

        Args:
            results_dict: Dict {algorithm_name: results}

        Returns:
            DataFrame con confronto
        """
        print("🔄 Confronto algoritmi MAB...")

        comparison_data = []

        for algo_name, results in results_dict.items():
            comparison_data.append({
                'Algorithm': algo_name,
                'Final_Cumulative_Reward': results['final_cumulative_reward'],
                'Final_CTR': results['final_ctr'],
                'Total_Events': results['total_events'],
                'Hit_Rate': results['final_ctr']
            })

        comparison_df = pd.DataFrame(comparison_data)
        comparison_df = comparison_df.sort_values('Final_Cumulative_Reward', ascending=False)

        print("✅ Confronto completato:")
        for _, row in comparison_df.iterrows():
            print(f"   {row['Algorithm']}: CTR={row['Final_CTR']:.4f}, "
                  f"Reward={row['Final_Cumulative_Reward']:.0f}")

        return comparison_df

    def plot_learning_curve(self, results: Dict, save_path: str = None):
        """
        Crea grafico della learning curve.

        Args:
            results: Dict con risultati simulazione
            save_path: Percorso per salvare il grafico
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            print("⚠️ matplotlib non installato, skipping plot")
            return

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

        # Cumulative Reward
        ax1.plot(results['cumulative_rewards'], color='#2E86AB', linewidth=2)
        ax1.set_title('Cumulative Reward Over Time')
        ax1.set_xlabel('Eventi')
        ax1.set_ylabel('Cumulative Reward')
        ax1.grid(True, alpha=0.3)

        # CTR
        ax2.plot(results['ctr_history'], color='#A23B72', linewidth=2)
        ax2.set_title('Click-Through Rate (CTR) Over Time')
        ax2.set_xlabel('Eventi')
        ax2.set_ylabel('CTR')
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"✅ Grafico salvato in {save_path}")

        plt.show()

    def save_results(self, results: Dict, output_path: str):
        """
        Salva risultati simulazione.

        Args:
            results: Dict con risultati
            output_path: Percorso file output
        """
        from src.common.utils import save_results

        save_results(results, output_path)
        print(f"✅ Risultati salvati in {output_path}")
