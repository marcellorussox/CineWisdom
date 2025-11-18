# src/simulation/simulator.py
import numpy as np
import pandas as pd
from typing import List, Dict

class MABSimulator:
    """
    Simulator per valutazione MAB con approccio Replay.

    Ad ogni evento sequenziale:
    1. Costruisce il contesto usando KBRSEngine
    2. Il MAB decide quale arm (film) scegliere
    3. Se coincide con l'evento reale → reward e update del MAB
    """

    def __init__(self, mab, kbrs_engine, reward_system):
        """
        Inizializza il simulator.

        Args:
            mab: Algoritmo MAB (es. LinUCB)
            kbrs_engine: KBRSEngine instance
            reward_system: RewardSystem instance
        """
        self.mab = mab
        self.kbrs_engine = kbrs_engine
        self.reward_system = reward_system
        self.cumulative_reward = 0
        self.rewards_history = []

    def run_replay(self, ratings_sorted_df: pd.DataFrame) -> Dict:
        """
        Esegue la simulazione Replay su TUTTI gli eventi ordinati per timestamp.

        Args:
            ratings_sorted_df: DataFrame con tutti i rating ordinati per timestamp

        Returns:
            Dict con risultati della simulazione
        """
        # Reset tracking
        self.cumulative_reward = 0
        self.rewards_history = []

        # Lista dei film candidati (tutti i film nel catalogo)
        candidate_movie_ids = self.kbrs_engine.movies_df['movieId'].tolist()

        # Iterazione sequenziale su TUTTI gli eventi
        for idx, event in ratings_sorted_df.iterrows():
            user_id = event['userId']
            movie_id = event['movieId']
            true_rating = event['rating']

            # 1. COSTRUISCI CONTESTO per MAB usando KBRS
            # Storia dell'utente PRIMA di questo evento (solo eventi passati!)
            user_history = ratings_sorted_df[
                (ratings_sorted_df.index < idx) &
                (ratings_sorted_df['userId'] == user_id)
            ]

            # Profilo utente (costruito con KBRS)
            user_profile = self.kbrs_engine.build_user_profile(user_history)

            # Feature del film dell'evento
            movie_features = self.kbrs_engine.get_item_features(movie_id)

            # Combina profilo + feature = CONTESTO per MAB
            context = np.concatenate([user_profile, movie_features])

            # 2. MAB DECIDE quale film raccomandare
            # Sceglie da un pool di film candidati
            # Per semplicità: sceglie un film a caso dal pool (può essere migliorato)
            recommended_movie_id = self.mab.choose_arm(context, candidate_movie_ids)

            # 3. VALUTA RICOMPENSA
            if recommended_movie_id == movie_id:
                # Match! Posso calcolare la ricompensa
                reward = self.reward_system.compute_reward(true_rating)
                self.cumulative_reward += reward

                # 4. AGGIORNA MAB con questa informazione
                self.mab.update(context, reward)

            self.rewards_history.append(self.cumulative_reward)

            if idx % 1000 == 0:
                print(f"  Processati {idx} eventi...")

        return {
            'cumulative_reward': self.rewards_history,
            'final_cumulative_reward': self.cumulative_reward,
            'final_ctr': self.cumulative_reward / len(ratings_sorted_df),
            'total_events': len(ratings_sorted_df)
        }
