"""
LinUCB - Linear Upper Confidence Bound Algorithm

Implementazione dell'algoritmo LinUCB per Multi-Armed Bandit contextuelle.
"""

import numpy as np
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class LinUCB:
    """
    Linear Upper Confidence Bound (LinUCB) Algorithm.

    Algoritmo di bandit contestuale che assume una relazione lineare
    tra il reward e le feature del contesto.

    Args:
        n_features: Dimensione del vettore di contesto
        alpha: Parametro di esplorazione (default: 1.0)
        random_state: Seed per riproducibilità (default: 42)
    """

    def __init__(self, n_features: int, alpha: float = 1.0, random_state: int = 42):
        """
        Inizializza LinUCB.

        Args:
            n_features: Dimensione del vettore di contesto
            alpha: Parametro di esplorazione
            random_state: Seed per riproducibilità
        """
        self.n_features = n_features
        self.alpha = alpha
        self.random_state = random_state

        # Inizializza generator random
        np.random.seed(random_state)

        # Matrice A: accumulo di (x_i * x_i^T) per ogni arms
        # Dict {arm_id: np.ndarray}
        self.A = {}

        # Vettore b: accumulo di (x_i * reward_i) per ogni arms
        # Dict {arm_id: np.ndarray}
        self.b = {}

        # Theta inverso per ogni arm
        # Dict {arm_id: np.ndarray}
        self.theta_inv = {}

        # Theta corrente per ogni arm
        # Dict {arm_id: np.ndarray}
        self.theta = {}

        # Statistiche
        self.n_arms = 0
        self.total_updates = 0

        logger.info(f"LinUCB initialized: features={n_features}, alpha={alpha}")

    def _init_arm(self, arm_id):
        """Inizializza un nuovo arm."""
        if arm_id not in self.A:
            self.A[arm_id] = np.eye(self.n_features)
            self.b[arm_id] = np.zeros(self.n_features)
            self.theta_inv[arm_id] = np.eye(self.n_features)
            self.theta[arm_id] = np.zeros(self.n_features)
            self.n_arms += 1
            logger.debug(f"Initialized arm {arm_id}")

    def choose_arm(self, context: np.ndarray, candidate_arms: List[int]) -> int:
        """
        Sceglie un arm usando LinUCB.

        Args:
            context: Vettore di contesto (shape: n_features,)
            candidate_arms: Lista di ID degli arms candidati

        Returns:
            ID dell'arm scelto
        """
        if not candidate_arms:
            raise ValueError("Nessun arm candidato disponibile")

        # Inizializza arms se necessario
        for arm_id in candidate_arms:
            self._init_arm(arm_id)

        # Calcola score per ogni arm
        arm_scores = {}

        for arm_id in candidate_arms:
            # Calcola theta corrente (se già aggiornato)
            if arm_id in self.theta_inv:
                try:
                    self.theta[arm_id] = self.theta_inv[arm_id] @ self.b[arm_id]
                except np.linalg.LinAlgError:
                    logger.warning(f"Singular matrix for arm {arm_id}, using pseudo-inverse")
                    self.theta[arm_id] = np.linalg.pinv(self.A[arm_id]) @ self.b[arm_id]

            # Calcola upper confidence bound
            context = context.reshape(-1, 1)  # (n_features, 1)

            mean_reward = self.theta[arm_id].T @ context.flatten()
            confidence = self.alpha * np.sqrt(
                context.flatten().T @ self.theta_inv[arm_id] @ context.flatten()
            )

            ucb_score = mean_reward + confidence
            arm_scores[arm_id] = ucb_score

        # Scegli arm con score più alto
        best_arm = max(arm_scores, key=arm_scores.get)

        logger.debug(f"Choosing arm {best_arm} with score {arm_scores[best_arm]:.4f}")

        return best_arm

    def update(self, context: np.ndarray, reward: float, arm_id: int):
        """
        Aggiorna il modello con una nuova osservazione.

        Args:
            context: Vettore di contesto
            reward: Reward osservato (0.0 o 1.0)
            arm_id: ID dell'arm utilizzato
        """
        # Inizializza arm se necessario
        self._init_arm(arm_id)

        # Aggiorna A e b
        context = context.flatten()
        self.A[arm_id] += np.outer(context, context)
        self.b[arm_id] += reward * context

        # Aggiorna theta inverso
        try:
            self.theta_inv[arm_id] = np.linalg.inv(self.A[arm_id])
        except np.linalg.LinAlgError:
            logger.warning(f"Singular matrix for arm {arm_id}, using pseudo-inverse")
            self.theta_inv[arm_id] = np.linalg.pinv(self.A[arm_id])

        # Aggiorna theta
        self.theta[arm_id] = self.theta_inv[arm_id] @ self.b[arm_id]

        self.total_updates += 1

        logger.debug(f"Updated arm {arm_id} with reward {reward}")

    def predict_reward(self, context: np.ndarray, arm_id: int) -> float:
        """
        Predice il reward atteso per un arm e contesto.

        Args:
            context: Vettore di contesto
            arm_id: ID dell'arm

        Returns:
            Reward predetto
        """
        if arm_id not in self.theta:
            return 0.0

        context = context.flatten()
        predicted_reward = self.theta[arm_id].T @ context

        return float(predicted_reward)

    def get_arm_statistics(self) -> Dict:
        """
        Ottiene statistiche per ogni arm.

        Returns:
            Dict con statistiche per arm
        """
        stats = {}

        for arm_id in self.A.keys():
            # Numero di aggiornamenti per arm (approx)
            n_updates = self.A[arm_id][0, 0]  # Traccia diagonale - 1

            stats[arm_id] = {
                'n_updates': n_updates,
                'mean_estimate': float(np.mean(self.theta[arm_id])),
                'confidence_norm': float(np.linalg.norm(self.theta[arm_id])),
                'theta_inv_norm': float(np.linalg.norm(self.theta_inv[arm_id]))
            }

        return stats

    def reset(self):
        """Reset l'algoritmo."""
        self.A = {}
        self.b = {}
        self.theta_inv = {}
        self.theta = {}
        self.n_arms = 0
        self.total_updates = 0

        logger.info("LinUCB reset")

    def __repr__(self) -> str:
        return (f"LinUCB(n_features={self.n_features}, "
                f"alpha={self.alpha}, "
                f"arms={self.n_arms}, "
                f"updates={self.total_updates})")


class LinUCBWithDisjoint:
    """
    LinUCB con modelli disjoint (ogni arm ha il proprio modello lineare).

    Versione standard di LinUCB dove ogni arm ha la propria matrice A e b.
    """

    def __init__(self, n_features: int, alpha: float = 1.0, random_state: int = 42):
        """
        Inizializza LinUCB disjoint.

        Args:
            n_features: Dimensione del vettore di contesto
            alpha: Parametro di esplorazione
            random_state: Seed per riproducibilità
        """
        super().__init__(n_features, alpha, random_state)

    def choose_arm(self, context: np.ndarray, candidate_arms: List[int]) -> int:
        """
        Sceglie un arm usando LinUCB disjoint.

        Args:
            context: Vettore di contesto (shape: n_features,)
            candidate_arms: Lista di ID degli arms candidati

        Returns:
            ID dell'arm scelto
        """
        return super().choose_arm(context, candidate_arms)

    def update(self, context: np.ndarray, reward: float, arm_id: int):
        """
        Aggiorna il modello con una nuova osservazione.

        Args:
            context: Vettore di contesto
            reward: Reward osservato
            arm_id: ID dell'arm utilizzato
        """
        super().update(context, reward, arm_id)
