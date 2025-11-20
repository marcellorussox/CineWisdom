"""
Bandit Policies for Online Learning.

This module defines the abstract base class and concrete implementations
for Multi-Armed Bandit policies used in the CineWisdom online learning phase.
"""

from __future__ import annotations

import abc
import numpy as np
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass


class BanditPolicy(abc.ABC):
    """Abstract base class for Bandit Policies."""

    def __init__(self, num_arms: int):
        self.num_arms = num_arms
        self.counts = np.zeros(num_arms, dtype=int)
        self.rewards = np.zeros(num_arms, dtype=float)
        self.total_counts = 0

    @abc.abstractmethod
    def select_arm(self) -> int:
        """Select an arm to pull."""
        pass

    @abc.abstractmethod
    def update(self, arm: int, reward: float) -> None:
        """Update policy with observed reward."""
        pass

    @abc.abstractmethod
    def get_statistics(self) -> Dict:
        """Get internal statistics."""
        pass


@dataclass
class EpsilonGreedyConfig:
    epsilon: float = 0.1


class EpsilonGreedy(BanditPolicy):
    """Epsilon-Greedy Policy."""

    def __init__(self, num_arms: int, config: EpsilonGreedyConfig = EpsilonGreedyConfig()):
        super().__init__(num_arms)
        self.config = config
        self.values = np.zeros(num_arms, dtype=float)

    def select_arm(self) -> int:
        if np.random.random() < self.config.epsilon:
            return np.random.randint(0, self.num_arms)
        
        # Break ties randomly
        max_value = np.max(self.values)
        best_arms = np.where(self.values == max_value)[0]
        return np.random.choice(best_arms)

    def update(self, arm: int, reward: float) -> None:
        self.counts[arm] += 1
        self.total_counts += 1
        self.rewards[arm] += reward
        
        # Incremental update
        n = self.counts[arm]
        value = self.values[arm]
        self.values[arm] = value + (reward - value) / n

    def get_statistics(self) -> Dict:
        return {
            'name': 'EpsilonGreedy',
            'epsilon': self.config.epsilon,
            'counts': self.counts.tolist(),
            'values': self.values.tolist()
        }


@dataclass
class UCB1Config:
    confidence_level: float = 2.0


class UCB1(BanditPolicy):
    """UCB1 Policy."""

    def __init__(self, num_arms: int, config: UCB1Config = UCB1Config()):
        super().__init__(num_arms)
        self.config = config
        self.values = np.zeros(num_arms, dtype=float)

    def select_arm(self) -> int:
        # Ensure all arms are pulled at least once
        for arm in range(self.num_arms):
            if self.counts[arm] == 0:
                return arm

        # Calculate UCB values
        log_total = np.log(self.total_counts)
        confidence = np.sqrt(self.config.confidence_level * log_total / self.counts)
        ucb_values = self.values + confidence
        
        return np.argmax(ucb_values)

    def update(self, arm: int, reward: float) -> None:
        self.counts[arm] += 1
        self.total_counts += 1
        self.rewards[arm] += reward
        
        n = self.counts[arm]
        value = self.values[arm]
        self.values[arm] = value + (reward - value) / n

    def get_statistics(self) -> Dict:
        return {
            'name': 'UCB1',
            'confidence_level': self.config.confidence_level,
            'counts': self.counts.tolist(),
            'values': self.values.tolist()
        }


@dataclass
class ThompsonSamplingConfig:
    prior_strength: float = 2.0  # Beta(alpha, beta) prior strength


class ThompsonSampling(BanditPolicy):
    """Thompson Sampling Policy."""

    def __init__(self, num_arms: int, config: ThompsonSamplingConfig = ThompsonSamplingConfig()):
        super().__init__(num_arms)
        self.config = config
        # Initialize Beta priors
        self.alphas = np.ones(num_arms) * config.prior_strength
        self.betas = np.ones(num_arms) * config.prior_strength

    def select_arm(self) -> int:
        samples = np.random.beta(self.alphas, self.betas)
        return np.argmax(samples)

    def update(self, arm: int, reward: float) -> None:
        self.counts[arm] += 1
        self.total_counts += 1
        self.rewards[arm] += reward
        
        # Update Beta distribution
        # Reward is assumed to be binary (0 or 1) or in [0, 1]
        # For non-binary rewards, we can use Bernoulli trial logic with probability = reward
        # or just add reward to alpha and (1-reward) to beta
        self.alphas[arm] += reward
        self.betas[arm] += (1.0 - reward)

    def get_statistics(self) -> Dict:
        return {
            'name': 'ThompsonSampling',
            'alphas': self.alphas.tolist(),
            'betas': self.betas.tolist(),
            'counts': self.counts.tolist()
        }
