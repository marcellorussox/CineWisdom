"""
Advanced Thompson Sampling implementation with optimization features.

This module provides:
- Optimistic initialization to reduce early exploration
- Temperature scheduling for controlled exploration
- Batch updates with regularization
- Informative priors support
"""

from __future__ import annotations

import numpy as np
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass


@dataclass
class ThompsonSamplingConfig:
    """Configuration for Thompson Sampling algorithm."""
    # Prior strength: higher values reduce initial uncertainty
    prior_strength: float = 2.0  # Beta(2,2) instead of Beta(1,1)

    # Temperature scheduling
    initial_temperature: float = 1.0
    min_temperature: float = 0.1
    temperature_decay: float = 0.995  # Per-iteration decay

    # Exploration control
    min_exploration_rate: float = 0.05  # Force exploration at least 5%

    # Batch update
    batch_size: int = 1  # Set >1 for batch learning
    regularization: float = 0.01  # L2 regularization

    # Warm start support
    warm_start_data: Optional[Dict[int, Tuple[int, int]]] = None  # arm -> (alpha, beta)


class AdvancedThompsonSampling:
    """
    Thompson Sampling with advanced optimization features.

    Optimizations implemented:
    1. Optimistic Initialization: Beta(prior_strength, prior_strength)
    2. Temperature Scheduling: Gradually reduce exploration
    3. Batch Updates: Accumulate rewards and update less frequently
    4. Regularization: Prevent overconfidence
    5. Informative Priors: Support for warm starting
    """

    def __init__(self, num_arms: int, config: Optional[ThompsonSamplingConfig] = None):
        self.num_arms = num_arms
        self.config = config or ThompsonSamplingConfig()
        self.iteration = 0
        self.temperature = self.config.initial_temperature

        # Initialize Beta distribution parameters with optimistic priors
        self.alphas = np.ones(num_arms) * self.config.prior_strength
        self.betas = np.ones(num_arms) * self.config.prior_strength

        # Batch update buffers
        self.reward_buffer: List[int] = []
        self.arm_buffer: List[int] = []

        # Apply warm start if provided
        if self.config.warm_start_data:
            for arm_idx, (alpha, beta) in self.config.warm_start_data.items():
                if 0 <= arm_idx < num_arms:
                    self.alphas[arm_idx] = alpha
                    self.betas[arm_idx] = beta

        # Statistics tracking
        self.selections = np.zeros(num_arms, dtype=int)
        self.cumulative_rewards = np.zeros(num_arms, dtype=int)

    def select_arm(self) -> int:
        """
        Select arm using Thompson Sampling with temperature scaling.

        Returns:
            Index of selected arm (0 to num_arms-1)
        """
        # Sample from posterior Beta distributions
        samples = np.random.beta(self.alphas, self.betas)

        # Apply temperature scaling to control exploration
        # Lower temperature -> more exploitation (sharp peaks)
        # Higher temperature -> more exploration (flatter distribution)
        scaled_samples = samples ** (1.0 / self.temperature)

        # Force exploration: check if any arm has been under-explored
        if len(self.selections) > 0:
            min_selections = np.min(self.selections)
            total_selections = np.sum(self.selections)

            # If system is too converged, force exploration of least selected arm
            if total_selections > 0 and min_selections > 0:
                exploration_rate = min_selections / total_selections
                if exploration_rate < self.config.min_exploration_rate:
                    # Return least selected arm
                    return int(np.argmin(self.selections))

        selected_arm = int(np.argmax(scaled_samples))
        self.selections[selected_arm] += 1
        self.iteration += 1

        return selected_arm

    def update(self, arm: int, reward: int) -> None:
        """
        Update posterior for the selected arm with regularization.

        Args:
            arm: Arm index (0 to num_arms-1)
            reward: Reward received (0 or 1)
        """
        # Add to batch buffer
        self.arm_buffer.append(arm)
        self.reward_buffer.append(reward)

        # Process batch if buffer is full
        if len(self.arm_buffer) >= self.config.batch_size:
            self._update_batch()

    def _update_batch(self) -> None:
        """Update posteriors using accumulated batch of rewards."""
        if not self.arm_buffer:
            return

        # Count rewards per arm
        arm_counts = {}
        for arm in set(self.arm_buffer):
            arm_counts[arm] = self.arm_buffer.count(arm)

        # Update each arm's posterior
        for arm in arm_counts:
            # Count successes and failures for this arm in the batch
            arm_rewards = [self.reward_buffer[i] for i, a in enumerate(self.arm_buffer) if a == arm]
            successes = sum(arm_rewards)
            failures = len(arm_rewards) - successes

            # Update with regularization
            # Add L2 regularization to prevent overconfidence
            self.alphas[arm] += successes
            self.betas[arm] += failures

            # Apply regularization (prevents alpha/beta from growing too large)
            self.alphas[arm] += self.config.regularization
            self.betas[arm] += self.config.regularization

        # Update cumulative statistics
        for arm, reward in zip(self.arm_buffer, self.reward_buffer):
            self.cumulative_rewards[arm] += reward

        # Decay temperature
        self.temperature = max(
            self.config.min_temperature,
            self.temperature * self.config.temperature_decay
        )

        # Clear buffers
        self.arm_buffer.clear()
        self.reward_buffer.clear()

    def finalize(self) -> None:
        """Process any remaining rewards in the buffer."""
        if self.arm_buffer:
            self._update_batch()

    def get_means(self) -> np.ndarray:
        """Get mean estimates for each arm (E[Beta(alpha, beta)] = alpha / (alpha + beta))."""
        return self.alphas / (self.alphas + self.betas)

    def get_statistics(self) -> Dict:
        """
        Get comprehensive statistics about the current state.

        Returns:
            Dictionary with detailed statistics
        """
        self.finalize()  # Ensure all pending updates are processed

        means = self.get_means()
        ucb_scores = means + np.sqrt(
            2 * np.log(self.iteration + 1) / (self.alphas + self.betas)
        )

        return {
            'iteration': self.iteration,
            'alphas': self.alphas.copy(),
            'betas': self.betas.copy(),
            'means': means,
            'ucb_scores': ucb_scores,
            'selections': self.selections.copy(),
            'cumulative_rewards': self.cumulative_rewards.copy(),
            'success_rates': self.cumulative_rewards / np.maximum(self.selections, 1),
            'temperature': self.temperature,
            'exploration_rate': np.min(self.selections) / np.maximum(np.sum(self.selections), 1),
        }

    def __repr__(self) -> str:
        return (f"AdvancedThompsonSampling(n_arms={self.num_arms}, "
                f"iteration={self.iteration}, "
                f"temp={self.temperature:.3f})")
