"""
Configuration Management per CineWisdom NCF+MAB System.

Centralized configuration per tutti i componenti del sistema.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Any, Optional


@dataclass
class DataConfig:
    """Configuration for data loading and preprocessing."""
    data_path: str = "datasets/raw"
    processed_path: str = "datasets/processed"
    splits_path: str = "datasets/splits"

    # Split ratios
    train_size: float = 0.8
    val_size: float = 0.1
    test_size: float = 0.1

    # User filtering
    min_ratings_per_user: int = 3

    # Random seed
    random_state: int = 42





@dataclass
class MABConfig:
    """Configuration for Multi-Armed Bandit."""
    algorithm: str = "thompson"  # 'thompson', 'ucb', 'epsilon'

    # Thompson Sampling specific
    prior_strength: float = 2.0
    initial_temperature: float = 1.0
    min_temperature: float = 0.1
    temperature_decay: float = 0.995

    # Epsilon-Greedy specific
    epsilon: float = 0.1

    # UCB specific
    confidence_level: float = 2.0


@dataclass
class RewardConfig:
    """Configuration for reward system."""
    # Reward thresholds
    high_rating_threshold: float = 4.0
    medium_rating_threshold: float = 3.0

    # Reward weights
    weight_accuracy: float = 0.6
    weight_novelty: float = 0.2
    weight_satisfaction: float = 0.2


@dataclass
class ExperimentConfig:
    """Configuration for experiments."""
    experiment_name: str = "ncf_mab_experiment"
    output_dir: str = "experiments"
    models_dir: str = "models"
    plots_dir: str = "plots"

    # Logging
    log_level: str = "INFO"
    save_checkpoints: bool = True
    save_best_only: bool = False


@dataclass
class CineWisdomConfig:
    """Master configuration for CineWisdom system."""
    data: DataConfig = field(default_factory=DataConfig)

    mab: MABConfig = field(default_factory=MABConfig)
    reward: RewardConfig = field(default_factory=RewardConfig)
    experiment: ExperimentConfig = field(default_factory=ExperimentConfig)

    # Version
    version: str = "2.0.0"

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'data': self.data.__dict__,

            'mab': self.mab.__dict__,
            'reward': self.reward.__dict__,
            'experiment': self.experiment.__dict__,
            'version': self.version
        }

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> CineWisdomConfig:
        """Create configuration from dictionary."""
        config = cls()
        for section, values in config_dict.items():
            if hasattr(config, section):
                section_config = getattr(config, section)
                for key, value in values.items():
                    if hasattr(section_config, key):
                        setattr(section_config, key, value)
        return config

    def save(self, path: str) -> None:
        """Save configuration to file."""
        import json
        with open(path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: str) -> CineWisdomConfig:
        """Load configuration from file."""
        import json
        with open(path, 'r') as f:
            config_dict = json.load(f)
        return cls.from_dict(config_dict)


# Global configuration instance
config = CineWisdomConfig()
