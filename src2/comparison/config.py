"""
Configuration Module - Parametric settings for both Traditional and MAB pipelines

This module centralizes all configuration parameters to allow easy tuning
of both Traditional and MAB pipelines without modifying code.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
from pathlib import Path


@dataclass
class PipelineConfig:
    """Configuration for a single pipeline (Traditional or MAB)."""

    # Data paths
    data_dir: str = "datasets"
    output_dir: str = "results"

    # Random seed for reproducibility
    random_state: int = 42


@dataclass
class TraditionalConfig(PipelineConfig):
    """Configuration specific to Traditional ML Pipeline."""

    # Split ratios (default: 80/10/10)
    train_size: float = 0.8
    val_size: float = 0.1
    test_size: float = 0.1

    # Filtering
    min_ratings_per_user: int = 5
    rating_threshold: float = 3.5  # For determining relevant items

    # Evaluation metrics
    k_values: List[int] = None

    # Prediction settings
    use_cosine_similarity: bool = True

    def __post_init__(self):
        if self.k_values is None:
            self.k_values = [5, 10, 20]


@dataclass
class MABConfig(PipelineConfig):
    """Configuration specific to MAB Pipeline."""

    # MAB algorithm settings
    algorithm: str = "thompson_sampling"  # or "epsilon_greedy", "ucb"
    alpha: float = 1.0
    beta: float = 1.0

    # Thompson Sampling specific
    temperature_init: float = 1.0
    temperature_min: float = 0.1
    temperature_decay: float = 0.995

    # Batch updates
    batch_size: int = 100
    update_frequency: int = 10

    # Simulation settings
    n_iterations: int = 2000
    n_recommendations: int = 20

    # Reward system
    reward_type: str = "composite"  # 'accuracy', 'exploration', 'composite'
    w_accuracy: float = 0.2
    w_exploration: float = 0.8

    # KBRS settings
    use_kbrs: bool = True
    top_k_similar: int = 100


@dataclass
class ComparisonConfig:
    """Configuration for the comparative analysis."""

    # Which pipelines to run
    run_traditional: bool = True
    run_mab: bool = True

    # Data configuration
    data_dir: str = "datasets"
    processed_dir: str = "datasets/processed"
    raw_dir: str = "datasets/raw"

    # Output directories
    results_dir: str = "results/comparison"
    traditional_dir: str = "results/traditional"
    mab_dir: str = "results/mab"

    # Traditional configuration
    traditional_config: TraditionalConfig = None

    # MAB configuration
    mab_config: MABConfig = None

    # Visualization settings
    save_plots: bool = True
    plot_format: str = "png"
    dpi: int = 150

    # Comparison settings
    compare_on_same_data: bool = True
    use_same_user_sample: bool = True
    n_sample_users: Optional[int] = None

    def __post_init__(self):
        if self.traditional_config is None:
            self.traditional_config = TraditionalConfig()

        if self.mab_config is None:
            self.mab_config = MABConfig()


class ConfigManager:
    """Utility class to manage and validate configurations."""

    @staticmethod
    def validate_traditional_config(config: TraditionalConfig) -> bool:
        """Validate Traditional pipeline configuration."""
        if not (0 < config.train_size < 1):
            raise ValueError(f"train_size must be in (0, 1), got {config.train_size}")

        if not (0 <= config.val_size < 1):
            raise ValueError(f"val_size must be in [0, 1), got {config.val_size}")

        if not (0 <= config.test_size < 1):
            raise ValueError(f"test_size must be in [0, 1), got {config.test_size}")

        total = config.train_size + config.val_size + config.test_size
        if abs(total - 1.0) > 1e-6:
            raise ValueError(
                f"Split ratios must sum to 1.0, got {total} "
                f"(train={config.train_size}, val={config.val_size}, test={config.test_size})"
            )

        if config.min_ratings_per_user < 1:
            raise ValueError(f"min_ratings_per_user must be >= 1, got {config.min_ratings_per_user}")

        if not (1 <= config.rating_threshold <= 5):
            raise ValueError(f"rating_threshold must be in [1, 5], got {config.rating_threshold}")

        return True

    @staticmethod
    def validate_mab_config(config: MABConfig) -> bool:
        """Validate MAB pipeline configuration."""
        if config.n_iterations < 10:
            raise ValueError(f"n_iterations must be >= 10, got {config.n_iterations}")

        if config.n_recommendations < 1:
            raise ValueError(f"n_recommendations must be >= 1, got {config.n_recommendations}")

        if not (0 < config.temperature_decay < 1):
            raise ValueError(f"temperature_decay must be in (0, 1), got {config.temperature_decay}")

        if config.batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {config.batch_size}")

        total_weight = config.w_accuracy + config.w_exploration
        if abs(total_weight - 1.0) > 1e-6:
            raise ValueError(
                f"Reward weights must sum to 1.0, got {total_weight} "
                f"(w_accuracy={config.w_accuracy}, w_exploration={config.w_exploration})"
            )

        return True

    @staticmethod
    def validate_comparison_config(config: ComparisonConfig) -> bool:
        """Validate comparison configuration."""
        if not (config.run_traditional or config.run_mab):
            raise ValueError("At least one pipeline (traditional or mab) must be enabled")

        # Validate nested configs
        if config.run_traditional and config.traditional_config:
            ConfigManager.validate_traditional_config(config.traditional_config)

        if config.run_mab and config.mab_config:
            ConfigManager.validate_mab_config(config.mab_config)

        return True

    @staticmethod
    def create_custom_config(
        train_size: float = 0.8,
        val_size: float = 0.1,
        test_size: float = 0.1,
        min_ratings: int = 5,
        mab_iterations: int = 2000,
        mab_recommendations: int = 20,
        reward_type: str = "composite",
        **kwargs
    ) -> ComparisonConfig:
        """
        Create a custom comparison configuration with easy parameter tuning.

        Args:
            train_size: Training set ratio
            val_size: Validation set ratio
            test_size: Test set ratio
            min_ratings: Minimum ratings per user filter
            mab_iterations: Number of MAB iterations
            mab_recommendations: Recommendations per iteration
            reward_type: Type of MAB reward ('accuracy', 'exploration', 'composite')
            **kwargs: Additional parameters for both pipelines

        Returns:
            ComparisonConfig with custom parameters
        """
        traditional_config = TraditionalConfig(
            train_size=train_size,
            val_size=val_size,
            test_size=test_size,
            min_ratings_per_user=min_ratings,
            **kwargs
        )

        mab_config = MABConfig(
            n_iterations=mab_iterations,
            n_recommendations=mab_recommendations,
            reward_type=reward_type,
            **kwargs
        )

        config = ComparisonConfig(
            traditional_config=traditional_config,
            mab_config=mab_config,
            **kwargs
        )

        return config

    @staticmethod
    def preset_fast_experiments() -> ComparisonConfig:
        """Preset for quick experiments (small data, fast execution)."""
        return ConfigManager.create_custom_config(
            train_size=0.7,
            val_size=0.15,
            test_size=0.15,
            min_ratings=10,
            mab_iterations=500,
            mab_recommendations=10,
            random_state=42
        )

    @staticmethod
    def preset_thorough_experiments() -> ComparisonConfig:
        """Preset for thorough experiments (full data, long execution)."""
        return ConfigManager.create_custom_config(
            train_size=0.8,
            val_size=0.1,
            test_size=0.1,
            min_ratings=5,
            mab_iterations=5000,
            mab_recommendations=50,
            random_state=123
        )

    @staticmethod
    def preset_publication_quality() -> ComparisonConfig:
        """Preset for publication-quality results."""
        return ConfigManager.create_custom_config(
            train_size=0.8,
            val_size=0.1,
            test_size=0.1,
            min_ratings=5,
            k_values=[5, 10, 20, 50],
            mab_iterations=10000,
            mab_recommendations=100,
            reward_type="composite",
            w_accuracy=0.3,
            w_exploration=0.7,
            temperature_decay=0.998,
            save_plots=True,
            plot_format="png",
            dpi=300,
            random_state=42
        )


# Predefined configuration presets
DEFAULT_CONFIG = ComparisonConfig()
FAST_CONFIG = ConfigManager.preset_fast_experiments()
THOROUGH_CONFIG = ConfigManager.preset_thorough_experiments()
PUBLICATION_CONFIG = ConfigManager.preset_publication_quality()

# Export presets
__all__ = [
    'PipelineConfig',
    'TraditionalConfig',
    'MABConfig',
    'ComparisonConfig',
    'ConfigManager',
    'DEFAULT_CONFIG',
    'FAST_CONFIG',
    'THOROUGH_CONFIG',
    'PUBLICATION_CONFIG'
]
