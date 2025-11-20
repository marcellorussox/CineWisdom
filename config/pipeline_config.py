"""
Unified Configuration for CineWisdom Pipeline
"""
from dataclasses import dataclass

@dataclass
class DataConfig:
    """Data configuration"""
    data_path: str = "datasets/raw"
    output_path: str = "datasets/processed"

    # Split ratios
    train_ratio: float = 0.70
    val_ratio: float = 0.15
    test_ratio: float = 0.15

    # Minimum interactions
    min_ratings_per_user: int = 5
    min_ratings_per_movie: int = 5

@dataclass
class NCFConfig:
    """NCF model configuration"""
    embedding_dim: int = 64
    mlp_hidden_dims: list = None
    learning_rate: float = 0.001
    batch_size: int = 32
    num_epochs: int = 50
    patience: int = 5

    def __post_init__(self):
        if self.mlp_hidden_dims is None:
            self.mlp_hidden_dims = [128, 64]

@dataclass
class MABConfig:
    """MAB configuration"""
    algorithms: list = None
    n_iterations: int = 300
    exploration_threshold: float = 4.0

    def __post_init__(self):
        if self.algorithms is None:
            self.algorithms = ['thompson', 'ucb1', 'epsilon']

@dataclass
class SVDConfig:
    """Feature extraction configuration"""
    n_components: int = 1024
    random_state: int = 42

# Global config instance
config = {
    'data': DataConfig(),
    'ncf': NCFConfig(),
    'mab': MABConfig(),
    'svd': SVDConfig()
}
