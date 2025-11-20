"""
Neural Collaborative Filtering (NCF) model implementation.
"""

from __future__ import annotations

import torch
import torch.nn as nn
import numpy as np
from typing import Dict, Any, Optional

from .base import BaseModel
from .embedding import UserMovieEmbedding, FeatureEmbedding


class NCF(BaseModel):
    """
    Neural Collaborative Filtering model with GMF, MLP, and SVD movie features.

    This model combines:
    - Generalized Matrix Factorization (GMF): Linear interaction between user and item embeddings
    - Multi-Layer Perceptron (MLP): Non-linear interaction
    - SVD Movie Features: TruncatedSVD compressed DBpedia features

    Architecture:
    1. TruncatedSVD: Reduces DBpedia features from ~16k → 4096 dimensions
    2. Feature Projection: Learnable linear layer projects 4096 → 128 dimensions
    3. User and Movie Embeddings: 128 dims each
    4. GMF path: Element-wise multiplication (128 dims)
    5. MLP path: Fully connected layers (output: 64 dims)
    6. Fusion: Concatenate GMF + MLP + projected features (128 + 64 + 128 = 320 dims)
    7. Output: Single rating prediction

    Benefits:
    - SVD preserves most important directions in high-dimensional space
    - Learnable projection adapts features to rating prediction task
    - Balanced between feature preservation and task-specific adaptation
    """

    def __init__(
        self,
        num_users: int,
        num_movies: int,
        feature_dim: int = 0,  # NEW: Dimensionality of SVD movie features
        embedding_dim: int = 128,
        mlp_hidden_dims: list[int] | None = None,
        mlp_dropout: float = 0.2,
        use_movie_features: bool = True,  # NEW: Toggle for using movie features
        use_bias: bool = True
    ):
        """
        Initialize NCF model.

        Args:
            num_users: Number of unique users
            num_movies: Number of unique movies
            feature_dim: Dimensionality of movie features (SVD compressed)
            embedding_dim: Dimension of user/movie embeddings
            mlp_hidden_dims: List of hidden layer dimensions for MLP
            mlp_dropout: Dropout probability
            use_movie_features: Whether to use precomputed movie features
            use_bias: Whether to use bias terms
        """
        super().__init__()

        self.num_users = num_users
        self.num_movies = num_movies
        self.feature_dim = feature_dim
        self.embedding_dim = embedding_dim
        self.mlp_hidden_dims = mlp_hidden_dims or [256, 128, 64]
        self.mlp_dropout = mlp_dropout
        self.use_movie_features = use_movie_features

        # Store model configuration
        self.model_config = {
            'num_users': num_users,
            'num_movies': num_movies,
            'feature_dim': feature_dim,
            'embedding_dim': embedding_dim,
            'mlp_hidden_dims': self.mlp_hidden_dims,
            'mlp_dropout': mlp_dropout,
            'use_movie_features': use_movie_features,
        }

        # User and Movie Embeddings
        self.user_movie_embedding = UserMovieEmbedding(
            num_users=num_users,
            num_movies=num_movies,
            embedding_dim=embedding_dim
        )

        # GMF (Generalized Matrix Factorization) path
        self.gmf_dim = embedding_dim

        # MLP path
        self.mlp_input_dim = embedding_dim * 2  # Concatenated user and movie embeddings
        self.mlp_layers = nn.ModuleList()

        # Build MLP layers
        prev_dim = self.mlp_input_dim
        for hidden_dim in self.mlp_hidden_dims:
            self.mlp_layers.append(nn.Linear(prev_dim, hidden_dim))
            self.mlp_layers.append(nn.ReLU())
            self.mlp_layers.append(nn.Dropout(mlp_dropout))
            prev_dim = hidden_dim

        # Final fusion layer
        # SVD (4096) + Proiezione learnable (4096 → 128)
        fusion_dim = self.gmf_dim + self.mlp_hidden_dims[-1]
        if self.use_movie_features and self.feature_dim > 0:
            fusion_dim += self.embedding_dim  # Add projected movie features (128)

        self.fusion_layer = nn.Linear(fusion_dim, fusion_dim)
        self.fusion_relu = nn.ReLU()

        # Output layer
        self.output_layer = nn.Linear(fusion_dim, 1)

        # Bias terms
        self.use_bias = use_bias
        if use_bias:
            self.user_bias = nn.Embedding(num_users, 1)
            self.movie_bias = nn.Embedding(num_movies, 1)
            self.global_bias = nn.Parameter(torch.zeros(1))

            # Initialize bias embeddings
            nn.init.normal_(self.user_bias.weight, std=0.01)
            nn.init.normal_(self.movie_bias.weight, std=0.01)

        # Movie feature projection (SVD 4096 → embedding_dim 128)
        if self.use_movie_features and self.feature_dim > 0:
            # Project SVD features (4096 dims) to embedding dimension (128) for fusion
            self.feature_proj = nn.Linear(self.feature_dim, self.embedding_dim)
        else:
            self.feature_proj = None

    def forward(
        self,
        user_ids: torch.Tensor,
        movie_ids: torch.Tensor,
        movie_features: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """
        Forward pass.

        Args:
            user_ids: User IDs [batch_size]
            movie_ids: Movie IDs [batch_size]
            movie_features: Optional movie features [batch_size, feature_dim]

        Returns:
            Predicted ratings [batch_size, 1]
        """
        # Get user and movie embeddings
        user_emb, movie_emb = self.user_movie_embedding(user_ids, movie_ids)

        # GMF path: Element-wise multiplication
        gmf_output = user_emb * movie_emb

        # MLP path: Concatenate embeddings and pass through MLP
        mlp_input = torch.cat([user_emb, movie_emb], dim=-1)
        mlp_output = mlp_input
        for layer in self.mlp_layers:
            mlp_output = layer(mlp_output)

        # Start fusion with GMF and MLP
        fusion_parts = [gmf_output, mlp_output]

        # Add movie features (with projection)
        if self.use_movie_features and movie_features is not None and self.feature_proj is not None:
            # Project SVD features (4096 → 128)
            projected_features = self.feature_proj(movie_features)
            fusion_parts.append(projected_features)

        # Fuse all parts
        fusion_input = torch.cat(fusion_parts, dim=-1)
        fusion_output = self.fusion_relu(self.fusion_layer(fusion_input))

        # Final prediction
        prediction = self.output_layer(fusion_output)

        # Add bias terms
        if self.use_bias:
            user_bias = self.user_bias(user_ids).squeeze()
            movie_bias = self.movie_bias(movie_ids).squeeze()
            prediction = prediction.squeeze() + user_bias + movie_bias + self.global_bias
            prediction = prediction.unsqueeze(-1)

        # 🔧 CRITICAL FIX: Map predictions to valid rating range [0.5, 5.0]
        # Use sigmoid + scaling for smooth mapping and better gradient flow
        # Sigmoid outputs [0, 1], then scale to [0.5, 5.0]
        prediction = torch.sigmoid(prediction) * 4.5 + 0.5

        return prediction

    def get_model_info(self) -> Dict[str, Any]:
        """Get model information."""
        total_params = sum(p.numel() for p in self.parameters())
        trainable_params = sum(p.numel() for p in self.parameters() if p.requires_grad)

        info = {
            'model_type': 'NCF',
            'num_users': self.num_users,
            'num_movies': self.num_movies,
            'feature_dim': self.feature_dim,
            'use_movie_features': self.use_movie_features,
            'embedding_dim': self.embedding_dim,
            'mlp_hidden_dims': self.mlp_hidden_dims,
            'total_parameters': total_params,
            'trainable_parameters': trainable_params,
        }

        return info

    def predict(
        self,
        user_ids: torch.Tensor | list[int] | int,
        movie_ids: torch.Tensor | list[int] | int,
        movie_features: Optional[torch.Tensor] = None,
        device: str = 'cpu'
    ) -> np.ndarray:
        """
        Predict ratings for user-movie pairs.

        Args:
            user_ids: User ID(s)
            movie_ids: Movie ID(s)
            movie_features: Optional movie features
            device: Device to use

        Returns:
            Predicted ratings as numpy array
        """
        self.eval()
        self.to(device)

        # Convert to tensors if needed
        if isinstance(user_ids, int):
            user_ids = torch.tensor([user_ids], dtype=torch.long, device=device)
        elif isinstance(user_ids, list):
            user_ids = torch.tensor(user_ids, dtype=torch.long, device=device)

        if isinstance(movie_ids, int):
            movie_ids = torch.tensor([movie_ids], dtype=torch.long, device=device)
        elif isinstance(movie_ids, list):
            movie_ids = torch.tensor(movie_ids, dtype=torch.long, device=device)

        with torch.no_grad():
            predictions = self.forward(user_ids, movie_ids, movie_features)
            return predictions.cpu().numpy()
