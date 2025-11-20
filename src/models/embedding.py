"""
Embedding utilities for NCF model.
"""

from __future__ import annotations

import torch
import torch.nn as nn


class FeatureEmbedding(nn.Module):
    """Feature embedding layer for categorical and numerical features."""

    def __init__(
        self,
        embedding_dims: Dict[str, int],
        numerical_features: list[str] | None = None
    ):
        """
        Initialize feature embeddings.

        Args:
            embedding_dims: Dictionary mapping feature_name to embedding_dim
            numerical_features: List of numerical feature names
        """
        super().__init__()
        self.embedding_dims = embedding_dims
        self.numerical_features = numerical_features or []

        # Create embedding layers for categorical features
        self.embeddings = nn.ModuleDict()
        for feature_name, embedding_dim in embedding_dims.items():
            if feature_name not in self.numerical_features:
                # For now, assume vocab_size is embedding_dim (will be updated during training)
                self.embeddings[feature_name] = nn.Embedding(
                    num_embeddings=embedding_dim,
                    embedding_dim=embedding_dim
                )

        # Linear projection for numerical features
        self.numerical_proj = nn.ModuleDict()
        for feature_name in self.numerical_features:
            self.numerical_proj[feature_name] = nn.Linear(1, embedding_dims[feature_name])

    def forward(self, features: Dict[str, torch.Tensor]) -> torch.Tensor:
        """
        Forward pass through embedding layers.

        Args:
            features: Dictionary of feature tensors

        Returns:
            Concatenated embeddings
        """
        embeddings = []

        for feature_name, feature_tensor in features.items():
            if feature_name in self.numerical_features:
                # Project numerical features
                emb = self.numerical_proj[feature_name](feature_tensor.unsqueeze(-1))
            else:
                # Use categorical embeddings
                emb = self.embeddings[feature_name](feature_tensor)

            embeddings.append(emb)

        return torch.cat(embeddings, dim=-1)


class UserMovieEmbedding(nn.Module):
    """Combined user and movie embeddings for NCF."""

    def __init__(
        self,
        num_users: int,
        num_movies: int,
        embedding_dim: int = 128
    ):
        """
        Initialize user and movie embeddings.

        Args:
            num_users: Number of unique users
            num_movies: Number of unique movies
            embedding_dim: Dimension of embeddings
        """
        super().__init__()
        self.user_embedding = nn.Embedding(num_users, embedding_dim)
        self.movie_embedding = nn.Embedding(num_movies, embedding_dim)

        # Initialize embeddings
        nn.init.normal_(self.user_embedding.weight, std=0.01)
        nn.init.normal_(self.movie_embedding.weight, std=0.01)

    def forward(self, user_ids: torch.Tensor, movie_ids: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        """
        Get user and movie embeddings.

        Args:
            user_ids: User IDs
            movie_ids: Movie IDs

        Returns:
            Tuple of (user_embedding, movie_embedding)
        """
        user_emb = self.user_embedding(user_ids)
        movie_emb = self.movie_embedding(movie_ids)

        return user_emb, movie_emb
