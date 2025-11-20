"""
Dataset utilities for CineWisdom NCF+MAB System.
"""

from __future__ import annotations

import torch
from torch.utils.data import Dataset, DataLoader
import pandas as pd
from typing import Tuple, Optional


class RatingDataset(Dataset):
    """
    Dataset for rating prediction.

    Converts rating data into PyTorch tensors for training.
    """

    def __init__(
        self,
        ratings_df: pd.DataFrame,
        user_id_map: Optional[dict] = None,
        movie_id_map: Optional[dict] = None,
        movie_features_df: Optional[pd.DataFrame] = None,  # NEW: SVD movie features
        feature_dim: int = 0,  # NEW: Dimensionality of features
        rating_scale: Tuple[float, float] = (0.5, 5.0)  # FIX: MovieLens ratings are [0.5, 5.0], not [1.0, 5.0]
    ):
        """
        Initialize dataset.

        Args:
            ratings_df: DataFrame with columns ['userId', 'movieId', 'rating']
            user_id_map: Mapping from original user IDs to 0-indexed IDs
            movie_id_map: Mapping from original movie IDs to 0-indexed IDs
            movie_features_df: DataFrame with movie features (SVD compressed)
            feature_dim: Dimensionality of movie features
            rating_scale: Original rating scale (min, max)
        """
        self.ratings_df = ratings_df.copy()
        self.movie_features_df = movie_features_df
        self.feature_dim = feature_dim

        # Create ID mappings if not provided
        if user_id_map is None:
            unique_users = sorted(self.ratings_df['userId'].unique())
            self.user_id_map = {uid: idx for idx, uid in enumerate(unique_users)}
        else:
            self.user_id_map = user_id_map

        if movie_id_map is None:
            unique_movies = sorted(self.ratings_df['movieId'].unique())
            self.movie_id_map = {mid: idx for idx, mid in enumerate(unique_movies)}
        else:
            self.movie_id_map = movie_id_map

        # Normalize user and movie IDs
        self.ratings_df['user_idx'] = self.ratings_df['userId'].map(self.user_id_map)
        self.ratings_df['movie_idx'] = self.ratings_df['movieId'].map(self.movie_id_map)

        # Store movie features as a dictionary for lazy loading (MEMORY OPTIMIZATION)
        # Instead of merging 640k rows with 1024 features (= 5GB RAM), we lookup on-demand
        self.movie_features_dict = None
        if movie_features_df is not None:
            # Create a dict: {movieId: [feature_0, feature_1, ..., feature_1023]}
            feature_cols = [col for col in movie_features_df.columns if col.startswith('ncf_feature_')]
            self.movie_features_dict = {}
            for _, row in movie_features_df.iterrows():
                movie_id = row['movieId']
                features = row[feature_cols].values.astype('float32')  # Use float32 to save memory
                self.movie_features_dict[movie_id] = features
            
            print(f"  Loaded features for {len(self.movie_features_dict)} movies into lookup dict")

        # Check for unmapped IDs (should be rare with proper mapping)
        missing_users = self.ratings_df['user_idx'].isna().sum()
        missing_movies = self.ratings_df['movie_idx'].isna().sum()

        if missing_users > 0 or missing_movies > 0:
            raise ValueError(
                f"Found unmapped IDs: {missing_users} users, {missing_movies} movies. "
                "This should not happen if mappings are created from all data."
            )

        # Convert to integers
        self.ratings_df['user_idx'] = self.ratings_df['user_idx'].astype(int)
        self.ratings_df['movie_idx'] = self.ratings_df['movie_idx'].astype(int)

        # Store original scale for denormalization
        self.rating_scale = rating_scale

    def __len__(self) -> int:
        return len(self.ratings_df)

    def __getitem__(self, idx: int) -> dict:
        row = self.ratings_df.iloc[idx]

        # Ensure values are valid before conversion
        user_idx = int(row['user_idx'])
        movie_idx = int(row['movie_idx'])
        rating = float(row['rating'])

        # Prepare base return dict
        item = {
            'user_ids': torch.tensor(user_idx, dtype=torch.long),
            'movie_ids': torch.tensor(movie_idx, dtype=torch.long),
            'ratings': torch.tensor(rating, dtype=torch.float),
            'userId': row['userId'],
            'movieId': row['movieId']
        }

        # Add movie features if available (lazy loading from dict)
        if self.movie_features_dict is not None and self.feature_dim > 0:
            movie_id = row['movieId']
            if movie_id in self.movie_features_dict:
                movie_features = self.movie_features_dict[movie_id]
            else:
                # Movie not in features dict (shouldn't happen, but handle gracefully)
                movie_features = torch.zeros(self.feature_dim, dtype=torch.float32)
            item['movie_features'] = torch.tensor(movie_features, dtype=torch.float32)

        return item

    def get_id_maps(self) -> Tuple[dict, dict]:
        """Get user and movie ID mappings."""
        return self.user_id_map, self.movie_id_map

    def get_num_users_movies(self) -> Tuple[int, int]:
        """Get number of unique users and movies."""
        return len(self.user_id_map), len(self.movie_id_map)


def create_data_loaders(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    movie_features_df: Optional[pd.DataFrame] = None,  # NEW
    feature_dim: int = 0,  # NEW
    batch_size: int = 512,
    num_workers: int = 4
) -> Tuple[DataLoader, DataLoader, DataLoader, dict]:
    """
    Create PyTorch DataLoaders for train/val/test sets.

    Args:
        train_df: Training DataFrame
        val_df: Validation DataFrame
        test_df: Test DataFrame
        movie_features_df: DataFrame with SVD movie features
        feature_dim: Dimensionality of movie features
        batch_size: Batch size
        num_workers: Number of DataLoader workers

    Returns:
        Tuple of (train_loader, val_loader, test_loader, id_maps)
    """
    # IMPORTANT: Create ID mappings from ALL data (train + val + test) to ensure consistency
    all_df = pd.concat([train_df, val_df, test_df], ignore_index=True)

    # Create ID mappings from all data
    all_dataset = RatingDataset(all_df)
    user_id_map = all_dataset.user_id_map
    movie_id_map = all_dataset.movie_id_map

    print(f"\nCreated ID mappings from all data:")
    print(f"  Users: {len(user_id_map)}")
    print(f"  Movies: {len(movie_id_map)}")

    # Now create datasets using the comprehensive mappings
    train_dataset = RatingDataset(
        train_df,
        user_id_map=user_id_map,
        movie_id_map=movie_id_map,
        movie_features_df=movie_features_df,  # NEW
        feature_dim=feature_dim  # NEW
    )
    val_dataset = RatingDataset(
        val_df,
        user_id_map=user_id_map,
        movie_id_map=movie_id_map,
        movie_features_df=movie_features_df,  # NEW
        feature_dim=feature_dim  # NEW
    )
    test_dataset = RatingDataset(
        test_df,
        user_id_map=user_id_map,
        movie_id_map=movie_id_map,
        movie_features_df=movie_features_df,  # NEW
        feature_dim=feature_dim  # NEW
    )

    print(f"\nDataset sizes after ID mapping:")
    print(f"  Train: {len(train_dataset)}")
    print(f"  Val: {len(val_dataset)}")
    print(f"  Test: {len(test_dataset)}")

    if movie_features_df is not None:
        print(f"  Movie features: {feature_dim} dimensions")

    # Create data loaders
    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=num_workers
    )

    val_loader = DataLoader(
        val_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers
    )

    test_loader = DataLoader(
        test_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers
    )

    id_maps = {
        'user_id_map': user_id_map,
        'movie_id_map': movie_id_map,
        'num_users': len(user_id_map),
        'num_movies': len(movie_id_map)
    }

    # Add feature info if available
    if movie_features_df is not None:
        id_maps['feature_dim'] = feature_dim
        id_maps['movie_features_df'] = movie_features_df

    return train_loader, val_loader, test_loader, id_maps
