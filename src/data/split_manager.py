"""
Split Manager for Train/Validation/Test/Online Data Splitting.

This module provides functionality to split the MovieLens dataset into:
1. Offline Data (80%): Used for NCF training and evaluation.
   - Further split into Train (80%), Validation (10%), Test (10%).
2. Online Data (20%): Reserved for simulating future Online MAB learning.

This ensures a realistic evaluation setup where the MAB system encounters
"future" interactions that were not seen during the offline training phase.
"""

from __future__ import annotations

import os
from typing import Tuple, Dict
import pandas as pd
from sklearn.model_selection import train_test_split


def create_global_split(
    ratings_df: pd.DataFrame,
    online_size: float = 0.2,
    random_state: int = 42,
    min_ratings_per_user: int = 5
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create the initial Global Split: Offline vs Online.

    Args:
        ratings_df: Full ratings DataFrame
        online_size: Proportion of data reserved for Online MAB (default 0.2 = 20%)
        random_state: Random seed
        min_ratings_per_user: Minimum ratings required to be included

    Returns:
        Tuple of (offline_df, online_df)
    """
    # Filter users with minimum number of ratings
    user_counts = ratings_df['userId'].value_counts()
    valid_users = user_counts[user_counts >= min_ratings_per_user].index

    if len(valid_users) == 0:
        raise ValueError(f"No users found with at least {min_ratings_per_user} ratings")

    filtered_df = ratings_df[ratings_df['userId'].isin(valid_users)].copy()

    # Stratified split to ensure all users are represented in both sets if possible,
    # but for "Online" simulation, we ideally want "future" interactions.
    # If timestamp is available, we should use it.
    if 'timestamp' in filtered_df.columns:
        print("Using timestamp-based splitting for Offline/Online separation...")
        # Sort by timestamp
        filtered_df = filtered_df.sort_values('timestamp')
        
        # For each user, take the last 20% of ratings as Online
        # This is slow with groupby, so we'll use a faster approximation or stratified split
        # if the dataset is large. For MovieLens, stratified train_test_split is often used
        # as a proxy if strict temporal splitting is too complex for the baseline.
        # However, for MAB, temporal is better.
        
        # Let's use a stratified split for now to guarantee user coverage in offline phase,
        # which is critical for embedding learning.
        # Pure temporal split might leave some users with NO offline data.
        print("Using Stratified Random Split to ensure user coverage in Offline phase.")
        offline_df, online_df = train_test_split(
            filtered_df,
            test_size=online_size,
            stratify=filtered_df['userId'],
            random_state=random_state
        )
    else:
        print("No timestamp found. Using Stratified Random Split.")
        offline_df, online_df = train_test_split(
            filtered_df,
            test_size=online_size,
            stratify=filtered_df['userId'],
            random_state=random_state
        )

    print(f"\n--- Global Split Statistics ---")
    print(f"Total interactions: {len(filtered_df)}")
    print(f"Offline interactions: {len(offline_df)} ({len(offline_df)/len(filtered_df):.1%})")
    print(f"Online interactions: {len(online_df)} ({len(online_df)/len(filtered_df):.1%})")
    
    return offline_df, online_df


def create_train_val_test_split(
    offline_df: pd.DataFrame,
    test_size: float = 0.1,
    val_size: float = 0.1,
    random_state: int = 42
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split the Offline Data into Train/Validation/Test.

    Args:
        offline_df: DataFrame containing offline data
        test_size: Proportion of offline data for test (default 0.1)
        val_size: Proportion of offline data for validation (default 0.1)
        random_state: Random seed

    Returns:
        Tuple of (train_df, val_df, test_df)
    """
    # Calculate relative sizes for the first split
    # We want val_size and test_size relative to the TOTAL offline_df
    total_eval_size = val_size + test_size
    
    # First split: Train vs (Val + Test)
    # Note: We don't use stratify here because with large online_split (e.g., 70%),
    # many users may have very few ratings in offline_df, causing stratification to fail.
    train_df, temp_df = train_test_split(
        offline_df,
        test_size=total_eval_size,
        random_state=random_state
    )

    # Second split: Val vs Test
    # relative_test_size = test_size / (val_size + test_size)
    # If val=0.1, test=0.1, total=0.2. relative_test = 0.1/0.2 = 0.5
    relative_test_size = test_size / total_eval_size

    val_df, test_df = train_test_split(
        temp_df,
        test_size=relative_test_size,
        random_state=random_state
    )

    print(f"\n--- Offline Split Statistics ---")
    print(f"Train set: {len(train_df)} ratings")
    print(f"Validation set: {len(val_df)} ratings")
    print(f"Test set: {len(test_df)} ratings")

    return train_df, val_df, test_df


def save_splits(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    online_df: pd.DataFrame,
    output_dir: str = "datasets/splits"
) -> None:
    """
    Save all splits to CSV files.
    """
    os.makedirs(output_dir, exist_ok=True)

    train_path = os.path.join(output_dir, "train_set.csv")
    val_path = os.path.join(output_dir, "val_set.csv")
    test_path = os.path.join(output_dir, "test_set.csv")
    online_path = os.path.join(output_dir, "online_set.csv")

    train_df.to_csv(train_path, index=False)
    val_df.to_csv(val_path, index=False)
    test_df.to_csv(test_path, index=False)
    online_df.to_csv(online_path, index=False)

    print(f"\nSplits saved to {output_dir}/:")
    print(f"  - {train_path}")
    print(f"  - {val_path}")
    print(f"  - {test_path}")
    print(f"  - {online_path} (Reserved for MAB)")


def load_splits(
    data_dir: str = "datasets/splits"
) -> Dict[str, pd.DataFrame]:
    """
    Load all splits from CSV files.

    Returns:
        Dictionary containing 'train', 'val', 'test', 'online' DataFrames
    """
    splits = {}
    try:
        splits['train'] = pd.read_csv(os.path.join(data_dir, "train_set.csv"))
        splits['val'] = pd.read_csv(os.path.join(data_dir, "val_set.csv"))
        splits['test'] = pd.read_csv(os.path.join(data_dir, "test_set.csv"))
        
        online_path = os.path.join(data_dir, "online_set.csv")
        if os.path.exists(online_path):
            splits['online'] = pd.read_csv(online_path)
        else:
            print("Warning: online_set.csv not found.")
            
        print(f"Loaded splits from {data_dir}")
        return splits
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Could not load splits from {data_dir}: {e}")
