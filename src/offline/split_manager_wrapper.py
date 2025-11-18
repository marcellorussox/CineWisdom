"""
Split Manager Wrapper - Wrapper per gestione split

Wrapper semplificato per SplitManager esistente.
"""

import pandas as pd
from typing import Tuple, Optional
from pathlib import Path


def create_train_val_test_split(ratings_df: pd.DataFrame,
                                test_size: float = 0.1,
                                val_size: float = 0.1,
                                random_state: int = 42,
                                min_ratings_per_user: int = 5) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Crea split train/validation/test stratificato.

    Wrapper per SplitManager.create_traditional_split()

    Args:
        ratings_df: DataFrame con rating
        test_size: Proporzione test set (default: 0.1)
        val_size: Proporzione validation set (default: 0.1)
        random_state: Seed per riproducibilità (default: 42)
        min_ratings_per_user: Minimo rating per utente (default: 5)

    Returns:
        Tuple (train_df, val_df, test_df)
    """
    from src.data.split_manager import SplitManager

    print(f"🔄 Creazione split:")
    print(f"   - Test size: {test_size:.1%}")
    print(f"   - Val size: {val_size:.1%}")
    print(f"   - Min ratings/user: {min_ratings_per_user}")
    print(f"   - Random state: {random_state}")

    split_manager = SplitManager()
    train_df, val_df, test_df = split_manager.create_traditional_split(
        ratings_df=ratings_df,
        test_size=test_size,
        val_size=val_size,
        random_state=random_state,
        min_ratings_per_user=min_ratings_per_user
    )

    print(f"✅ Split creati:")
    print(f"   - Train: {len(train_df)} rating")
    print(f"   - Validation: {len(val_df)} rating")
    print(f"   - Test: {len(test_df)} rating")

    return train_df, val_df, test_df


def save_splits(train_df: pd.DataFrame,
                val_df: pd.DataFrame,
                test_df: pd.DataFrame,
                output_dir: str = "datasets/splits"):
    """
    Salva gli split in file CSV.

    Wrapper per SplitManager.save_splits()

    Args:
        train_df: DataFrame training
        val_df: DataFrame validation
        test_df: DataFrame test
        output_dir: Directory di output
    """
    from src.data.split_manager import SplitManager

    print(f"🔄 Salvataggio split in {output_dir}...")

    split_manager = SplitManager()
    split_manager.save_splits(train_df, val_df, test_df, output_dir)

    print(f"✅ Split salvati in {output_dir}")


def load_splits(split_dir: str = "datasets/splits") -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Carica split salvati da file CSV.

    Args:
        split_dir: Directory contenente gli split

    Returns:
        Tuple (train_df, val_df, test_df)
    """
    split_path = Path(split_dir)

    train_df = pd.read_csv(split_path / "train_set.csv")
    val_df = pd.read_csv(split_path / "val_set.csv")
    test_df = pd.read_csv(split_path / "test_set.csv")

    print(f"✅ Split caricati da {split_dir}")
    print(f"   - Train: {len(train_df)}")
    print(f"   - Val: {len(val_df)}")
    print(f"   - Test: {len(test_df)}")

    return train_df, val_df, test_df


def get_split_statistics(train_df: pd.DataFrame,
                        val_df: pd.DataFrame,
                        test_df: pd.DataFrame) -> dict:
    """
    Calcola statistiche degli split.

    Args:
        train_df: DataFrame training
        val_df: DataFrame validation
        test_df: DataFrame test

    Returns:
        Dict con statistiche
    """
    stats = {
        'train': {
            'n_ratings': len(train_df),
            'n_users': train_df['userId'].nunique(),
            'n_movies': train_df['movieId'].nunique(),
            'avg_rating': train_df['rating'].mean()
        },
        'val': {
            'n_ratings': len(val_df),
            'n_users': val_df['userId'].nunique(),
            'n_movies': val_df['movieId'].nunique(),
            'avg_rating': val_df['rating'].mean()
        },
        'test': {
            'n_ratings': len(test_df),
            'n_users': test_df['userId'].nunique(),
            'n_movies': test_df['movieId'].nunique(),
            'avg_rating': test_df['rating'].mean()
        }
    }

    total_ratings = len(train_df) + len(val_df) + len(test_df)

    stats['split_percentages'] = {
        'train': len(train_df) / total_ratings * 100,
        'val': len(val_df) / total_ratings * 100,
        'test': len(test_df) / total_ratings * 100
    }

    return stats


def print_split_statistics(stats: dict):
    """
    Stampa statistiche degli split in formato leggibile.

    Args:
        stats: Dict con statistiche
    """
    print("\n" + "="*70)
    print("STATISTICHE SPLIT")
    print("="*70)

    for split_name, split_stats in stats.items():
        if split_name != 'split_percentages':
            print(f"\n📊 {split_name.upper()}:")
            print(f"   Rating: {split_stats['n_ratings']:,} ({stats['split_percentages'][split_name]:.1f}%)")
            print(f"   Utenti: {split_stats['n_users']:,}")
            print(f"   Film: {split_stats['n_movies']:,}")
            print(f"   Rating medio: {split_stats['avg_rating']:.2f}")

    print("="*70 + "\n")
