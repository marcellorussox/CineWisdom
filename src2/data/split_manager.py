"""
Split Manager - Traditional ML Split (80/10/10)

Gestisce la divisione del dataset in train/validation/test sets
con approccio stratificato per evitare data leakage.
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from typing import Tuple, Dict
import os
from pathlib import Path


class SplitManager:
    """Manager per la gestione degli split del dataset."""

    def __init__(self, ratings_path: str, movies_path: str, output_dir: str = "datasets/splits"):
        """
        Inizializza il SplitManager.

        Args:
            ratings_path: Percorso al file ratings.csv
            movies_path: Percorso al file normalized_movies.csv
            output_dir: Directory di output per i splits
        """
        self.ratings_path = ratings_path
        self.movies_path = movies_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.valid_movie_ids = None
        self.ratings_df = None

    def load_data(self) -> pd.DataFrame:
        """
        Carica i dati e filtra i film validi.

        Returns:
            DataFrame con i ratings filtrati
        """
        print("Loading ratings data...")
        self.ratings_df = pd.read_csv(self.ratings_path)

        print(f"Ratings loaded: {len(self.ratings_df)} records")
        print(f"Unique users: {self.ratings_df['userId'].nunique()}")
        print(f"Unique movies: {self.ratings_df['movieId'].nunique()}")

        print("\nLoading valid movie IDs from normalized_movies.csv...")
        valid_movies = pd.read_csv(self.movies_path)
        self.valid_movie_ids = set(valid_movies['movieId'].unique())

        print(f"Valid movies in catalog: {len(self.valid_movie_ids)}")

        # Filtra ratings per includere solo film validi
        filtered_ratings = self.ratings_df[
            self.ratings_df['movieId'].isin(self.valid_movie_ids)
        ].copy()

        print(f"\nFiltered ratings: {len(filtered_ratings)} records")
        print(f"Unique users after filtering: {filtered_ratings['userId'].nunique()}")
        print(f"Unique movies after filtering: {filtered_ratings['movieId'].nunique()}")

        return filtered_ratings

    def create_traditional_split(
        self,
        test_size: float = 0.2,
        val_size: float = 0.1,
        random_state: int = 42,
        min_ratings_per_user: int = 5
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Crea split train/validation/test stratificato.

        Args:
            test_size: Dimensione del test set (default: 0.2 per 80/10/10)
            val_size: Dimensione del validation set (default: 0.1)
            random_state: Seed per riproducibilità
            min_ratings_per_user: Minimo numero di ratings per utente

        Returns:
            Tuple di (train, validation, test) DataFrames
        """
        print("\n" + "="*60)
        print("CREATING TRADITIONAL SPLIT (80/10/10)")
        print("="*60)

        # Carica dati
        if self.ratings_df is None:
            df = self.load_data()
        else:
            df = self.ratings_df.copy()

        # Filtra utenti con pochi ratings
        user_counts = df['userId'].value_counts()
        valid_users = user_counts[user_counts >= min_ratings_per_user].index

        print(f"\nFiltering users with at least {min_ratings_per_user} ratings...")
        print(f"Users with ≥{min_ratings_per_user} ratings: {len(valid_users)}")
        print(f"Users filtered out: {user_counts[user_counts < min_ratings_per_user].sum()} ratings")

        df_filtered = df[df['userId'].isin(valid_users)].copy()
        print(f"Total ratings after user filtering: {len(df_filtered)}")

        # Stratified split basato sugli userId
        # Prima split: train (80%) vs temp (20%)
        train_users, temp_users = train_test_split(
            valid_users,
            test_size=(test_size + val_size),
            random_state=random_state,
            shuffle=True
        )

        print(f"\nTrain users: {len(train_users)}")
        print(f"Temp users (for val+test): {len(temp_users)}")

        # Second split: validation (10%) vs test (10%)
        val_users, test_users = train_test_split(
            temp_users,
            test_size=val_size / (test_size + val_size),
            random_state=random_state,
            shuffle=True
        )

        print(f"Validation users: {len(val_users)}")
        print(f"Test users: {len(test_users)}")

        # Crea i dataset
        train_df = df_filtered[df_filtered['userId'].isin(train_users)].copy()
        val_df = df_filtered[df_filtered['userId'].isin(val_users)].copy()
        test_df = df_filtered[df_filtered['userId'].isin(test_users)].copy()

        print("\n" + "-"*60)
        print("SPLIT STATISTICS")
        print("-"*60)
        print(f"Train set: {len(train_df)} ratings ({len(train_df)/len(df_filtered)*100:.1f}%)")
        print(f"Validation set: {len(val_df)} ratings ({len(val_df)/len(df_filtered)*100:.1f}%)")
        print(f"Test set: {len(test_df)} ratings ({len(test_df)/len(df_filtered)*100:.1f}%)")

        # Verifica distribuzione rating
        print("\n" + "-"*60)
        print("RATING DISTRIBUTION")
        print("-"*60)

        for name, split_df in [("Train", train_df), ("Validation", val_df), ("Test", test_df)]:
            rating_dist = split_df['rating'].value_counts().sort_index()
            print(f"\n{name}:")
            for rating, count in rating_dist.items():
                pct = count / len(split_df) * 100
                print(f"  Rating {rating}: {count:6d} ({pct:5.1f}%)")

        # Verifica overlap (dovrebbe essere 0)
        train_users_set = set(train_users)
        val_users_set = set(val_users)
        test_users_set = set(test_users)

        overlap = len(train_users_set & val_users_set | train_users_set & test_users_set | val_users_set & test_users_set)
        print(f"\n{'✓' if overlap == 0 else '✗'} User overlap across splits: {overlap} (should be 0)")

        return train_df, val_df, test_df

    def save_splits(
        self,
        train_df: pd.DataFrame,
        val_df: pd.DataFrame,
        test_df: pd.DataFrame,
        suffix: str = ""
    ) -> Dict[str, str]:
        """
        Salva i split su file CSV.

        Args:
            train_df: Training set
            val_df: Validation set
            test_df: Test set
            suffix: Suffisso opzionale per i nomi file

        Returns:
            Dict con i percorsi dei file salvati
        """
        print("\n" + "="*60)
        print("SAVING SPLITS")
        print("="*60)

        file_paths = {}

        for name, df in [("train", train_df), ("val", val_df), ("test", test_df)]:
            filename = f"{name}_set{suffix}.csv"
            filepath = self.output_dir / filename

            df.to_csv(filepath, index=False)
            file_paths[name] = str(filepath)

            print(f"{name.capitalize():10s} saved: {filepath} ({len(df):6d} records)")

        print(f"\nAll splits saved in: {self.output_dir}")

        # Salva anche metadati
        metadata = {
            "train_size": len(train_df),
            "val_size": len(val_df),
            "test_size": len(test_df),
            "total_size": len(train_df) + len(val_df) + len(test_df),
            "unique_users": len(set(train_df['userId']) | set(val_df['userId']) | set(test_df['userId'])),
            "unique_movies": len(set(train_df['movieId']) | set(val_df['movieId']) | set(test_df['movieId'])),
            "train_pct": len(train_df) / (len(train_df) + len(val_df) + len(test_df)) * 100,
            "val_pct": len(val_df) / (len(train_df) + len(val_df) + len(test_df)) * 100,
            "test_pct": len(test_df) / (len(train_df) + len(val_df) + len(test_df)) * 100
        }

        metadata_path = self.output_dir / f"split_metadata{suffix}.json"
        import json
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        print(f"Metadata saved: {metadata_path}")

        return file_paths

    def create_complete_split(self) -> Dict[str, str]:
        """
        Esegue il processo completo: load, split, save.

        Returns:
            Dict con i percorsi dei file salvati
        """
        print("\n" + "="*70)
        print("TRADITIONAL SPLIT MANAGER - COMPLETE WORKFLOW")
        print("="*70)

        # Crea split
        train_df, val_df, test_df = self.create_traditional_split(
            test_size=0.2,
            val_size=0.1,
            random_state=42,
            min_ratings_per_user=5
        )

        # Salva
        file_paths = self.save_splits(train_df, val_df, test_df)

        print("\n" + "="*70)
        print("✓ SPLIT COMPLETED SUCCESSFULLY")
        print("="*70)

        return file_paths


def create_traditional_split():
    """
    Funzione principale per creare lo split tradizionale.

    Usage:
        from src.data.split_manager import create_traditional_split
        file_paths = create_traditional_split()
    """
    # Percorsi dei file
    ratings_path = "datasets/raw/ratings.csv"
    movies_path = "datasets/processed/normalized_movies.csv"
    output_dir = "datasets/splits"

    # Crea manager e split
    manager = SplitManager(
        ratings_path=ratings_path,
        movies_path=movies_path,
        output_dir=output_dir
    )

    # Esegui split completo
    file_paths = manager.create_complete_split()

    return file_paths


if __name__ == "__main__":
    # Esegui se chiamato direttamente
    create_traditional_split()
