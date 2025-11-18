"""
Utils - Funzioni di utilità comuni

Contiene funzioni di utilità utilizzate in entrambe le pipeline.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from collections import Counter
from pathlib import Path


def filter_users_by_rating_count(ratings_df: pd.DataFrame,
                                 min_ratings: int = 5) -> pd.DataFrame:
    """
    Filtra utenti con un numero minimo di rating.

    Args:
        ratings_df: DataFrame con rating
        min_ratings: Numero minimo di rating per utente

    Returns:
        DataFrame filtrato
    """
    print(f"🔄 Filtraggio utenti (min {min_ratings} rating)...")

    user_counts = ratings_df['userId'].value_counts()
    valid_users = user_counts[user_counts >= min_ratings].index

    filtered_df = ratings_df[ratings_df['userId'].isin(valid_users)]

    print(f"✅ Filtraggio completato:")
    print(f"   - Utenti originali: {ratings_df['userId'].nunique()}")
    print(f"   - Utenti dopo filtro: {filtered_df['userId'].nunique()}")
    print(f"   - Rating originali: {len(ratings_df)}")
    print(f"   - Rating dopo filtro: {len(filtered_df)}")

    return filtered_df


def get_timestamp_sorted_ratings(ratings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ordina i rating per timestamp.

    Args:
        ratings_df: DataFrame con rating

    Returns:
        DataFrame ordinato per timestamp
    """
    print("🔄 Ordinamento rating per timestamp...")

    if 'timestamp' not in ratings_df.columns:
        print("⚠️ Colonna 'timestamp' non trovata")
        return ratings_df

    sorted_df = ratings_df.sort_values('timestamp').reset_index(drop=True)

    print(f"✅ Ordinamento completato:")
    print(f"   - Prima: {ratings_df['timestamp'].min()}")
    print(f"   - Dopo: {sorted_df['timestamp'].min()}")
    print(f"   - Ultima: {sorted_df['timestamp'].max()}")

    return sorted_df


def compute_popularity_baseline(ratings_df: pd.DataFrame,
                               n_recommendations: int = 10) -> Dict[int, float]:
    """
    Calcola baseline popularity (film più popolari).

    Args:
        ratings_df: DataFrame con rating
        n_recommendations: Numero di raccomandazioni baseline

    Returns:
        Dict {movieId: popularity_score}
    """
    print(f"🔄 Calcolo baseline popularity...")

    # Conta occorrenze di ogni film
    movie_counts = ratings_df['movieId'].value_counts()

    # Normalizza i punteggi
    max_count = movie_counts.max()
    popularity_scores = (movie_counts / max_count).to_dict()

    # Seleziona top-N
    top_movies = movie_counts.head(n_recommendations)

    print(f"✅ Baseline popularity calcolata:")
    print(f"   - Film totali: {len(movie_counts)}")
    print(f"   - Top {n_recommendations} film selezionati")

    return {mid: popularity_scores[mid] for mid in top_movies.index}


def get_user_history(ratings_df: pd.DataFrame,
                    user_id: int,
                    before_timestamp: Optional[int] = None) -> pd.DataFrame:
    """
    Recupera la storia rating di un utente.

    Args:
        ratings_df: DataFrame con rating
        user_id: ID dell'utente
        before_timestamp: Filtra solo rating prima di questo timestamp

    Returns:
        DataFrame con storia utente
    """
    user_ratings = ratings_df[ratings_df['userId'] == user_id]

    if before_timestamp is not None:
        user_ratings = user_ratings[user_ratings['timestamp'] < before_timestamp]

    return user_ratings.sort_values('timestamp')


def compute_global_statistics(ratings_df: pd.DataFrame) -> Dict:
    """
    Calcola statistiche globali del dataset.

    Args:
        ratings_df: DataFrame con rating

    Returns:
        Dict con statistiche
    """
    print("🔄 Calcolo statistiche globali...")

    stats = {
        'num_users': ratings_df['userId'].nunique(),
        'num_movies': ratings_df['movieId'].nunique(),
        'num_ratings': len(ratings_df),
        'density': len(ratings_df) / (ratings_df['userId'].nunique() * ratings_df['movieId'].nunique()),
        'avg_ratings_per_user': len(ratings_df) / ratings_df['userId'].nunique(),
        'avg_ratings_per_movie': len(ratings_df) / ratings_df['movieId'].nunique(),
        'rating_distribution': ratings_df['rating'].value_counts().to_dict(),
        'timestamp_range': {
            'min': ratings_df['timestamp'].min(),
            'max': ratings_df['timestamp'].max()
        }
    }

    print("✅ Statistiche globali:")
    print(f"   - Utenti: {stats['num_users']}")
    print(f"   - Film: {stats['num_movies']}")
    print(f"   - Rating: {stats['num_ratings']}")
    print(f"   - Densità: {stats['density']:.6f}")
    print(f"   - Rating medi per utente: {stats['avg_ratings_per_user']:.2f}")
    print(f"   - Rating medi per film: {stats['avg_ratings_per_movie']:.2f}")

    return stats


def create_data_splits(ratings_df: pd.DataFrame,
                      test_size: float = 0.1,
                      val_size: float = 0.1,
                      random_state: int = 42,
                      min_ratings_per_user: int = 5) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Crea split train/val/test usando sklearn.

    Args:
        ratings_df: DataFrame con rating
        test_size: Proporzione test set
        val_size: Proporzione validation set
        random_state: Seed per riproducibilità
        min_ratings_per_user: Numero minimo di rating per utente

    Returns:
        Tuple (train_df, val_df, test_df)
    """
    from sklearn.model_selection import train_test_split

    print(f"🔄 Creazione split (test={test_size:.1%}, val={val_size:.1%})...")

    # Filtra utenti con pochi rating
    user_counts = ratings_df['userId'].value_counts()
    valid_users = user_counts[user_counts >= min_ratings_per_user].index
    filtered_df = ratings_df[ratings_df['userId'].isin(valid_users)]

    print(f"   - Utenti filtrati: {ratings_df['userId'].nunique()} → {filtered_df['userId'].nunique()}")

    # Primo split: train + temp (val+test)
    val_test_size = val_size + test_size

    train_df, temp_df = train_test_split(
        filtered_df,
        test_size=val_test_size,
        stratify=filtered_df['userId'],
        random_state=random_state
    )

    # Secondo split: val + test dal temp
    relative_test_size = test_size / val_test_size

    val_df, test_df = train_test_split(
        temp_df,
        test_size=relative_test_size,
        stratify=temp_df['userId'],
        random_state=random_state
    )

    print("✅ Split creati:")
    print(f"   - Train: {len(train_df)} ({len(train_df)/len(ratings_df):.1%})")
    print(f"   - Val:   {len(val_df)} ({len(val_df)/len(ratings_df):.1%})")
    print(f"   - Test:  {len(test_df)} ({len(test_df)/len(ratings_df):.1%})")

    return train_df, val_df, test_df


def save_splits(train_df: pd.DataFrame,
                val_df: pd.DataFrame,
                test_df: pd.DataFrame,
                output_dir: str):
    """
    Salva gli split in file CSV separati.

    Args:
        train_df: DataFrame train
        val_df: DataFrame validation
        test_df: DataFrame test
        output_dir: Directory di output
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    train_df.to_csv(output_path / "train_set.csv", index=False)
    val_df.to_csv(output_path / "val_set.csv", index=False)
    test_df.to_csv(output_path / "test_set.csv", index=False)

    print(f"✅ Split salvati in {output_dir}")


def get_split_statistics(train_df: pd.DataFrame,
                        val_df: pd.DataFrame,
                        test_df: pd.DataFrame) -> Dict:
    """
    Calcola statistiche degli split.

    Args:
        train_df: DataFrame train
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
        'validation': {
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

    return stats


def print_split_statistics(split_stats: Dict):
    """
    Stampa le statistiche degli split in formato leggibile.

    Args:
        split_stats: Dict con statistiche (output di get_split_statistics)
    """
    print("\n📊 Statistiche Split:")
    print("="*60)

    for split_name, stats in split_stats.items():
        print(f"\n{split_name.upper()}:")
        print(f"  - Rating: {stats['n_ratings']:,}")
        print(f"  - Utenti: {stats['n_users']:,}")
        print(f"  - Film: {stats['n_movies']:,}")
        print(f"  - Rating medio: {stats['avg_rating']:.2f}")

    print("="*60)


def save_results(results: Dict, output_path: str):
    """
    Salva risultati in formato JSON.

    Args:
        results: Dict con risultati
        output_path: Percorso file output
    """
    import json

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)

    print(f"✅ Risultati salvati in {output_path}")


def load_results(input_path: str) -> Dict:
    """
    Carica risultati da file JSON.

    Args:
        input_path: Percorso file input

    Returns:
        Dict con risultati
    """
    import json

    with open(input_path, 'r') as f:
        results = json.load(f)

    print(f"✅ Risultati caricati da {input_path}")

    return results


def validate_data(ratings_df: pd.DataFrame,
                 movies_df: pd.DataFrame) -> bool:
    """
    Valida la coerenza dei dati.

    Args:
        ratings_df: DataFrame rating
        movies_df: DataFrame film

    Returns:
        True se i dati sono validi
    """
    print("🔄 Validazione dati...")

    # Verifica che tutti i movieId nei rating esistano nel catalogo
    rating_movies = set(ratings_df['movieId'].unique())
    catalog_movies = set(movies_df['movieId'].unique())

    missing_movies = rating_movies - catalog_movies
    if missing_movies:
        print(f"⚠️ Warning: {len(missing_movies)} film nei rating non trovati nel catalogo")
        # Non è un errore critico, alcuni film potrebbero non avere metadata

    # Verifica rating nella fascia 0.5-5.0
    invalid_ratings = ratings_df[(ratings_df['rating'] < 0.5) | (ratings_df['rating'] > 5.0)]
    if len(invalid_ratings) > 0:
        print(f"❌ Error: {len(invalid_ratings)} rating fuori dalla fascia 0.5-5.0")
        return False

    # Verifica timestamp validi
    if ratings_df['timestamp'].min() < 0:
        print(f"❌ Error: Timestamp negativi trovati")
        return False

    print("✅ Validazione completata - Dati coerenti")

    return True
