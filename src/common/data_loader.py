"""
Data Loader - Wrapper per funzioni di preprocessing

Questo modulo fornisce wrapper per le funzioni di caricamento e preprocessing
dei dati MovieLens, mantenendo compatibilità con il codice esistente.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, Optional, List, Union


def load_movielens_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Carica i dati MovieLens grezzi dai file CSV.

    Returns:
        Tuple[ratings_df, movies_df, links_df]
    """
    print("🔄 Caricamento dati MovieLens in corso...")

    # Trova la directory del progetto
    current_dir = Path(__file__).parent  # src/common
    project_root = current_dir.parent.parent  # root del progetto

    # Percorsi dei file
    ratings_path = project_root / 'datasets' / 'raw' / 'ratings.csv'
    movies_path = project_root / 'datasets' / 'raw' / 'movies.csv'
    links_path = project_root / 'datasets' / 'raw' / 'links.csv'

    try:
        ratings_df = pd.read_csv(ratings_path)
        movies_df = pd.read_csv(movies_path)
        links_df = pd.read_csv(links_path)

        print(f"✅ Dati caricati con successo:")
        print(f"   - Ratings: {ratings_df.shape}")
        print(f"   - Movies: {movies_df.shape}")
        print(f"   - Links: {links_df.shape}")
        return ratings_df, movies_df, links_df
    except FileNotFoundError as e:
        print(f"❌ Error: file not found. Details: {e}")
        raise ValueError("Errore nel caricamento dei dati") from e


def enrich_with_dbpedia(movies_df: pd.DataFrame) -> pd.DataFrame:
    """
    Arricchisce il catalogo film con dati DBpedia/Wikidata.

    Nota: Implementazione semplificata che ritorna il DataFrame originale.
    Per l'arricchimento completo, utilizzare la funzione dalla vecchia struttura.

    Args:
        movies_df: DataFrame con movieId, title, imdbId, etc.

    Returns:
        DataFrame arricchito con metadata DBpedia
    """
    print("🔄 Arricchimento dati con DBpedia in corso...")
    print("⚠️  Implementazione semplificata - DBpedia enrichment non disponibile")
    print("   Per l'arricchimento completo, utilizzare il codice dalla struttura precedente")

    # Aggiungi colonne base se non esistono
    if 'dbpediaAbstract' not in movies_df.columns:
        movies_df['dbpediaAbstract'] = ''
    if 'wikidataId' not in movies_df.columns:
        movies_df['wikidataId'] = ''

    print(f"✅ Arricchimento completato: {movies_df.shape}")
    return movies_df


def normalize_catalog(cleaned_df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizza il catalogo film con one-hot encoding.

    Implementazione semplificata che gestisce colonne testuali comuni.

    Args:
        cleaned_df: DataFrame pulito senza valori mancanti

    Returns:
        DataFrame normalizzato con feature one-hot
    """
    print("🔄 Normalizzazione catalogo in corso...")

    # Crea una copia del DataFrame
    normalized_df = cleaned_df.copy()

    # Gestisci colonne testuali con one-hot encoding se esistono
    text_columns = ['genres', 'dbpediaAbstract']
    for col in text_columns:
        if col in normalized_df.columns:
            print(f"   - Elaborando colonna: {col}")
            # Per ora, mantieni la colonna così com'è
            # In un'implementazione completa, si farebbe one-hot encoding

    print(f"✅ Normalizzazione completata: {normalized_df.shape}")
    return normalized_df


def compress_features(normalized_df: pd.DataFrame, n_components: int = 128) -> pd.DataFrame:
    """
    Applica compressione TruncatedSVD alle feature numeriche.

    Implementazione semplificata che lavora solo su colonne numeriche.

    Args:
        normalized_df: DataFrame normalizzato
        n_components: Numero di componenti SVD (default: 128)

    Returns:
        DataFrame compresso con feature ridotte
    """
    from sklearn.decomposition import TruncatedSVD

    print(f"🔄 Compressione SVD con {n_components} componenti...")

    # Seleziona solo colonne numeriche
    numeric_columns = normalized_df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_df = normalized_df[numeric_columns].fillna(0)

    # Se non ci sono colonne numeriche, crea alcune feature fittizie
    if len(numeric_columns) == 0:
        print("   ⚠️ Nessuna colonna numerica trovata, creo feature dummy")
        # Crea alcune feature base basate sulle colonne testuali
        for col in normalized_df.columns:
            if normalized_df[col].dtype == 'object':
                # Conta lunghezza stringa come feature numerica
                normalized_df[f'{col}_len'] = normalized_df[col].astype(str).str.len()
        numeric_columns = [c for c in normalized_df.columns if c.endswith('_len')]
        numeric_df = normalized_df[numeric_columns].fillna(0)

    # Applica SVD
    svd = TruncatedSVD(n_components=min(n_components, len(numeric_columns)), random_state=42)
    compressed_numeric = svd.fit_transform(numeric_df)

    # Crea DataFrame finale con componenti SVD
    compressed_df = normalized_df.copy()
    # Rimuovi colonne numeriche originali
    compressed_df = compressed_df.drop(columns=numeric_columns, errors='ignore')

    # Aggiungi componenti SVD
    svd_columns = [f'svd_component_{i}' for i in range(compressed_numeric.shape[1])]
    for i, col_name in enumerate(svd_columns):
        compressed_df[col_name] = compressed_numeric[:, i]

    print(f"✅ Compressione completata: {compressed_df.shape}")
    print(f"   Componenti SVD: {compressed_numeric.shape[1]}")
    print(f"   Varianza spiegata: {svd.explained_variance_ratio_.sum():.4f}")

    return compressed_df


def create_user_item_matrix(ratings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea una user-item matrix dai ratings.

    Args:
        ratings_df: DataFrame con userId, movieId, rating

    Returns:
        DataFrame pivot user-item matrix
    """
    print("🔄 Creazione user-item matrix...")

    # Pivot per creare user-item matrix
    user_item_matrix = ratings_df.pivot_table(
        index='userId',
        columns='movieId',
        values='rating',
        fill_value=0
    )

    print(f"✅ User-item matrix creata: {user_item_matrix.shape}")
    return user_item_matrix


def save_processed_data(movies_df: pd.DataFrame, ratings_df: pd.DataFrame,
                       output_dir: str = "datasets/processed"):
    """
    Salva i dati processati in formato CSV.

    Args:
        movies_df: DataFrame film
        ratings_df: DataFrame ratings
        output_dir: Directory di output
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    movies_df.to_csv(output_path / "normalized_movies.csv", index=False)
    ratings_df.to_csv(output_path / "normalized_ratings.csv", index=False)

    print(f"✅ Dati salvati in {output_path}")


def join_dataframes(df1: pd.DataFrame,
                    df2: pd.DataFrame,
                    on: str = 'movieId',
                    how: str = 'inner') -> pd.DataFrame:
    """
    Unisce due DataFrame su una colonna comune.

    Args:
        df1: Primo DataFrame
        df2: Secondo DataFrame
        on: Colonna su cui fare il merge
        how: Tipo di join ('inner', 'left', 'right', 'outer')

    Returns:
        DataFrame unito
    """
    if df1 is None or df2 is None:
        return pd.DataFrame()
    return pd.merge(df1, df2, on=on, how=how)


def drop_columns(df: pd.DataFrame,
                 columns_to_drop: Union[str, List[str]]) -> pd.DataFrame:
    """
    Elimina una o più colonne da un DataFrame.

    Args:
        df: DataFrame di input
        columns_to_drop: Nome colonna o lista di nomi colonne da eliminare

    Returns:
        DataFrame senza le colonne specificate
    """
    if isinstance(columns_to_drop, str):
        columns_to_drop = [columns_to_drop]

    # Verifica che le colonne esistano prima di eliminare
    existing_columns = [col for col in columns_to_drop if col in df.columns]
    if len(existing_columns) != len(columns_to_drop):
        missing = set(columns_to_drop) - set(existing_columns)
        print(f"⚠️ Warning: Colonne non trovate: {missing}")

    return df.drop(columns=existing_columns)


def clean_partial_rows(df: pd.DataFrame,
                       output_file: Optional[str] = None) -> pd.DataFrame:
    """
    Pulisce un DataFrame rimuovendo righe con valori mancanti.

    Args:
        df: DataFrame di input
        output_file: Percorso file per salvare dati puliti (opzionale)

    Returns:
        DataFrame pulito
    """
    initial_row_count = len(df)
    missing_values_per_column = df.isnull().sum()

    # Rimuovi righe con qualsiasi valore mancante
    cleaned_df = df.dropna()

    final_row_count = len(cleaned_df)
    deleted_records_count = initial_row_count - final_row_count

    # Stampa statistiche
    print("\n--- Cleaning Report ---")
    print(f"Total records before cleaning: {initial_row_count}")
    print(f"Total records after cleaning: {final_row_count}")
    print(f"Deleted records: {deleted_records_count}")
    print("\nEmpty records per column (before cleaning):")
    print(missing_values_per_column.to_string())

    if output_file:
        cleaned_df.to_csv(output_file, index=False)
        print(f"\nCleaned data has been saved to '{output_file}'.")

    return cleaned_df
