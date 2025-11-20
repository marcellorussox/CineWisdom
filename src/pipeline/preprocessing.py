"""
Preprocessing Pipeline - High-level API for data preprocessing.

Questo modulo fornisce un'API di alto livello per tutto il preprocessing dei dati
senza dover gestire manualmente ogni passaggio.

Esempio d'uso:
```python
from src.pipeline.preprocessing import PreprocessingPipeline

# Inizializza pipeline
pipeline = PreprocessingPipeline(
    data_path='/path/to/raw/data',
    output_path='/path/to/output'
)

# Esegui preprocessing completo
data = pipeline.run_full_pipeline(
    enrich_dbpedia=True,
    normalize=True,
    compress=True,
    n_components=4096
)

# Accedi ai dati preprocessati
ratings_df = data['ratings_df']
movies_df = data['movies_df']
links_df = data['links_df']
cleaned_df = data['cleaned_df']
cosine_sim_matrix = data['cosine_sim_matrix']
movie_ids_series = data['movie_ids_series']
unique_movie_catalog = data['unique_movie_catalog']
```
"""

from __future__ import annotations

import os
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

from src.data.manager import (
    load_data,
    join_dataframes,
    enrich_movies,
    clean_partial_rows,
    drop_columns,
    normalize_movie_data_parallel,
    compress_kbrs_dataset,
)


@dataclass
class PreprocessingConfig:
    """Configuration for preprocessing pipeline."""
    # Data paths
    data_path: str = "datasets/raw"
    output_path: str = "datasets/processed"

    # Processing flags
    enrich_dbpedia: bool = True
    normalize: bool = True
    compress: bool = True

    # Compression settings
    n_components: int = 4096
    chunk_size: int = 200
    max_features_per_category: int = 40000

    # Cache settings
    use_cache: bool = True
    overwrite_cache: bool = False


class PreprocessingPipeline:
    """
    High-level API for complete data preprocessing pipeline.

    Gestisce automaticamente:
    - Caricamento dati raw
    - Enrichment DBpedia
    - Pulizia dati
    - Normalizzazione
    - Compressione SVD
    - Calcolo matrice similarità
    """

    def __init__(self, config: Optional[PreprocessingConfig] = None):
        """
        Initialize preprocessing pipeline.

        Args:
            config: PreprocessingConfig (se None, usa default)
        """
        self.config = config or PreprocessingConfig()

        # Ensure output directory exists
        os.makedirs(self.config.output_path, exist_ok=True)

    def load_raw_data(self) -> Dict[str, pd.DataFrame]:
        """
        Load raw data from CSV files.

        Returns:
            Dictionary con:
                - 'ratings_df': DataFrame ratings
                - 'movies_df': DataFrame movies
                - 'links_df': DataFrame links
        """
        print("📥 Loading raw data...")

        ratings_df, movies_df, links_df = load_data()

        if movies_df is None or links_df is None or ratings_df is None:
            raise ValueError("Failed to load raw data")

        data = {
            'ratings_df': ratings_df,
            'movies_df': movies_df,
            'links_df': links_df,
        }

        print(f"✅ Loaded: movies={movies_df.shape}, links={links_df.shape}, ratings={ratings_df.shape}")

        return data

    def create_unique_catalog(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Create unique movie catalog.

        Args:
            data: Dictionary with 'movies_df' and 'links_df'

        Returns:
            DataFrame with unique movie catalog
        """
        print("\n🔗 Creating unique catalog...")

        unique_movie_catalog = join_dataframes(
            data['movies_df'],
            data['links_df']
        )

        # Clean up
        unique_movie_catalog = unique_movie_catalog.drop(columns=['tmdbId'], errors='ignore')
        unique_movie_catalog['imdbId'] = unique_movie_catalog['imdbId'].apply(
            lambda x: f"tt{int(x):07d}" if pd.notna(x) else x
        )

        print(f"✅ Unique catalog created: {unique_movie_catalog.shape}")

        return unique_movie_catalog

    def enrich_catalog(self, unique_movie_catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich catalog with DBpedia data.

        Args:
            unique_movie_catalog: DataFrame with movie catalog

        Returns:
            DataFrame with enriched catalog
        """
        if not self.config.enrich_dbpedia:
            print("\n⏭️ Skipping DBpedia enrichment (disabled)")
            return unique_movie_catalog

        print("\n🔍 Enriching with DBpedia data...")

        enriched_movie_catalog = enrich_movies(unique_movie_catalog)

        print(f"✅ Enrichment completed: {enriched_movie_catalog.shape}")

        return enriched_movie_catalog

    def drop_columns_before_cleaning(self, enriched_catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Drop columns that cause excessive cleaning (as per notebook original).

        Args:
            enriched_catalog: DataFrame with enriched catalog

        Returns:
            DataFrame with specific columns dropped
        """
        print("\n🗑️ Dropping columns before cleaning (notebook-compatible)...")

        # Drop columns that cause excessive data loss
        columns_to_drop = ['title', 'imdbId', 'dbpediaAbstract', 'wikidataId']
        cleaned_catalog = drop_columns(enriched_catalog, columns_to_drop)

        print(f"✅ Dropped columns: {columns_to_drop}")
        print(f"   Shape after drop: {cleaned_catalog.shape}")

        return cleaned_catalog

    def clean_data(self, enriched_catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data by removing rows with missing values.

        Args:
            enriched_catalog: DataFrame with enriched catalog

        Returns:
            DataFrame with cleaned data
        """
        print("\n🧹 Cleaning data...")

        cleaned_df = clean_partial_rows(enriched_catalog)

        print(f"✅ Data cleaned: {cleaned_df.shape}")

        return cleaned_df

    def normalize_data(self, cleaned_df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize data with one-hot encoding and scaling.

        Args:
            cleaned_df: DataFrame with cleaned data

        Returns:
            DataFrame with normalized features
        """
        if not self.config.normalize:
            print("\n⏭️ Skipping normalization (disabled)")
            return cleaned_df

        print("\n⚙️ Normalizing data...")

        output_path = os.path.join(self.config.output_path, "normalized_movies.csv")

        normalized_data = normalize_movie_data_parallel(
            cleaned_df,
            output_path=output_path,
            chunk_size=self.config.chunk_size,
            max_features_per_category=self.config.max_features_per_category
        )

        print(f"✅ Normalization completed: {normalized_data.shape}")

        return normalized_data

    def compress_data(self, normalized_data: pd.DataFrame) -> pd.DataFrame:
        """
        Compress data with TruncatedSVD.

        Args:
            normalized_data: DataFrame with normalized features

        Returns:
            DataFrame with compressed features
        """
        if not self.config.compress:
            print("\n⏭️ Skipping compression (disabled)")
            return normalized_data

        print("\n🗜️ Compressing data with SVD...")

        compressed_df = compress_kbrs_dataset(
            normalized_data,
            n_components=self.config.n_components
        )

        print(f"✅ Compression completed: {compressed_df.shape}")

        return compressed_df

    def compute_similarity_matrix(self, compressed_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Compute cosine similarity matrix.

        Args:
            compressed_df: DataFrame with compressed features

        Returns:
            Tuple of (cosine_sim_matrix, movie_ids_series)
        """
        print("\n📐 Computing similarity matrix...")

        features_df = compressed_df.iloc[:, 1:]
        cosine_sim_matrix = cosine_similarity(features_df)
        movie_ids_series = compressed_df['movieId']

        print(f"✅ Similarity matrix computed: {cosine_sim_matrix.shape}")

        return cosine_sim_matrix, movie_ids_series

    def run_full_pipeline(self) -> Dict[str, any]:
        """
        Run complete preprocessing pipeline.

        Returns:
            Dictionary with all processed data:
                - 'ratings_df': Raw ratings DataFrame
                - 'movies_df': Raw movies DataFrame
                - 'links_df': Raw links DataFrame
                - 'unique_movie_catalog': Catalog with metadata
                - 'cleaned_df': Cleaned data
                - 'normalized_data': Normalized features
                - 'compressed_df': Compressed features
                - 'cosine_sim_matrix': Similarity matrix
                - 'movie_ids_series': Movie IDs
        """
        print("🚀 Starting preprocessing pipeline...")
        print("=" * 80)

        # Step 1: Load raw data
        data = self.load_raw_data()

        # Step 2: Create unique catalog
        unique_movie_catalog = self.create_unique_catalog(data)

        # Step 3: Enrich catalog
        enriched_catalog = self.enrich_catalog(unique_movie_catalog)

        # Step 3.5: Drop columns (CRITICAL FIX - replicate notebook exact logic)
        enriched_catalog = self.drop_columns_before_cleaning(enriched_catalog)

        # Step 4: Clean data
        cleaned_df = self.clean_data(enriched_catalog)

        # Step 5: Normalize data
        normalized_data = self.normalize_data(cleaned_df)

        # Step 6: Compress data
        compressed_df = self.compress_data(normalized_data)

        # Step 7: Compute similarity matrix
        cosine_sim_matrix, movie_ids_series = self.compute_similarity_matrix(compressed_df)

        # Compile results
        results = {
            'ratings_df': data['ratings_df'],
            'movies_df': data['movies_df'],
            'links_df': data['links_df'],
            'unique_movie_catalog': unique_movie_catalog,
            'cleaned_df': cleaned_df,
            'normalized_data': normalized_data,
            'compressed_df': compressed_df,
            'cosine_sim_matrix': cosine_sim_matrix,
            'movie_ids_series': movie_ids_series,
        }

        print("\n" + "=" * 80)
        print("✅ Preprocessing pipeline completed!")
        self.print_summary(results)

        return results

    def print_summary(self, data: Dict[str, any]) -> None:
        """Print preprocessing summary."""
        print("\n📊 PREPROCESSING SUMMARY:")
        print(f"  • Raw data:")
        print(f"    - Movies: {data['movies_df'].shape}")
        print(f"    - Ratings: {data['ratings_df'].shape}")
        print(f"    - Links: {data['links_df'].shape}")
        print(f"  • Processed data:")
        print(f"    - Unique catalog: {data['unique_movie_catalog'].shape}")
        print(f"    - Cleaned: {data['cleaned_df'].shape}")
        print(f"    - Normalized: {data['normalized_data'].shape}")
        print(f"    - Compressed: {data['compressed_df'].shape}")
        print(f"  • Similarity matrix: {data['cosine_sim_matrix'].shape}")

    def get_preprocessing_info(self, data: Dict[str, any]) -> Dict[str, str]:
        """Get formatted preprocessing info."""
        return {
            'data_shape': f"Movies: {data['movies_df'].shape}, Ratings: {data['ratings_df'].shape}",
            'cleaned_shape': f"{data['cleaned_df'].shape}",
            'compressed_shape': f"{data['compressed_df'].shape}",
            'similarity_shape': f"{data['cosine_sim_matrix'].shape}",
            'enrichment': 'Enabled' if self.config.enrich_dbpedia else 'Disabled',
            'compression': f"SVD ({self.config.n_components} components)" if self.config.compress else 'Disabled',
        }
