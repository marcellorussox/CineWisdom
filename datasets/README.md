# 📊 Datasets Directory

This directory contains the MovieLens datasets used by CineWisdom.

## Structure

```
datasets/
└── ml-small-100k/
    ├── raw/                    # Original MovieLens files
    │   ├── ratings.csv        # User ratings (100,836 ratings)
    │   ├── movies.csv         # Movie metadata (9,742 movies)
    │   └── links.csv          # IMDb/TMDb links
    │
    ├── processed/             # Enriched and preprocessed data
    │   ├── movies_enriched.csv      # DBpedia/Wikidata enriched
    │   └── normalized_movies.csv    # Feature-engineered movies
    │
    └── splits/                # Train/validation/test/online splits
        ├── train.csv          # Training set (64%)
        ├── val.csv            # Validation set (8%)
        ├── test.csv           # Test set (8%)
        └── online.csv         # Online simulation set (20%)
```

## Dataset Information

### MovieLens 100K

- **Source**: [GroupLens Research](https://grouplens.org/datasets/movielens/)
- **Size**: 100,836 ratings
- **Users**: 610
- **Movies**: 9,742
- **Rating Scale**: 0.5 - 5.0 (half-star increments)
- **Sparsity**: 98.3%

### Data Files

#### `raw/ratings.csv`
User ratings with columns:
- `userId`: User ID
- `movieId`: Movie ID  
- `rating`: Rating (0.5-5.0)
- `timestamp`: Unix timestamp

#### `raw/movies.csv`
Movie metadata with columns:
- `movieId`: Movie ID
- `title`: Movie title with year
- `genres`: Pipe-separated genres

#### `raw/links.csv`
External links with columns:
- `movieId`: Movie ID
- `imdbId`: IMDb ID
- `tmdbId`: TMDb ID

#### `processed/movies_enriched.csv`
Enriched with DBpedia/Wikidata:
- `dbpediaDirector`: Directors (list)
- `dbpediaActors`: Actors (list)
- `dbpediaAbstract`: Movie description
- `dbpediaGenre`: Genres from DBpedia
- `dbpediaRuntime`: Runtime in minutes
- Additional semantic metadata

#### `processed/normalized_movies.csv`
Feature-engineered for KBRS:
- One-hot encoded genres (~20 columns)
- One-hot encoded directors (~500 columns)
- One-hot encoded actors (~1000 columns)
- Normalized numerical features
- **Total**: ~2000 features per movie

## Usage

### Loading Data

```python
from src.data.manager import load_data

ratings_df, movies_df, links_df = load_data(data_dir='datasets/ml-small-100k/raw')
```

### Loading Splits

```python
import pandas as pd

train_df = pd.read_csv('datasets/splits/train.csv')
val_df = pd.read_csv('datasets/splits/val.csv')
test_df = pd.read_csv('datasets/splits/test.csv')
online_df = pd.read_csv('datasets/splits/online.csv')
```

## Data Splits

### Offline/Online Split (80/20)

- **Offline**: 80,668 ratings → Used for train/val/test
- **Online**: 20,168 ratings → Used for MAB simulation

**Method**: Timestamp-based (chronological)

### Train/Val/Test Split

From offline set:
- **Train**: 64,534 ratings (80% of offline)
- **Val**: 8,067 ratings (10% of offline)
- **Test**: 8,067 ratings (10% of offline)

**Method**: Stratified random (ensures user coverage)

## Notes

- All splits are **non-overlapping** (no data leakage)
- Enrichment requires **internet connection** for SPARQL queries
- Normalized data is **cached** to avoid recomputation
- File sizes: raw (~5MB), enriched (~35MB), normalized (~35MB)
