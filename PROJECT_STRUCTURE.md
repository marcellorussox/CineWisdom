# 📁 CineWisdom Project Structure

Detailed overview of the CineWisdom codebase organization.

---

## 🗂️ Directory Tree

```
CineWisdom/
├── 📄 kbrs_pipeline.py              # Main entry point - orchestrates full pipeline
├── 📄 requirements.txt              # Python dependencies
├── 📄 README.md                     # Project overview and quick start
├── 📄 TESTING.md                    # Complete testing documentation
├── 📄 PROJECT_STRUCTURE.md          # This file
├── 📄 TECHNICAL_DOCS.md             # Algorithm details and theory
│
├── 📁 src/                          # Source code (256KB, 17 files)
│   │
│   ├── 📁 recommender/              # KBRS recommendation engine
│   │   ├── kbrs.py                 # Core KBRS implementation (489 lines)
│   │   └── __init__.py
│   │
│   ├── 📁 simulation/               # Online learning simulation
│   │   ├── kbrs_simulator.py       # MAB online simulator (267 lines)
│   │   └── __init__.py
│   │
│   ├── 📁 evaluation/               # Metrics and evaluation
│   │   ├── kbrs_evaluator.py       # KBRS-specific evaluator (365 lines)
│   │   ├── metrics.py              # Generic metrics (RMSE, MAE, NDCG) (132 lines)
│   │   └── __init__.py
│   │
│   ├── 📁 data/                     # Data processing and enrichment
│   │   ├── manager.py              # Data loading & preprocessing (420 lines)
│   │   ├── split_manager.py        # Train/val/test splitting (115 lines)
│   │   ├── sparql.py               # DBpedia/Wikidata queries (170 lines)
│   │   ├── templates.py            # SPARQL query templates (58 lines)
│   │   ├── mapping.py              # Data mapping utilities (150 lines)
│   │   └── __init__.py
│   │
│   ├── 📁 viz/                      # Visualization tools
│   │   ├── plot_manager.py         # Plotting utilities (128 lines)
│   │   └── __init__.py
│   │
│   └── 📁 core/                     # Core utilities and config
│       ├── config.py               # Configuration classes (130 lines)
│       └── __init__.py
│
├── 📁 datasets/                     # Data storage
│   └── 📁 ml-small-100k/           # MovieLens 100K dataset
│       ├── 📁 raw/                 # Original CSVs
│       │   ├── ratings.csv         # User ratings (100K)
│       │   ├── movies.csv          # Movie metadata
│       │   └── links.csv           # IMDb/TMDb links
│       │
│       ├── 📁 processed/           # Enriched & normalized data
│       │   ├── movies_enriched.csv # DBpedia-enriched movies
│       │   └── normalized_movies.csv # Feature-engineered movies
│       │
│       └── 📁 splits/              # Train/val/test/online splits
│           ├── train.csv           # Training set (64%)
│           ├── val.csv             # Validation set (8%)
│           ├── test.csv            # Test set (8%)
│           └── online.csv          # Online simulation set (20%)
│
├── 📁 models/                       # Saved models and artifacts
│   └── 📁 kbrs/
│       └── 📁 ml-small-100k/
│           ├── cosine_sim_matrix.npy  # Precomputed similarity (724MB)
│           └── movie_ids.csv          # Movie ID mapping
│
├── 📁 results/                      # Experiment results
│   └── 📁 ml-small-100k/
│       └── 📁 semantic_mab/
│           ├── online_history.csv     # Full simulation log
│           ├── offline_evaluation.json # Offline metrics
│           ├── online_evaluation.json  # Online metrics
│           ├── EXPERIMENT_REPORT.md    # Analysis report
│           └── 📁 plots/              # Visualizations
│               ├── kbrs_mab_evaluation.png
│               ├── learning_curve.png
│               └── strategy_selection.png
│
└── 📁 docs/                         # Documentation
    └── 📁 plans/                    # Design documents
        └── 2025-11-19-kbrs-mab-pipeline.md
```

---

## 📦 Module Descriptions

### Core Pipeline

#### `kbrs_pipeline.py`
**Main entry point** that orchestrates the entire pipeline:

1. **Data Loading**: Load MovieLens dataset
2. **Enrichment**: Query DBpedia/Wikidata for metadata
3. **Preprocessing**: Normalize features, create one-hot encodings
4. **Splitting**: Create train/val/test/online splits
5. **Feature Creation**: Compute cosine similarity matrix
6. **Offline Evaluation**: Test KBRS on held-out data
7. **Online Simulation**: Run MAB with real-time feedback
8. **Results**: Save metrics, plots, and reports

**Usage**:
```bash
python kbrs_pipeline.py --dataset ml-small-100k
```

---

### Recommender Module (`src/recommender/`)

#### `kbrs.py`
**Knowledge-Based Recommender System** implementation.

**Key Classes**:
- `KBRS`: Main recommender class

**Key Methods**:
- `recommend_movies_hybrid()`: Generate recommendations using hybrid strategy
- `recommend_movies_exploitation()`: High-similarity recommendations
- `recommend_movies_exploration()`: Low-similarity for discovery
- `recommend_movies_semantic()`: Filter by director/cast/genre
- `predict_rating()`: Predict rating for user-movie pair
- `create_user_profile()`: Aggregate user preferences

**Features**:
- Cosine similarity-based recommendations
- Multiple strategies (exploitation, exploration, semantic)
- Caching for performance
- Configurable k_similar parameter

---

### Simulation Module (`src/simulation/`)

#### `kbrs_simulator.py`
**Online MAB simulator** for adaptive strategy selection.

**Key Classes**:
- `OnlineKBRSSimulator`: Main simulator class

**Key Methods**:
- `simulate_online()`: Run full online simulation
- `_select_strategy()`: Thompson Sampling for strategy selection
- `_compute_reward()`: Calculate reward from user feedback
- `_update_mab()`: Update MAB statistics

**MAB Strategies**:
- **Arm 0**: Exploitation (high similarity)
- **Arm 1**: Exploration (low similarity)
- **Arm 2**: Semantic - Genre
- **Arm 3**: Semantic - Cast
- **Arm 4**: Semantic - Director

**Algorithm**: Thompson Sampling (Beta distribution)

---

### Evaluation Module (`src/evaluation/`)

#### `kbrs_evaluator.py`
**Comprehensive evaluation** for KBRS+MAB system.

**Key Methods**:
- `evaluate_offline()`: Offline metrics (RMSE, MAE, Coverage)
- `evaluate_online()`: Online metrics (Reward, Regret, Strategy rates)
- `plot_results()`: Generate visualizations
- `generate_report()`: Create markdown report

**Metrics**:
- RMSE, MAE (prediction accuracy)
- Coverage (% of recommendable items)
- Cumulative reward
- Strategy selection rates
- Cumulative regret

#### `metrics.py`
**Generic evaluation metrics**.

**Functions**:
- `RMSE()`: Root Mean Square Error
- `MAE()`: Mean Absolute Error
- `PrecisionAtK()`: Precision at K
- `NDCGAtK()`: Normalized Discounted Cumulative Gain

---

### Data Module (`src/data/`)

#### `manager.py`
**Data loading and preprocessing**.

**Key Functions**:
- `load_data()`: Load MovieLens CSVs
- `enrich_movies()`: Query DBpedia/Wikidata
- `normalize_movie_data_parallel()`: Feature engineering with multiprocessing
- `create_knowledge_features()`: Extract semantic features

**Features**:
- Parallel processing (8 cores)
- One-hot encoding for genres, cast, directors
- TF-IDF for text features
- Normalization of numerical features

#### `split_manager.py`
**Data splitting utilities**.

**Key Functions**:
- `create_global_split()`: Split into offline/online sets
- `create_train_val_test_split()`: Split offline into train/val/test
- `stratified_split()`: Ensure user coverage

**Strategy**: Timestamp-based for offline/online, stratified random for train/val/test

#### `sparql.py`
**SPARQL query execution** for DBpedia/Wikidata.

**Key Functions**:
- `query_dbpedia()`: Execute SPARQL query
- `batch_query()`: Query multiple movies efficiently
- `parse_results()`: Parse SPARQL JSON results

**Endpoints**:
- DBpedia: `http://dbpedia.org/sparql`
- Wikidata: `https://query.wikidata.org/sparql`

#### `templates.py`
**SPARQL query templates**.

**Templates**:
- `DBPEDIA_MOVIE_QUERY`: Federated query for movie metadata

#### `mapping.py`
**Data mapping utilities**.

**Key Classes**:
- `MappingManager`: Manage ID mappings
- `FeatureMapper`: Map features to indices

---

### Visualization Module (`src/viz/`)

#### `plot_manager.py`
**Plotting utilities** for results visualization.

**Key Functions**:
- `plot_online_learning_curve()`: RMSE/reward over time
- `plot_strategy_selection()`: Strategy selection evolution
- `plot_reward_distribution()`: Reward by strategy
- `plot_cumulative_regret()`: Regret over time

**Style**: Seaborn with custom color palettes

---

### Core Module (`src/core/`)

#### `config.py`
**Configuration classes** for system parameters.

**Key Classes**:
- `DataConfig`: Data preprocessing settings
- `MABConfig`: MAB algorithm parameters
- `RewardConfig`: Reward function settings
- `ExperimentConfig`: Experiment configuration
- `CineWisdomConfig`: Global configuration

**Features**:
- JSON serialization
- Default values
- Validation

---

## 📊 Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                     Data Pipeline                            │
└─────────────────────────────────────────────────────────────┘

1. Raw Data (MovieLens)
   ├── ratings.csv (100K ratings)
   ├── movies.csv (9.7K movies)
   └── links.csv (IMDb/TMDb IDs)
          │
          ▼
2. Enrichment (DBpedia/Wikidata)
   └── movies_enriched.csv
       ├── Directors
       ├── Actors
       ├── Genres
       ├── Abstracts
       └── Themes
          │
          ▼
3. Feature Engineering
   └── normalized_movies.csv
       ├── One-hot: genres (20 dims)
       ├── One-hot: directors (500 dims)
       ├── One-hot: actors (1000 dims)
       └── Numerical: runtime, year
          │
          ▼
4. Similarity Computation
   └── cosine_sim_matrix.npy (9742 × 9742)
          │
          ▼
5. Data Splitting
   ├── train.csv (64%)
   ├── val.csv (8%)
   ├── test.csv (8%)
   └── online.csv (20%)
          │
          ▼
6. Evaluation & Simulation
   ├── Offline: RMSE, MAE on test set
   └── Online: MAB simulation on online set
          │
          ▼
7. Results
   ├── Metrics (JSON)
   ├── History (CSV)
   ├── Plots (PNG)
   └── Report (MD)
```

---

## 🔧 Configuration

### Environment Variables

```bash
# Optional: Set Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Optional: Set data directory
export CINEWISDOM_DATA_DIR="datasets/ml-small-100k"

# Optional: Set results directory
export CINEWISDOM_RESULTS_DIR="results/ml-small-100k"
```

### Config Files

Configuration is managed through `src/core/config.py`:

```python
from src.core.config import CineWisdomConfig

config = CineWisdomConfig()
config.data.dataset_name = "ml-small-100k"
config.mab.n_arms = 5
config.mab.algorithm = "thompson_sampling"
config.save("config.json")
```

---

## 📈 Performance

### File Sizes

| File | Size | Description |
|------|------|-------------|
| `cosine_sim_matrix.npy` | 724 MB | Precomputed similarity matrix |
| `normalized_movies.csv` | 33 MB | Feature-engineered movies |
| `movies_enriched.csv` | 2 MB | DBpedia-enriched metadata |
| `online_history.csv` | 296 KB | Simulation history (20K interactions) |

### Memory Usage

- **Data Loading**: ~500 MB
- **Feature Engineering**: ~1 GB
- **Similarity Computation**: ~2 GB
- **Online Simulation**: ~1.5 GB

### Execution Time (MacBook Pro M1, 16GB RAM)

| Step | Time |
|------|------|
| Data Loading | 5s |
| Enrichment (9K movies) | 2-4 hours |
| Feature Engineering | 10 min |
| Similarity Matrix | 5 min |
| Offline Evaluation (2K samples) | 2 min |
| Online Simulation (20K interactions) | 30 min |
| **Total (with enrichment)** | **3-5 hours** |
| **Total (skip enrichment)** | **45 min** |

---

## 🎯 Key Design Decisions

1. **Modular Architecture**: Each component is independent and testable
2. **Parallel Processing**: Multiprocessing for data preprocessing
3. **Precomputation**: Similarity matrix computed once, reused
4. **Caching**: User profiles and recommendations cached
5. **Sparse Matrices**: Memory-efficient storage for large matrices
6. **JSON Serialization**: Easy result storage and sharing
7. **Markdown Reports**: Human-readable experiment documentation

---

## 📝 Notes

- All paths are relative to project root
- File sizes are approximate (ml-small-100k dataset)
- Execution times vary based on hardware and dataset size
- Memory usage peaks during similarity matrix computation

---

**Last Updated**: 2025-01-21  
**Project Version**: 1.0.0  
**Python Version**: 3.8+
