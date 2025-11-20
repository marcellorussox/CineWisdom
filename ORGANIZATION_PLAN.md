# Dataset & Results Organization Plan

## Directory Structure
```
datasets/
├── ml-small-100k/           # MAIN DATASET (per progetto)
│   ├── raw/                 # Original data
│   ├── processed/           # DBpedia enriched + normalized
│   └── splits/              # Train/val/test/online splits
├── ml-1m/                   # Keep for reference
│   └── results/             # Move old results here
├── ml-20m-filtered/         # Keep for future experiments
│   └── results/             # Move results here
└── archived/                # Old/intermediate files

models/
├── kbrs/                    # KBRS models (cosine_sim matrices, etc.)
│   └── ml-small-100k/
└── ncf/                     # NCF models (archived)
    └── ml-1m/

results/
├── ml-small-100k/           # Main results
│   ├── kbrs/
│   └── mab/
├── ml-1m/                   # Reference results
└── ml-20m-filtered/         # Future experiments

plots/
├── ml-small-100k/
├── ml-1m/
└── ml-20m-filtered/

logs/
├── ml-small-100k/
├── ml-1m/
└── ml-20m-filtered/
```

## Cleanup Actions

### 1. Remove Intermediate Files
- `datasets/*/processed/normalized_movies_optimized.csv` (if exists)
- `datasets/*/processed/dbpedia_data.csv` (checkpoint files, can regenerate)
- Old NCF feature files (except archived ones)

### 2. Archive Old NCF Work
- Move NCF models to `models/ncf/ml-1m/`
- Move NCF results to `results/ml-1m/ncf/`

### 3. Setup ML-Small as Primary
- Ensure `datasets/ml-small-100k/raw/` has clean data
- Create empty subdirectories for results

### 4. Keep
- All enriched/normalized data (takes hours to regenerate)
- All trained models
- All final results/plots
