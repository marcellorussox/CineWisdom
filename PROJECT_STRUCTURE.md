# CineWisdom Project Structure

Complete directory organization for multi-dataset experiments.

## 📁 Directory Tree

```
CineWisdom/
├── datasets/                      # Data storage (organized by dataset)
│   ├── ml-small-100k/            # MovieLens 100k
│   │   ├── raw/                  # ratings.csv, movies.csv
│   │   ├── processed/            # enriched data + SVD features
│   │   └── splits/               # train/val/test/online splits
│   ├── ml-1m/                    # MovieLens 1M
│   │   ├── raw/                  # ratings.csv, movies.csv
│   │   ├── processed/            # enriched data + SVD features
│   │   └── splits/               # train/val/test/online splits
│   └── archived/                 # Previous experiments backup
│
├── models/                        # Trained models (organized by dataset)
│   ├── ml-small-100k/            # NCF models for ML-Small
│   └── ml-1m/                    # NCF models for ML-1M
│
├── results/                       # Simulation results (organized by dataset)
│   ├── ml-small-100k/
│   │   ├── archived/
│   │   │   ├── run_30offline_70online/
│   │   │   └── run_80offline_20online/
│   │   └── online_simulation_*.csv
│   └── ml-1m/
│
├── plots/                         # Visualizations (organized by dataset)
│   ├── ml-small-100k/
│   └── ml-1m/
│
├── logs/                          # Execution logs (organized by dataset + run)
│   ├── ml-small-100k/
│   │   ├── run_30offline_70online/
│   │   └── run_80offline_20online/
│   └── ml-1m/
│
├── src/                           # Source code
│   ├── data/                     # Data management
│   ├── models/                   # NCF implementation
│   ├── training/                 # Training loop
│   ├── evaluation/               # Metrics
│   ├── bandit/                   # MAB policies
│   ├── simulation/               # Online simulator
│   └── viz/                      # Plotting utilities
│
├── scripts/                       # Utility scripts
│   └── convert_ml1m_to_csv.py
│
├── docs/                          # Documentation
└── main_pipeline.py              # Main entry point
```

## 🚀 Usage Examples

### Run with ML-1M (default)
```bash
python main_pipeline.py --dataset ml-1m --mode preprocess --enrich
python main_pipeline.py --dataset ml-1m --mode split --online_split 0.2
python main_pipeline.py --dataset ml-1m --mode train --use_features
python main_pipeline.py --dataset ml-1m --mode online --bandit thompson
```

### Run with ML-Small
```bash
python main_pipeline.py --dataset ml-small-100k --mode train --use_features
```

### Save logs
```bash
python main_pipeline.py --dataset ml-1m --mode train | \
    tee logs/ml-1m/run_80offline_20online/training.log
```

## 📊 Dataset Comparison

| Dataset | Users | Movies | Ratings | Sparsity | RAM Usage |
|---------|-------|--------|---------|----------|-----------|
| **ML-Small** | 610 | 9.7k | 100k | 98.3% | ~500MB |
| **ML-1M** | 6k | 3.7k | 1M | 95.5% | ~1-2GB |
