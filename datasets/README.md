# CineWisdom Datasets

This directory contains multiple MovieLens datasets with isolated structures.

## Structure

```
datasets/
├── ml-small-100k/          # MovieLens Small (100k ratings)
│   ├── raw/                # Original CSV files
│   ├── processed/          # Enriched + normalized data
│   └── splits/             # Train/Val/Test/Online splits
├── ml-1m/                  # MovieLens 1M (1M ratings)
│   ├── raw/                # Original CSV files
│   ├── processed/          # Enriched + normalized data
│   └── splits/             # Train/Val/Test/Online splits
└── archived/               # Previous runs
```

## Usage

Select dataset with `--dataset` parameter:

```bash
# Use ML-1M (default)
python main_pipeline.py --dataset ml-1m --mode preprocess --enrich

# Use ML-Small
python main_pipeline.py --dataset ml-small-100k --mode train --use_features
```

## Dataset Statistics

### ML-Small (100k)
- Users: 610
- Movies: 9,724
- Ratings: 100,836
- Sparsity: 98.3%

### ML-1M
- Users: 6,040
- Movies: 3,706
- Ratings: 1,000,209
- Sparsity: 95.5%
