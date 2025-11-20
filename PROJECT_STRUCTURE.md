# 📁 Project Structure

```
CineWisdom/
├── README.md                   # Main entry point
├── TECHNICAL_DOCS.md           # Architecture & Theory documentation
├── kbrs_pipeline.py            # MAIN SCRIPT: Runs the full KBRS+MAB pipeline
├── datasets/
│   └── ml-small-100k/          # MovieLens Small Dataset
│       ├── raw/                # Original CSVs
│       ├── processed/          # Enriched & Normalized data
│       └── splits/             # Train/Val/Test/Online splits
├── models/
│   └── kbrs/
│       └── ml-small-100k/      # Saved models (Cosine Matrix, ID mappings)
├── results/
│   └── ml-small-100k/
│       └── kbrs/
│           ├── EXPERIMENT_REPORT.md  # Analysis of results
│           ├── online_history.csv    # Full simulation log
│           ├── plots/                # Generated visualizations
│           └── *.json                # Metrics
├── src/
│   ├── data/
│   │   ├── manager.py          # Data loading & preprocessing
│   │   ├── sparql.py           # DBpedia enrichment logic
│   │   └── split_manager.py    # Data splitting logic
│   ├── recommender/
│   │   └── kbrs.py             # Core KBRS logic (Cosine Sim)
│   ├── simulation/
│   │   └── kbrs_simulator.py   # Online MAB Simulator (Thompson Sampling)
│   └── evaluation/
│       └── kbrs_evaluator.py   # Metrics & Plotting
└── scripts/
    └── cleanup.sh              # Utility to clean temp files
```
