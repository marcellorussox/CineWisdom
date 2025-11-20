# CineWisdom Training Logs

This directory contains execution logs for all pipeline runs, organized by dataset and configuration.

## Structure

```
logs/
├── ml-small-100k/                    # MovieLens 100k runs
│   ├── run_30offline_70online/       # Split: 30% offline, 70% online
│   │   ├── training.log              # NCF training output
│   │   └── mab_simulation.log        # MAB online simulation output
│   └── run_80offline_20online/       # Split: 80% offline, 20% online
│       ├── training.log
│       └── mab_simulation.log
└── ml-1m/                            # MovieLens 1M runs
    └── (future runs)
```

## Naming Convention

- **Dataset folder**: `ml-<dataset-size>/`
- **Run folder**: `run_<X>offline_<Y>online/` where X+Y=100
- **Log files**:
  - `training.log`: NCF model training with metrics
  - `mab_simulation.log`: Online MAB simulation results
  - `preprocessing.log`: Data preprocessing + enrichment (if saved)

## How to Save Logs

Use `tee` to save logs while viewing output:

```bash
# Training
caffeinate -i python main_pipeline.py --dataset ml-1m --mode train --use_features | \
    tee logs/ml-1m/run_80offline_20online/training.log

# MAB Simulation
python main_pipeline.py --dataset ml-1m --mode online --bandit thompson | \
    tee logs/ml-1m/run_80offline_20online/mab_simulation.log
```
