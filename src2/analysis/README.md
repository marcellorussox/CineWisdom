# Pondered Reward Analysis Module

## Overview

This module provides analytical tools to compute and visualize **Pondered Reward (R_P)** metrics from Multi-Armed Bandit (MAB) simulation history without re-running expensive simulations.

## Problem Statement

The MAB simulation uses an **Exploration Reward (R_A)** that only rewards predictions ≥4.0 for **UNSEEN** items. While this is a rigorous metric for measuring exploration effectiveness, it doesn't capture the model's overall accuracy.

The **Pondered Reward** balances both aspects:
- **R_A (Exploration)**: Binary reward for unseen high-quality predictions
- **R_G (General Accuracy Proxy)**: Model's overall accuracy rate

## Formula

```
R_P = 0.5 × R_A + 0.5 × R_G^proxy
```

Where:
- **R_A** = Original exploration reward from MAB history (binary: 0 or 1)
- **R_G^proxy** = Obtained dynamically from simulator.get_kbrs_general_accuracy()
- **R_G^proxy** = 0.00 for Popularity_Baseline (non-personalized)

## Usage

### 1. Standalone Script

```bash
python analyze_pondered_reward.py
```

This will:
- Load `datasets/processed/mab_history.csv`
- Compute pondered rewards
- Generate comparative plot
- Print summary report

### 2. Programmatic Usage

```python
from src.analysis.pondered_reward import load_and_analyze

# Load and analyze
# Note: Get R_G dynamically from simulator, not hardcoded!
# kbrs_rg = simulator.get_kbrs_general_accuracy()
analyzer = load_and_analyze(
    history_path="datasets/processed/mab_history.csv",
    kbrs_general_accuracy=kbrs_rg,  # Dynamic value from simulator
    baseline_general_accuracy=0.0,
    plot=True,
    save_plot_path="plots/pondered_reward_comparison.png",
)

# Get final metrics
final_metrics = analyzer.get_final_metrics()
print(f"Final R_P: {final_metrics['final_R_P']:.4f}")

# Generate summary report
print(analyzer.generate_summary_report())
```

### 3. Jupyter Notebook

Copy the code from `notebook_pondered_analysis.py` into a new cell in `kbrs.ipynb`.

## API Reference

### `PonderedRewardAnalyzer`

Main class for pondered reward analysis.

**Constructor:**
```python
PonderedRewardAnalyzer(
    history_df: pd.DataFrame,
    kbrs_general_accuracy: float,  # Required: get from simulator.get_kbrs_general_accuracy()
    baseline_general_accuracy: float = 0.0,
)
```

**Methods:**

- `compute_cumulative_metrics() -> pd.DataFrame`
  - Returns DataFrame with columns: `iteration`, `cum_avg_R_A`, `cum_avg_R_P`

- `get_final_metrics() -> dict`
  - Returns: `{'final_R_A': float, 'final_R_P': float, 'n_iterations': int}`

- `plot_comparative_rewards(figsize=(14,6), save_path=None, show=True)`
  - Generates comparative plot of R_A vs R_P

- `generate_summary_report() -> str`
  - Returns formatted text summary

### `load_and_analyze()`

Convenience function for quick analysis.

```python
load_and_analyze(
    history_path: str,
    kbrs_general_accuracy: float,  # Required: get from simulator.get_kbrs_general_accuracy()
    baseline_general_accuracy: float = 0.0,
    plot: bool = True,
    save_plot_path: Optional[str] = None,
) -> PonderedRewardAnalyzer
```

## Output

### Console Summary

```
======================================================================
PONDERED REWARD ANALYSIS SUMMARY
======================================================================

METRIC DEFINITIONS:
  • R_A (Exploration): Binary reward for UNSEEN items with pred ≥ 4.0
  • R_G (General Accuracy Proxy): Computed dynamically from simulation
  • R_P (Pondered): 0.5 × R_A + 0.5 × R_G

FINAL CUMULATIVE AVERAGE REWARDS:
  • R_A (Exploration):     0.5500
  • R_P (Pondered):        0.6900
  • Improvement:           0.1400
  • Relative Gain:         25.45%

MODEL SELECTION STATISTICS:
  • KBRS_Hybrid            :  9500 (95.00%)
  • Popularity_Baseline    :   500 ( 5.00%)

Total Iterations: 10000
======================================================================
```

### Comparative Plot

The plot shows two curves:
- **Green line**: R_A (Exploration Reward) - original metric
- **Blue line**: R_P (Pondered Reward) - balanced metric

Both curves show cumulative average over iterations, with final values annotated.

## Files

- `src/analysis/pondered_reward.py` - Main analysis module
- `analyze_pondered_reward.py` - Standalone script
- `notebook_pondered_analysis.py` - Notebook code snippet
- `src/analysis/README.md` - This documentation

## Dependencies

- `pandas` - Data manipulation
- `numpy` - Numerical operations
- `matplotlib` - Visualization

All dependencies are already included in the project's requirements.

## Expected Results

Based on typical MAB simulations:
- **R_A (Exploration)**: ~0.55 (55% success rate for unseen high-quality predictions)
- **R_P (Pondered)**: ~0.69 (69% when accounting for general accuracy)
- **Improvement**: ~25% relative gain

This demonstrates that while the KBRS_Hybrid model achieves 55% exploration success, it maintains 83% general accuracy, resulting in a more comprehensive performance metric of 69%.

## Notes

- The analysis is **analytical** (no re-simulation required)
- Uses existing MAB history CSV file
- General accuracy proxy is obtained dynamically from simulator (not hardcoded)
- Computation is fast and memory-efficient
- Can be extended to other weighting schemes (e.g., 0.3 × R_A + 0.7 × R_G)

## Future Extensions

Potential enhancements:
1. Configurable weighting (α × R_A + (1-α) × R_G)
2. Time-windowed analysis (e.g., first 1000 vs last 1000 iterations)
3. Statistical significance testing
4. Confidence intervals via bootstrap
5. Multi-metric comparison (add precision, recall, etc.)
