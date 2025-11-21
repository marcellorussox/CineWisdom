
# KBRS + MAB Evaluation Report

## Offline Performance

- **RMSE**: 0.9549
- **MAE**: 0.7408
- **Coverage**: 100.00%
- **Predictions**: 1,000 / 1,000

## Online Performance

### Overall Metrics
- **Final RMSE**: 0.9247
- **Final MAE**: 0.7188
- **Mean Reward**: 0.8403
- **Total Interactions**: 20,168

### Strategy Selection
- **strategy_0_rate**: 32.43%
- **strategy_1_rate**: 13.24%
- **strategy_2_rate**: 10.74%
- **strategy_3_rate**: 43.58%
- **Total Pulls**: 20,168

### MAB Learning
- **Final Cumulative Regret**: 94.36
- **Avg Regret (last 100)**: 0.0161

### Strategy Performance
|             |   reward_mean |   reward_std |   reward_count |   predicted_rating_<lambda> |
|:------------|--------------:|-------------:|---------------:|----------------------------:|
| cast        |        0.8272 |       0.1369 |           2671 |                      0.9921 |
| director    |        0.844  |       0.1272 |           6541 |                      0.9057 |
| exploration |        0.8449 |       0.1269 |           8789 |                      0.9015 |
| genre       |        0.8261 |       0.1334 |           2167 |                      0.9863 |

## Conclusions

The MAB system successfully learned to balance exploration and exploitation strategies
for the KBRS, adapting dynamically based on real-time feedback.

---
Generated: 2025-11-21 02:03:59.635212
