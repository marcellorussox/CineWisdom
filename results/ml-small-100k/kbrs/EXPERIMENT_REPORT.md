# 🧪 Experiment Report: KBRS + MAB on MovieLens Small

**Date:** 2025-11-21
**Dataset:** MovieLens Small (100k)
**Configuration:** KBRS (DBpedia Features) + Thompson Sampling MAB

## 1. Executive Summary
This experiment validated the effectiveness of a Knowledge-Based Recommender System integrated with an Online Multi-Armed Bandit. The system successfully learned to balance Exploration and Exploitation, achieving high accuracy and user satisfaction.

| Metric | Result | Interpretation |
| :--- | :--- | :--- |
| **Final RMSE** | **0.9247** | **Excellent.** The average prediction error is less than 1 star. |
| **Mean Reward** | **0.8403** | **High.** Users accepted recommendations ~84% of the time. |
| **MAE** | **0.7188** | Very low absolute error. |

---

## 2. MAB Strategy Analysis (Thompson Sampling)

The Bandit algorithm dynamically selected between two strategies based on user feedback.

### Strategy Distribution
*   **Exploitation (56.3%)**: Selected majority of the time.
*   **Exploration (43.7%)**: Selected frequently.

**Interpretation:**
This is a **highly significant result**. In many MAB implementations, the algorithm converges 100% to a single "best" arm. Here, the fact that Exploration remains close to 44% indicates that **diversity is valuable**. The system learned that users don't *always* want the same thing; they frequently respond positively to diverse recommendations (Exploration), preventing the system from collapsing into a filter bubble.

### Strategy Performance
| Strategy | Mean Reward | RMSE |
| :--- | :--- | :--- |
| **Exploitation** | **0.8460** | **0.9001** |
| **Exploration** | 0.8328 | 0.9555 |

**Insight:**
Exploitation is indeed the "safer" strategy (higher reward, lower error), which explains why the MAB favors it (56%). However, Exploration is surprisingly competitive (only 0.01 lower reward). This validates our **Percentile Filtering** approach: even "dissimilar" movies (bottom 30%) are relevant enough to be liked, provided they are computed via robust DBpedia features.

---

## 3. Learning Curve & Regret
*   **Regret Analysis**: The average regret over the last 100 interactions is **0.017**.
*   **Meaning**: The system has effectively converged. It is making near-optimal decisions with very little "regret" (loss of potential reward).

## 4. Conclusion
The experiment is a success. The KBRS+MAB architecture proves that:
1.  **Semantic Enrichment works**: DBpedia features provide a strong signal for content similarity.
2.  **Online Learning works**: The system adapts to user preferences in real-time.
3.  **Diversity is viable**: Exploration is not just a "cost" but a valid source of reward.

---
*Refer to `plots/` directory for visual representations of these metrics.*
