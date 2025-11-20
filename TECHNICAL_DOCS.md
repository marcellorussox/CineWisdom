# 📘 CineWisdom: Technical Documentation

## 1. System Architecture

CineWisdom is a **Knowledge-Based Recommender System (KBRS)** enhanced with a **Multi-Armed Bandit (MAB)** mechanism for dynamic strategy selection.

### Core Components
1.  **Data Enrichment (SPARQL)**: Extracts semantic features (Directors, Actors, Runtime) from DBpedia/Wikidata to enrich MovieLens data.
2.  **Feature Engineering**: Converts categorical knowledge into a dense feature matrix (One-Hot Encoding) and computes Cosine Similarity.
3.  **KBRS Engine**: A content-based predictor using weighted average ratings of similar items.
4.  **MAB Simulator**: An online learning module using Thompson Sampling to choose between recommendation strategies.

---

## 2. Theoretical Framework

### 2.1 Knowledge-Based Filtering
Unlike Collaborative Filtering (which relies on user overlap), our KBRS relies purely on item attributes.
$$ Similarity(i, j) = \cos(\vec{f_i}, \vec{f_j}) = \frac{\vec{f_i} \cdot \vec{f_j}}{\|\vec{f_i}\| \|\vec{f_j}\|} $$
Where $\vec{f}$ represents the feature vector (Genres + DBpedia Knowledge).

**Prediction Formula:**
$$ \hat{r}_{u,i} = \frac{\sum_{j \in H_u} sim(i, j) \cdot r_{u,j}}{\sum_{j \in H_u} sim(i, j)} $$
Where $H_u$ is the user's history.

### 2.2 Multi-Armed Bandit (Thompson Sampling)
To solve the Exploration-Exploitation dilemma, we treat recommendation strategies as "arms" in a bandit problem.
We model the reward probability of each arm using a Beta distribution:
$$ \theta_k \sim Beta(\alpha_k, \beta_k) $$

*   **Success ($\alpha$)**: User gives a high rating (≥ 3.5).
*   **Failure ($\beta$)**: User gives a low rating (< 3.5).

At each step, we sample $\hat{\theta}_k$ from the distributions and choose the strategy with the highest sample.

---

## 3. Key Intuitions & Design Decisions

### 3.1 Percentile-Based Filtering (Adaptive Thresholds)
**Problem:** Fixed similarity thresholds (e.g., $>0.7$) are brittle. Some movies have many close neighbors, others have few. A fixed threshold caused the system to return 0 recommendations for "unique" movies.
**Solution:** We implemented **Percentile Filtering**.
*   **Exploitation:** Select candidates in the **Top 30%** of similarity for that specific movie.
*   **Exploration:** Select candidates in the **Bottom 30%** (dissimilar but potentially interesting).
*   **Result:** The system adapts to the density of the feature space, ensuring recommendations are always generated.

### 3.2 The "Negative Cosine" Trap
**Problem:** Initially, we applied `StandardScaler` to One-Hot encoded features. This centered values around 0, creating negative values.
**Consequence:** Cosine similarity became negative. In the weighted average formula, a negative weight multiplied by a positive rating (5.0) resulted in negative predictions (e.g., -2.5), destroying RMSE.
**Solution:** We removed scaling. For binary features (presence/absence of an actor), raw 0/1 vectors ensure similarity is always $\in [0, 1]$, preserving the logic of the weighted average.

### 3.3 Data Leakage in Offline Evaluation
**Problem:** Initially, the offline evaluation passed the `test_set` as both the *target* and the *history*. The system found the target movie inside the history (similarity 1.0) and predicted the exact rating.
**Solution:** We strictly separated the data:
*   **History:** `train_set` (Past interactions)
*   **Target:** `test_set` (Future interactions to predict)
This yielded a realistic RMSE (~0.95) compared to the "cheated" one.

---

## 4. Strategies Implemented

1.  **Exploitation Arm**:
    *   Logic: Recommend movies strictly similar to what the user liked.
    *   Goal: Minimize immediate error (RMSE).
    *   Implementation: Filter candidates with Similarity > 70th percentile.

2.  **Exploration Arm**:
    *   Logic: Recommend movies diverse from the user's profile.
    *   Goal: Discover new interests and avoid "filter bubbles".
    *   Implementation: Filter candidates with Similarity < 30th percentile.

---

## 5. Performance Summary
The final system achieves an **RMSE of ~0.92** and a **Mean Reward of ~0.84**. This proves that a pure Knowledge-Based system, enriched with semantic data, can perform excellently without needing collaborative data (User-User similarity).
