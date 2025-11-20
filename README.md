# 🎬 CineWisdom: Knowledge-Based Recommender System

**CineWisdom** is an advanced recommender system that leverages **Semantic Knowledge (DBpedia)** and **Online Learning (Multi-Armed Bandit)** to provide personalized movie recommendations.

## 🚀 Key Features

*   **Knowledge-Based Core**: Uses SPARQL to enrich MovieLens data with semantic features (Directors, Actors, Runtime) from DBpedia/Wikidata.
*   **Online Learning (MAB)**: Implements **Thompson Sampling** to dynamically balance between **Exploration** (diversity) and **Exploitation** (similarity).
*   **Adaptive Filtering**: Uses percentile-based thresholds to adapt to feature density.
*   **Robust Evaluation**: Comprehensive offline (RMSE, MAE) and online (Regret, Reward) metrics.

## 📂 Documentation & Results

*   **[Technical Documentation](TECHNICAL_DOCS.md)**: 📘 Deep dive into the architecture, theory, and design decisions. **Start here.**
*   **[Experiment Report](results/ml-small-100k/kbrs/EXPERIMENT_REPORT.md)**: 🧪 Detailed analysis of the latest run on MovieLens Small.
*   **[Project Structure](PROJECT_STRUCTURE.md)**: 📁 Overview of the codebase layout.

## 🛠️ Installation & Usage

### Prerequisites
*   Python 3.8+
*   Dependencies: `pandas`, `numpy`, `scikit-learn`, `SPARQLWrapper`, `tqdm`, `matplotlib`, `seaborn`, `tabulate`

### Quick Start
To run the full pipeline (Enrichment -> Training -> Simulation -> Evaluation):

```bash
python kbrs_pipeline.py
```

This will:
1.  Load MovieLens Small.
2.  Enrich movies via SPARQL (if not cached).
3.  Compute Cosine Similarity matrix.
4.  Run Offline Evaluation.
5.  Run Online MAB Simulation.
6.  Generate Reports and Plots.

## 📊 Latest Performance (ML-Small)

| Metric | Value |
| :--- | :--- |
| **RMSE** | **0.92** |
| **Mean Reward** | **0.84** |
| **Exploitation Rate** | **56%** |

---
*Developed for Intelligent Web Course.*
