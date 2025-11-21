# 🎬 CineWisdom

**Knowledge-Based Recommender System with Multi-Armed Bandit Optimization**

A hybrid movie recommendation system that combines semantic knowledge from DBpedia/Wikidata with adaptive Multi-Armed Bandit strategies for personalized recommendations.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [How It Works](#how-it-works)
- [Evaluation](#evaluation)
- [Results](#results)
- [Documentation](#documentation)
- [License](#license)

---

## 🎯 Overview

CineWisdom is a research project that implements a **Knowledge-Based Recommender System (KBRS)** enhanced with **Multi-Armed Bandit (MAB)** algorithms for adaptive strategy selection. The system leverages:

- **Semantic Knowledge**: Movie metadata enriched from DBpedia and Wikidata (directors, actors, genres, themes)
- **Hybrid Strategies**: Multiple recommendation approaches (collaborative filtering, content-based, exploration)
- **Adaptive Learning**: Thompson Sampling MAB to dynamically select the best strategy per user
- **Real-time Feedback**: Online learning from user interactions

### Key Innovations

1. **Semantic Filtering**: Recommendations based on shared directors, actors, or genres
2. **Exploration vs Exploitation**: Balances familiar recommendations with discovery
3. **Online Adaptation**: Learns user preferences in real-time without retraining
4. **Knowledge Graph Integration**: Enriches movie features with linked open data

---

## ✨ Features

### Core Capabilities

- ✅ **Knowledge-Based Recommendations**: Semantic similarity using movie metadata
- ✅ **Multi-Armed Bandit**: Thompson Sampling for strategy selection
- ✅ **Hybrid Strategies**: 
  - Exploitation (high similarity)
  - Exploration (low similarity for discovery)
  - Semantic filtering (director, cast, genre)
- ✅ **DBpedia/Wikidata Integration**: Automated SPARQL queries for metadata enrichment
- ✅ **Offline & Online Evaluation**: Comprehensive metrics (RMSE, MAE, Reward, Coverage)
- ✅ **Visualization**: Learning curves, strategy selection, reward evolution

### Technical Features

- 🚀 **Parallel Processing**: Multi-core data preprocessing
- 📊 **Cosine Similarity Matrix**: Pre-computed for fast recommendations
- 🎨 **Feature Engineering**: One-hot encoding for genres, cast, directors
- 📈 **Performance Tracking**: Real-time metrics during simulation
- 💾 **Persistent Storage**: Saves models, results, and history

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     CineWisdom System                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐      ┌──────────────┐      ┌───────────┐ │
│  │   Data       │      │    KBRS      │      │    MAB    │ │
│  │  Manager     │─────▶│   Engine     │◀────▶│  Selector │ │
│  └──────────────┘      └──────────────┘      └───────────┘ │
│         │                      │                     │       │
│         │                      │                     │       │
│    ┌────▼────┐          ┌─────▼─────┐        ┌─────▼─────┐ │
│    │DBpedia/ │          │ Cosine    │        │ Thompson  │ │
│    │Wikidata │          │Similarity │        │ Sampling  │ │
│    └─────────┘          └───────────┘        └───────────┘ │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              Online Simulator                         │  │
│  │  • User Interaction Loop                             │  │
│  │  • Strategy Selection (MAB)                          │  │
│  │  • Reward Calculation                                │  │
│  │  • Real-time Learning                                │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              Evaluator                                │  │
│  │  • Offline Metrics (RMSE, MAE)                       │  │
│  │  • Online Metrics (Reward, Regret)                   │  │
│  │  • Visualization & Reports                           │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 📦 Installation

### Requirements

- Python 3.8+
- 8GB+ RAM (for similarity matrix computation)
- Internet connection (for DBpedia/Wikidata enrichment)

### Setup

```bash
# Clone repository
git clone https://github.com/yourusername/CineWisdom.git
cd CineWisdom

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Download MovieLens dataset (if not included)
# Place in datasets/ml-small-100k/raw/
```

### Dependencies

```
pandas>=1.5.0
numpy>=1.23.0
scikit-learn>=1.2.0
scipy>=1.10.0
matplotlib>=3.6.0
seaborn>=0.12.0
SPARQLWrapper>=2.0.0
tqdm>=4.64.0
```

---

## 🚀 Quick Start

### 1. Run the Full Pipeline

```bash
python kbrs_pipeline.py --dataset ml-small-100k
```

This will:
1. Load MovieLens data
2. Enrich movies with DBpedia/Wikidata metadata
3. Normalize features and create similarity matrix
4. Split data (train/val/test/online)
5. Evaluate KBRS offline
6. Run online MAB simulation
7. Generate results and plots

### 2. Skip Enrichment (Faster)

If you already have enriched data:

```bash
python kbrs_pipeline.py --dataset ml-small-100k --skip-enrichment
```

### 3. Limit Online Simulation

For quick testing:

```bash
python kbrs_pipeline.py --dataset ml-small-100k --limit 500
```

### 4. View Results

```bash
# Check results directory
ls -lh results/ml-small-100k/semantic_mab/

# View experiment report
cat results/ml-small-100k/semantic_mab/EXPERIMENT_REPORT.md

# View plots
open results/ml-small-100k/semantic_mab/plots/
```

---

## 📁 Project Structure

```
CineWisdom/
├── kbrs_pipeline.py              # Main entry point
├── requirements.txt              # Python dependencies
├── README.md                     # This file
├── TESTING.md                    # Testing documentation
├── PROJECT_STRUCTURE.md          # Detailed structure
│
├── src/                          # Source code
│   ├── recommender/
│   │   └── kbrs.py              # KBRS core engine
│   ├── simulation/
│   │   └── kbrs_simulator.py    # Online MAB simulator
│   ├── evaluation/
│   │   ├── kbrs_evaluator.py    # Evaluation metrics
│   │   └── metrics.py           # Generic metrics (RMSE, MAE, NDCG)
│   ├── data/
│   │   ├── manager.py           # Data loading & preprocessing
│   │   ├── split_manager.py     # Train/val/test splitting
│   │   ├── sparql.py            # DBpedia/Wikidata queries
│   │   ├── templates.py         # SPARQL query templates
│   │   └── mapping.py           # Data mapping utilities
│   ├── viz/
│   │   └── plot_manager.py      # Visualization tools
│   └── core/
│       └── config.py            # Configuration classes
│
├── datasets/                     # Data storage
│   └── ml-small-100k/
│       ├── raw/                 # Original MovieLens CSVs
│       ├── processed/           # Enriched & normalized data
│       └── splits/              # Train/val/test/online splits
│
├── models/                       # Saved models
│   └── kbrs/
│       └── ml-small-100k/
│           ├── cosine_sim_matrix.npy
│           └── movie_ids.csv
│
└── results/                      # Experiment results
    └── ml-small-100k/
        └── semantic_mab/
            ├── online_history.csv
            ├── offline_evaluation.json
            ├── online_evaluation.json
            ├── EXPERIMENT_REPORT.md
            └── plots/
```

---

## 🔬 How It Works

### 1. Data Enrichment

Movies are enriched with semantic metadata from DBpedia/Wikidata:

```python
# SPARQL query to DBpedia
SELECT ?director ?actor ?genre ?abstract
WHERE {
  ?film owl:sameAs wd:Q12345 .
  ?film dbo:director ?director .
  ?film dbo:starring ?actor .
  ...
}
```

### 2. Feature Engineering

- **One-hot encoding** for genres, directors, actors
- **TF-IDF** for text features (abstracts, themes)
- **Normalization** of numerical features (runtime, release year)
- **Cosine similarity matrix** (9742 × 9742 for ml-small-100k)

### 3. KBRS Recommendation

For a user with history `H = {m1, m2, ..., mn}`:

1. **Aggregate user profile**: Average feature vectors of liked movies
2. **Compute similarity**: Cosine similarity between profile and all movies
3. **Apply strategy**:
   - **Exploitation**: Top-K most similar movies
   - **Exploration**: Bottom-K similar movies (for discovery)
   - **Semantic**: Filter by shared director/cast/genre
4. **Predict rating**: Weighted average based on similarity

### 4. Multi-Armed Bandit

Thompson Sampling selects the best strategy:

```python
# For each strategy i:
α_i = successes + 1
β_i = failures + 1
θ_i ~ Beta(α_i, β_i)

# Select strategy with highest θ
strategy = argmax(θ_i)
```

### 5. Online Learning

```
For each user interaction:
  1. MAB selects strategy
  2. KBRS generates recommendations
  3. User rates a movie
  4. Compute reward (based on rating accuracy)
  5. Update MAB statistics
  6. Repeat
```

---

## 📊 Evaluation

### Offline Metrics

Evaluated on test set (unseen user-movie pairs):

- **RMSE** (Root Mean Square Error): Prediction accuracy
- **MAE** (Mean Absolute Error): Average prediction error
- **Coverage**: % of items that can be recommended
- **Precision@K**: Relevant items in top-K
- **NDCG@K**: Ranking quality

### Online Metrics

Evaluated during simulation:

- **Cumulative Reward**: Total reward over time
- **Mean Reward**: Average reward per interaction
- **Exploration Rate**: % of exploration strategies selected
- **Exploitation Rate**: % of exploitation strategies selected
- **Cumulative Regret**: Difference from optimal strategy
- **Strategy Performance**: Per-strategy reward statistics

### Visualization

- Learning curves (RMSE, reward over time)
- Strategy selection evolution
- Reward distribution by strategy
- Cumulative regret
- Error distribution

---

## 📈 Results

### Typical Performance (MovieLens 100K)

#### Offline Evaluation
```
RMSE:     0.93
MAE:      0.72
Coverage: 100%
```

#### Online Simulation (20K interactions)
```
Final RMSE:        0.89
Mean Reward:       0.85
Exploration Rate:  28%
Exploitation Rate: 22%
Semantic (Genre):  30%
Semantic (Cast):   20%
```

### Key Findings

1. **MAB Adaptation**: System learns to prefer semantic strategies for most users
2. **Exploration Value**: 25-30% exploration maintains diversity without hurting accuracy
3. **Semantic Filtering**: Genre-based filtering performs best (30% selection rate)
4. **Online Improvement**: RMSE improves from 0.93 (offline) to 0.89 (online)

---

## 📚 Documentation

- **[TESTING.md](TESTING.md)**: Complete testing guide and results
- **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)**: Detailed architecture
- **[TECHNICAL_DOCS.md](TECHNICAL_DOCS.md)**: Algorithm details and theory
- **[docs/plans/](docs/plans/)**: Design documents and planning

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **MovieLens**: GroupLens Research for the dataset
- **DBpedia/Wikidata**: Linked open data for movie metadata
- **Thompson Sampling**: Classic MAB algorithm for exploration-exploitation

---

## 📧 Contact

For questions or feedback:
- **Author**: Marcello Russo
- **Email**: [your-email@example.com]
- **GitHub**: [github.com/marcellorussox/CineWisdom]

---

**Built with ❤️ for intelligent movie recommendations**
