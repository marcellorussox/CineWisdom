# Code Planning Dettagliato - CineWisdom Opzione 3

## 📊 **ANALISI STATO ATTUALE**

### ✅ **COMPONENTI PRESENTI E FUNZIONANTI**

#### **1. Preprocessing (NON TOCCARE - FUNZIONA BENE)**
```
src/data/manager.py (442 righe) ✅
├── load_data()                    → Carica MovieLens
├── enrich_movies()                → Wikidata + DBpedia
├── compress_kbrs_dataset()        → TruncatedSVD (4096 comp.)
└── normalize_movie_data_parallel()→ Normalizzazione

src/pipeline/preprocessing.py (352 righe) ✅
└── PreprocessingPipeline          → API di alto livello
    ├── Config: PreprocessingConfig
    └── run_full_pipeline()
```

**⚠️ VINCOLO**: Non modificare questi file! Funzionano bene e sono usati dal notebook.

---

#### **2. KBRS (PRESENTE - DA ESTENDERE)**
```
src/recommender/kbrs.py (221 righe) ✅
└── KBRS class
    ├── __init__(unique_movie_catalog)
    ├── _get_movie_index()          → Cache ottimizzata
    ├── create_user_profile()
    ├── predict_rating()
    └── recommend_movies_hybrid()   → Solo exploitation attuale

⚠️ PROBLEMA: Non supporta exploration strategy
```

#### **3. MAB Algorithms (PRESENTE)**
```
src/bandit/algorithms.py (205 righe) ✅
└── AdvancedThompsonSampling        → GIÀ IMPLEMENTATO!
    ├── ThompsonSamplingConfig
    ├── select_arm()
    ├── update()
    └── get_statistics()
```

#### **4. Simulator (PRESENTE - DA RISCRIVERE)**
```
src/simulation/simulator.py (344 righe) ⚠️
└── MABSimulator
    └── run_simulation()           → Per Opzione 2 (KBRS vs Baseline)

⚠️ DA RISCRIVERE: Cambiare logica per Opzione 3
```

#### **5. Reward System (PRESENTE - DA MODIFICARE)**
```
src/simulation/reward_system.py (455 righe) ⚠️
└── AdvancedRewardSystem
    └── compute_reward()           → KBRS vs Baseline

⚠️ DA MODIFICARE: Exploration vs Exploitation
```

#### **6. Visualization (OK)**
```
src/viz/plot_manager.py (245 righe) ✅
└── plot_mab_performance()         → Usato dal notebook

✅ DA USARE: Non riscrivere, estendere se necessario
```

---

## 🎯 **ARCHITETTURA TARGET - Opzione 3**

```
OFFLINE:
Dataset
    ↓ (Preprocessing - NON TOCCARE)
Preprocessed Data (cosine_sim_matrix, movie_catalog)
    ↓ (NEW)
Train/Val/Test Split
    ↓ (NEW)
KBRS Training
    ↓ (NEW - estensione kbrs.py)
Trained KBRS Model

ONLINE:
User Request
    ↓ (NEW)
MAB (Exploration vs Exploitation)
    ├── Arm 0: EXPLORATION (nuovi film)
    └── Arm 1: EXPLOITATION (film simili)
    ↓ (MODIFY - aggiungere strategy param)
KBRS.recommend(strategy='exploration'|'exploitation')
    ↓
User Feedback → Reward
    ↓ (MODIFY)
MAB.update(user_id, strategy, reward)
```

---

## 📋 **PIANO DI IMPLEMENTAZIONE**

### **FASE 1: Train/Validation/Test Split (DAY 1-2)**

#### **Task 1.1: Creare Split Manager**
```bash
# File: src/data/split_manager.py
# Linee: ~80
```

**DA FARE:**
- [ ] Funzione: `create_train_val_test_split(ratings_df, test_size=0.1, val_size=0.1)`
- [ ] Split stratificato per utenti (ogni utente deve avere rating in tutti e 3 i set)
- [ ] Salvare su `datasets/splits/`
- [ ] Verifica: ogni utente ha almeno 3 rating per set
- [ ] Test unitario

**SPECIFICHE:**
```python
def create_train_val_test_split(
    ratings_df: pd.DataFrame,
    test_size: float = 0.1,
    val_size: float = 0.1,
    random_state: int = 42,
    min_ratings_per_user: int = 3
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns: (train_df, val_df, test_df)
    """
```

#### **Task 1.2: Creare KBRS Trainer**
```bash
# File: src/recommender/kbrs_trainer.py (NUOVO)
# Linee: ~150
```

**DA FARE:**
- [ ] Classe: `KBRSTrainer`
- [ ] Metodo: `train(train_df, movie_catalog, cosine_sim_matrix)`
- [ ] Metodo: `validate(val_df, trainer)`
- [ ] Metodo: `test(test_df, trainer)`
- [ ] Metriche: RMSE, MAE, Precision@K, NDCG@K
- [ ] Salvare modello addestrato

**SPECIFICHE:**
```python
class KBRSTrainer:
    def __init__(self, movie_catalog: pd.DataFrame, cosine_sim_matrix: np.ndarray):
        self.kbrs = KBRS(movie_catalog)
        self.cosine_sim_matrix = cosine_sim_matrix

    def train(self, train_df: pd.DataFrame):
        """Costruisce user profiles da train set"""

    def predict(self, user_id: int, movie_id: int) -> float:
        """Predice rating usando KBRS"""

    def validate(self, val_df: pd.DataFrame) -> dict:
        """Metriche su validation set"""

    def test(self, test_df: pd.DataFrame) -> dict:
        """Metriche su test set"""
```

#### **Task 1.3: Notebook Offline Evaluation**
```bash
# File: offline_evaluation.ipynb (NUOVO - separato dal main)
# Linee: ~50 celle
```

**DA FARE:**
- [ ] Carica dati (usa PreprocessingPipeline)
- [ ] Split train/val/test
- [ ] Train KBRS
- [ ] Validazione parametri (k_similar)
- [ ] Test finale
- [ ] Report risultati
- [ ] Plot metriche (usa plot_manager.py)

---

### **FASE 2: MAB Exploration vs Exploitation (DAY 3-5)**

#### **Task 2.1: Riscrivere MAB Manager**
```bash
# File: src/bandit/exploration_mab.py (NUOVO - sostituisce mab_manager.py logic)
# Linee: ~120
```

**DA FARE:**
- [ ] Classe: `ExplorationExploitationMAB`
- [ ] Usa `AdvancedThompsonSampling` internamente
- [ ] Due arms:
  - Arm 0: `EXPLORATION`
  - Arm 1: `EXPLOITATION`
- [ ] Metodo: `select_strategy(user_id)`
- [ ] Metodo: `register_feedback(user_id, strategy, reward)`
- [ ] Statistiche per-utente

**SPECIFICHE:**
```python
class ExplorationExploitationMAB:
    def __init__(self):
        self.user_mabs: Dict[int, AdvancedThompsonSampling] = {}
        # Per ogni utente, un Thompson Sampling separato

    def select_strategy(self, user_id: int) -> str:
        """Returns 'exploration' or 'exploitation'"""

    def register_feedback(self, user_id: int, strategy: str, reward: int):
        """Aggiorna preferenze utente"""

    def get_user_stats(self, user_id: int) -> dict:
        """Statistiche per utente: exploration_rate, exploitation_rate"""
```

#### **Task 2.2: Estendere KBRS per Supportare Strategy**
```bash
# File: src/recommender/kbrs.py (MODIFICARE - aggiungere metodi)
# Linee: +30 (non riscrivere!)
```

**DA FARE:**
- [ ] Modificare: `recommend_movies_hybrid(user_id, ratings_df, ..., strategy='exploitation')`
- [ ] Nuovo metodo: `recommend_for_exploration(user_id, seen_movies, n=10)`
- [ ] Nuovo metodo: `recommend_for_exploitation(user_id, seen_movies, n=10)`

**LOGICA EXPLORATION:**
- Film con similarità BASSA (diversi da quelli visti)
- Filter: `similarity < threshold` (es. 0.3)
- Priorità a film con feature diverse

**LOGICA EXPLOITATION:**
- Film con similarità ALTA (simili a quelli visti)
- Filter: `similarity > threshold` (es. 0.7)
- Priorità a film con feature simili

#### **Task 2.3: Modificare Reward System**
```bash
# File: src/simulation/reward_system.py (MODIFICARE)
# Linee: ~50 modifiche
```

**DA FARE:**
- [ ] Cambiare logica: non più KBRS vs Baseline
- [ ] Reward alto se utente apprezza strategia scelta
- [ ] Reward basso se utente rifiuta strategia
- [ ] Tracking preferenze per-utente

**SPECIFICHE:**
```python
def compute_reward(
    self,
    user_id: int,
    strategy: str,  # 'exploration' or 'exploitation'
    recommendations: List[Tuple[int, float]],
    actual_rating: float = None
) -> float:
    """
    Reward basato su:
    1. User appreciated exploration? → high reward for exploration
    2. User appreciated exploitation? → high reward for exploitation
    """
```

#### **Task 2.4: Aggiungere Altri Algoritmi MAB**
```bash
# File: src/bandit/algorithms.py (ESTENDERE)
# Linee: +150
```

**DA FARE:**
- [ ] Classe: `EpsilonGreedy` (semplice)
- [ ] Classe: `UCB1` (Upper Confidence Bound)
- [ ] Config unificata per tutti gli algoritmi
- [ ] Benchmark tra algoritmi

**SPECIFICHE:**
```python
@dataclass
class MABConfig:
    algorithm: str = 'thompson'  # 'thompson', 'epsilon', 'ucb'
    epsilon: float = 0.1
    confidence_level: float = 2.0
```

---

### **FASE 3: Simulatore e Pipeline (DAY 6-7)**

#### **Task 3.1: Riscrivere Simulator**
```bash
# File: src/simulation/simulator.py (MODIFICARE MAESTRAMENTE)
# Linee: ~100 modifiche
```

**DA FARE:**
- [ ] Mantenere interfaccia: `run_simulation()`
- [ ] Cambiare logica interna:
  - Per ogni utente: MAB sceglie strategy
  - KBRS genera raccomandazioni per strategy
  - Reward → MAB aggiorna
- [ ] Supportare più algoritmi MAB
- [ ] Tracking per-utente

**FLOW:**
```
for user_id in users:
    strategy = mab.select_strategy(user_id)
    recs = kbrs.recommend(user_id, strategy, n=10)
    reward = reward_system.compute(user_id, strategy, recs, actual_rating)
    mab.register_feedback(user_id, strategy, reward)
```

#### **Task 3.2: Estendere Plot Manager**
```bash
# File: src/viz/plot_manager.py (ESTENDERE)
# Linee: +50
```

**DA FARE:**
- [ ] Nuovo plot: `plot_exploration_exploitation_rates(history_df)`
- [ ] Nuovo plot: `plot_user_preferences(user_stats)`
- [ ] Nuovo plot: `plot_mab_algorithm_comparison(results_dict)`
- [ ] Consolidare tutti i plot in un unico file

#### **Task 3.3: Notebook MAB Completo**
```bash
# File: mab_exploration_exploitation.ipynb (MODIFICARE kbrs_final_working.ipynb)
# Linee: ~100
```

**DA FARE:**
- [ ] Sezione 1: Preprocessing (NON CAMBIARE - usa pipeline esistente)
- [ ] Sezione 2: Train/Val/Test (usa split_manager + kbrs_trainer)
- [ ] Sezione 3: MAB Online (usa exploration_mab)
- [ ] Sezione 4: Risultati (usa plot_manager esteso)
- [ ] ELIMINARE codice inline - usare sempre src/

**VINCOLO**: Notebook deve rimanere minimal, codice in src/

---

### **FASE 4: Backend Per Frontend (DAY 8)**

#### **Task 4.1: API Backend**
```bash
# File: src/api/recommender_api.py (NUOVO)
# Linee: ~80
```

**DA FARE:**
- [ ] Classe: `RecommenderAPI`
- [ ] Metodo: `preprocess_data()`
- [ ] Metodo: `train_model()`
- [ ] Metodo: `get_recommendation(user_id, n=10)`
- [ ] Metodo: `simulate_mab(n_iterations)`
- [ ] JSON serializable

**SPECIFICHE:**
```python
class RecommenderAPI:
    def __init__(self):
        self.pipeline = PreprocessingPipeline()
        self.trainer = None
        self.mab = None

    def get_recommendation(self, user_id: int, n: int = 10) -> dict:
        """
        Returns:
        {
            'user_id': int,
            'recommendations': List[dict],
            'strategy': str,  # exploration or exploitation
            'explanation': str
        }
        """
```

#### **Task 4.2: Configurazione Unificata**
```bash
# File: src/config.py (NUOVO)
# Linee: ~50
```

**DA FARE:**
- [ ] PreprocessingConfig (già esiste in pipeline)
- [ ] TrainingConfig
- [ ] MABConfig (per algoritmo)
- [ ] EvaluationConfig (metriche)
- [ ] Exportabile da CLI e notebook

---

## 🔧 **STRUTTURA FILE FINALE**

```
src/
├── data/
│   ├── manager.py                     ✅ (NON TOCCARE)
│   ├── sparql.py                      ✅
│   ├── mapping.py                     ✅
│   ├── templates.py                   ✅
│   └── split_manager.py               🆕
├── recommender/
│   ├── kbrs.py                        📝 (estendere)
│   └── kbrs_trainer.py                🆕
├── bandit/
│   ├── algorithms.py                  📝 (aggiungere Epsilon, UCB)
│   └── exploration_mab.py             🆕 (sostituisce logica mab_manager)
├── simulation/
│   ├── simulator.py                   📝 (riscrivere logica)
│   └── reward_system.py               📝 (modificare per strategy)
├── viz/
│   └── plot_manager.py                📝 (estendere plot)
├── api/
│   └── recommender_api.py             🆕
├── config.py                          🆕
└── experiments/
    └── mab_experiment.py              ✅ (probabilmente non usato)
```

---

## 📊 **COMPATIBILITÀ NOTEBOOK**

### **VINCOLI:**
1. ✅ Preprocessing: NON TOCCARE (funziona, usato dal notebook)
2. ✅ import da src/: Mantenere tutte le import esistenti
3. ⚠️ Nuovi import: Aggiungere senza rompere quelli vecchi
4. 📝 Modificare kbrs_final_working.ipynb: Rimpiazzare sezione MAB con Opzione 3
5. ❌ Codice inline: ZERO! Tutto in src/

### **STRATEGIA MIGRATION:**
```python
# PRIMA (Opzione 2 - KBRS vs Baseline):
from src.bandit.mab_manager import MABManager
mab = MABManager(['KBRS_Hybrid', 'Popularity_Baseline'])

# DOPO (Opzione 3 - Exploration vs Exploitation):
from src.bandit.exploration_mab import ExplorationExploitationMAB
mab = ExplorationExploitationMAB()
# user_mabs = {} per ogni utente
```

---

## ⏱️ **TIMELINE DETTAGLIATA**

| Day | Task | Deliverable | Hours |
|-----|------|-------------|-------|
| 1 | Task 1.1 | split_manager.py | 4h |
| 1 | Task 1.2 | kbrs_trainer.py | 4h |
| 2 | Task 1.3 | offline_evaluation.ipynb | 6h |
| 3 | Task 2.1 | exploration_mab.py | 6h |
| 4 | Task 2.2 | kbrs.py (esteso) | 5h |
| 4 | Task 2.3 | reward_system.py (modificato) | 3h |
| 5 | Task 2.4 | algorithms.py (Epsilon + UCB) | 6h |
| 6 | Task 3.1 | simulator.py (riscritto) | 6h |
| 6 | Task 3.2 | plot_manager.py (esteso) | 3h |
| 7 | Task 3.3 | mab_exploration_exploitation.ipynb | 6h |
| 8 | Task 4.1 | recommender_api.py | 5h |
| 8 | Task 4.2 | config.py | 2h |

**TOTALE: 8 giorni (~50 ore)**

---

## 🚀 **PROSSIMA ESECUZIONE**

### **STEP 1: Iniziare Day 1**
Lancia la prossima esecuzione con:

```
Task: Implementare Train/Validation/Test Split

Istruzioni:
1. Crea src/data/split_manager.py
2. Implementa create_train_val_test_split()
3. Testa con un subset di dati
4. Verifica che ogni utente abbia rating in tutti e 3 i set
5. Crea test unitario

Vincoli:
- NON toccare src/data/manager.py (funziona bene)
- NON toccare src/pipeline/preprocessing.py
- Mantieni compatibilità con notebook esistente
```

### **VERIFICA STEP 1:**
```python
# Test rapido:
from src.data.split_manager import create_train_val_test_split
from src.data.manager import load_data

ratings_df, _, _ = load_data()
train, val, test = create_train_val_test_split(ratings_df)
print(f"Train: {len(train)}, Val: {len(val)}, Test: {len(test)}")
# Verifica che ogni utente sia in tutti e 3 i set
```

---

## ⚠️ **DECISIONI CRITICHE**

### **1. Mantenere Compatibilità Notebook?**
- ✅ SI - Il notebook deve funzionare sempre
- ✅ SI - Preferisco migration graduale
- ✅ SI - Creo nuovo notebook invece di modificare quello vecchio

### **2. Performance vs Semplicità?**
- ✅ Semplicità prima - codice leggibile
- ✅ Performance OK cosi com'è (già ottimizzato KBRS)
- ✅ Focus su correttezza architetturale

### **3. Test Coverage?**
- ✅ Test unitari per split_manager
- ✅ Test di integrazione per pipeline completa
- ✅ Test manuale nel notebook

---

## 📝 **NOTE PER FUTURE ITERAZIONI**

1. **UCB Implementation**: UCB1 semplice, non serve UCB-V
2. **Epsilon-Greedy**: Per confronto con Thompson
3. **Reward Shaping**: Possibile miglioramento futuro
4. **Cold-Start**: Per utenti nuovi (da implementare dopo)
5. **Streamlit Frontend**: Usare RecommenderAPI come backend

---

**🎬 PRONTO PER IMPLEMENTAZIONE!**

*Creato: 2025-11-18*
*Per esecuzione con skill: superpowers:executing-plans*
