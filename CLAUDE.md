# CineWisdom - Opzione 3: MAB per Exploration vs Exploitation

## 🎯 **OBIETTIVO ARCHITETTURALE**

**Implementare l'Opzione 3**: KBRS addestrato offline + MAB per decidere exploration vs exploitation per ogni utente.

---

## 📊 **STATO ATTUALE (DOPO PULIZIA)**

### ✅ **Pulizia Completata**
- `src2/` eliminato (era vuoto, tentativo refactoring fallito)
- `src/` PULITO con 21 file Python organizzati
- Backup: `src2_backup_20251118/`

### 📁 **Struttura src/ (Funzionante)**
```
src/
├── recommender/kbrs.py         [KBRS Engine - content-based]
├── bandit/mab_manager.py       [MAB Manager - Thompson Sampling]
├── bandit/algorithms.py        [Advanced Thompson Sampling]
├── simulation/simulator.py     [Simulazione MAB]
├── simulation/reward_system.py [Reward System]
├── data/manager.py             [Wikidata + DBpedia + SVD]
├── analysis/pondered_reward.py [Analisi reward]
└── viz/plot_manager.py         [Visualizzazioni]
```

---

## 🎯 **ARCHITETTURA TARGET - Opzione 3**

### **OFFLINE (Addestramento)**
```
Dataset → Train/Validation/Test Split
    ↓
Train: Costruire profili utente dal KBRS
    ↓
Validation: Trovare parametri ottimali (k_similar, soglie, etc.)
    ↓
Test: Valutare accuratezza KBRS
```

### **ONLINE (MAB per Exploration vs Exploitation)**
```
Per ogni utente che arriva:
├── MAB decide: "Esplorare" o "Sfruttare"?
│   ├── EXPLORATION: Film nuovi (rischioso, scoperta)
│   └── EXPLORATION: Film simili a quelli visti (sicuro)
├── KBRS genera raccomandazioni
└── Reward feedback → MAB impara preferenze utente
```

---

## 🏗️ **REFACTORING NECESSARIO**

### **1. Addestramento Offline (DA FARE)**

#### **Step 1a: Train/Validation/Test Split**
```python
# src/data/split_manager.py
def create_train_val_test_split(ratings_df):
    """Split 80/10/10 stratificato per utenti"""
    # Implementare split bilanciato
```

#### **Step 1b: KBRS Training**
```python
# src/recommender/kbrs_trainer.py
class KBRSTrainer:
    def train(self, train_df):
        """Addestra KBRS su train set"""
        # Costruisce user profiles
        # Calcola similarity matrix
        # Salva modello

    def validate(self, val_df):
        """Valida su validation set"""
        # RMSE, MAE, Precision@K

    def test(self, test_df):
        """Test finale su test set"""
        # Metriche finali
```

### **2. MAB per Exploration vs Exploitation (DA RISCRIVERE)**

#### **Current (SBAGLIATO - Opzione 2):**
```python
# Attualmente MAB sceglie tra:
# - KBRS_Hybrid (Arm 0)
# - Popularity_Baseline (Arm 1)
```

#### **Target (CORRETTO - Opzione 3):**
```python
# src/bandit/exploration_mab.py
class ExplorationExploitationMAB:
    def __init__(self):
        # Due arms:
        # - Arm 0: EXPLORATION (film nuovi, non simili)
        # - Arm 1: EXPLOITATION (film simili a quelli visti)

    def select_strategy(self, user_id):
        """Decide se esplorare o sfruttare per questo utente"""
        # Thompson Sampling decide

    def register_feedback(self, user_id, reward):
        """Aggiorna preferenze utente"""
        # Se reward alto → conferma strategia
        # Se reward basso → cambia strategia
```

### **3. KBRS Modificato (DA AGGIORNARE)**

#### **Current:**
```python
# src/recommender/kbrs.py
def recommend_movies_hybrid(self, user_id, n=10):
    # Trova sempre film simili (solo exploitation)
```

#### **Target:**
```python
def recommend_movies_hybrid(self, user_id, strategy, n=10):
    if strategy == 'exploration':
        # Trova film DIVERSI da quelli visti
        # Maggiore distanza nella similarità
    else:  # exploitation
        # Trova film SIMILI a quelli visti
        # Minore distanza nella similarità
```

---

## 📋 **PIANO DI IMPLEMENTAZIONE**

### **Fase 1: Addestramento Offline (2-3 giorni)**

#### **Step 1: Implementare Train/Val/Test Split**
- [ ] Creare `src/data/split_manager.py`
- [ ] Split stratificato 80/10/10
- [ ] Verificare che ogni utente abbia abbastanza rating

#### **Step 2: KBRS Trainer**
- [ ] Creare `src/recommender/kbrs_trainer.py`
- [ ] Metodi: `train()`, `validate()`, `test()`
- [ ] Metriche: RMSE, MAE, Precision@K, NDCG@K

#### **Step 3: Esperimento Offline**
- [ ] Notebook: `offline_evaluation.ipynb`
- [ ] Addestrare KBRS su train
- [ ] Validare iperparametri su val
- [ ] Test finale su test
- [ ] Documentare risultati

### **Fase 2: MAB Exploration vs Exploitation (2-3 giorni)**

#### **Step 4: Riscrivere MAB**
- [ ] Eliminare `src/bandit/mab_manager.py` (era per Opzione 2)
- [ ] Creare `src/bandit/exploration_mab.py` (Opzione 3)
- [ ] Due arms: exploration vs exploitation
- [ ] Thompson Sampling per scegliere strategia

#### **Step 5: Modificare KBRS**
- [ ] Aggiungere parametro `strategy` a `recommend_movies_hybrid()`
- [ ] Logica exploration: film diversi (distanza > threshold)
- [ ] Logica exploitation: film simili (distanza < threshold)

#### **Step 6: Reward System**
- [ ] Modificare `src/simulation/reward_system.py`
- [ ] Reward alto se utente apprezza strategia scelta
- [ ] MAB impara preferenze individuali

### **Fase 3: Simulazione Online (1-2 giorni)**

#### **Step 7: Simulatore MAB**
- [ ] Modificare `src/simulation/simulator.py`
- [ ] Per ogni utente: MAB sceglie strategia
- [ ] KBRS genera raccomandazioni
- [ ] Reward feedback → MAB aggiorna

#### **Step 8: Notebook Completo**
- [ ] Notebook: `mab_exploration_exploitation.ipynb`
- [ ] Pipeline completa: offline + online
- [ ] Visualizzazioni: learning curves, performance

### **Fase 4: Validazione (1 giorno)**

#### **Step 9: Esperimento Completo**
- [ ] Addestramento offline
- [ ] Simulazione online
- [ ] Confronto: exploration vs exploitation rate
- [ ] Analisi: quali utenti preferiscono esplorare?

---

## 🔧 **FILE DA CREARE/MODIFICARE**

### **Nuovi File:**
```
src/
├── data/
│   └── split_manager.py         [NUOVO]
├── recommender/
│   ├── kbrs_trainer.py          [NUOVO]
│   └── kbrs.py                  [MODIFICARE]
└── bandit/
    └── exploration_mab.py       [NUOVO - sostituisce mab_manager.py]
```

### **File da Modificare:**
```
src/
├── simulation/
│   ├── reward_system.py         [MODIFICARE]
│   └── simulator.py             [MODIFICARE]
├── data/
│   └── manager.py               [GIÀ HA TUTTO]
└── analysis/
    └── pondered_reward.py       [MODIFICARE per nuove metriche]
```

---

## 🎯 **RISULTATO ATTESO**

### **Offline (Addestramento)**
```
Train (80%): 10,000+ interazioni
Validation (10%): 1,200+ interazioni
Test (10%): 1,200+ interazioni

KBRS Performance:
- RMSE: <0.85
- Precision@10: >0.15
- NDCG@10: >0.25
```

### **Online (MAB)**
```
Per ogni utente:
- MAB impara preferenze individuali
- Alcuni utenti: preferiscono exploration (60% nuovi film)
- Altri utenti: preferiscono exploitation (80% film simili)
- Convergenza in 100-200 iterazioni
```

### **Benefici Opzione 3**
✅ **Personalizzazione reale**: Ogni utente ha strategia ottimale
✅ **No cold-start**: MAB impara preferenze online
✅ **Bilanciamento automatico**: Sistema trova equilibrio exploration/exploitation
✅ **Interpretazione chiara**: Sappiamo perché MAB sceglie

---

## 🚀 **AVVIO IMPLEMENTAZIONE**

### **Next Step: Step 1**
Iniziamo con l'implementare train/validation/test split:

```bash
jupyter notebook
# Crea: offline_evaluation.ipynb
# Implementa: src/data/split_manager.py
# Obiettivo: Addestrare KBRS offline
```

**Tempo stimato totale: 6-8 giorni**

---

## 📚 **RIFERIMENTI**

### **Paper da Leggere:**
1. "Multi-Armed Bandits for Online Recommendation" - Li et al.
2. "Exploration-Exploitation in Recommender Systems" - Said & Berkovsky
3. "Thompson Sampling for Contextual Bandits" - Agrawal & Goyal

### **Implementazioni Esistenti:**
- `src/bandit/algorithms.py`: Già ha AdvancedThompsonSampling (perfetto!)
- `src/data/manager.py`: Già ha Wikidata + DBpedia + SVD
- `src/simulation/reward_system.py`: Da modificare per Opzione 3

---

## ⚠️ **NOTE IMPORTANTI**

1. **KBRS attuale NON è addestrato**: Usa solo similarità content-based
2. **MAB attuale sceglie algoritmo**: SBAGLIATO per Opzione 3
3. **Reward system deve cambiare**: Da KBRS vs Baseline → Exploration vs Exploitation
4. **Simulatore deve essere riscritto**: Per gestire strategie per-utente

---

**🎬 PRONTO PER L'IMPLEMENTAZIONE!**

*Ultimo aggiornamento: 2025-11-18*
*Versione: 4.0 - Opzione 3: MAB Exploration vs Exploitation*
