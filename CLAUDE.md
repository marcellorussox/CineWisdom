# CLAUDE.md - Guida Completa per CineWisdom (Versione Corretta)

## 📋 Indice

1.  [Panoramica del Progetto](https://www.google.com/search?q=%23panoramica-del-progetto)
2.  [🎯 Le Due Metodologie di Valutazione (Come da Progetto)](https://www.google.com/search?q=%23-le-due-metodologie-di-valutazione-come-da-progetto)
3.  [Struttura del Progetto](https://www.google.com/search?q=%23struttura-del-progetto)
4.  [Pipeline di Esecuzione - DUAL EVALUATION](https://www.google.com/search?q=%23pipeline-di-esecuzione---dual-evaluation)
5.  [Gestione Dataset - Implementazione Split](https://www.google.com/search?q=%23gestione-dataset---implementazione-split)
6.  [File Chiave](https://www.google.com/search?q=%23file-chiave)
7.  [Configurazioni](https://www.google.com/search?q=%23configurazioni)
8.  [Stato Attuale e Roadmap](https://www.google.com/search?q=%23stato-attuale-e-roadmap)
9.  [Vantaggi Dual-Evaluation](https://www.google.com/search?q=%23vantaggi-dual-evaluation)
10. [Troubleshooting](https://www.google.com/search?q=%23troubleshooting)

-----

## 🎬 Panoramica del Progetto

### Cos'è CineWisdom

**CineWisdom** è un sistema di raccomandazione avanzato che combina:

  - **KBRS (Knowledge-Based Recommender System)**: Sistema personalizzato basato su similarità semantica (usando DBpedia).
  - **Multi-Armed Bandit (MAB)**: Algoritmo (es. LinUCB) per mediare il dilemma exploration vs. exploitation usando il "contesto" (le feature KBRS).

### Obiettivo Principale

Implementare e valutare un Recommender System ibrido (KBRS+MAB) utilizzando le **due metodologie di verifica sperimentale** richieste:

1.  **Valutazione Offline Tradizionale**: Per misurare l'**accuratezza** predittiva del motore KBRS.
2.  **Valutazione Online Simulata (MAB)**: Per misurare l'**efficacia** della strategia MAB nell'imparare e massimizzare le ricompense nel tempo.

-----

## 🎯 Le Due Metodologie di Valutazione (Come da Progetto)

Come da specifiche, il progetto richiede due framework di verifica distinti. **Non sono uno la correzione dell'altro**, ma due esperimenti paralleli che rispondono a domande diverse.

### 1\. Valutazione Offline (Accuratezza Predittiva)

  - **Logica**: Metodologia ML standard.
  - **Domanda a cui risponde**: "Quanto è *accurato* il mio motore KBRS (basato su DBpedia) nel predire i rating degli utenti *a freddo*?"
  - **Come si implementa**:
      - Si prende il dataset e si fa uno **split statico** (es. 80% Train / 10% Validation / 10% Test).
      - Si "allena" il modello sul **Train Set** (es. costruendo i profili utente).
      - Si fa "tuning" degli iperparametri (es. `k` della similarità) sul **Validation Set**.
      - Si "testa" il modello finale sul **Test Set**, per la valutazione definitiva.
  - **Metriche Chiave**: **RMSE**, **MAE**, Precision@K, NDCG@K.

### 2\. Valutazione Online (Efficacia MAB)

  - **Logica**: Simulazione di un ambiente "live" (detta *Replay Evaluation*).
  - **Domanda a cui risponde**: "Quanto è *efficace* la mia strategia MAB (es. LinUCB) nell'imparare i gusti dell'utente nel tempo per massimizzare le ricompense (es. i 'click' o i rating alti)?"
  - **Come si implementa**:
      - Si prende **TUTTO** il dataset e lo si **ordina per timestamp** (dal più vecchio al più recente).
      - **Nessuno split\!** Si inizializza il modello MAB "vuoto" (senza conoscenza).
      - Si itera sul dataset un evento alla volta, in ordine cronologico.
      - Ad ogni evento `(utente, film, rating)`:
        1.  Il MAB usa il contesto (feature utente/film) per **decidere** quale film `J` raccomandare.
        2.  Si confronta `J` con il film reale dell'evento (`film`).
        3.  Se `J == film`, si registra una **ricompensa** e il modello MAB si **aggiorna** (impara).
        4.  Se `J != film`, l'evento si ignora (non si può sapere la ricompensa).
  - **Metriche Chiave**: **Cumulative Reward** (Ricompensa Cumulata nel tempo), **CTR (simulato)**.

-----

## 🏗️ Struttura del Progetto

```
📦 CineWisdom/
├── 📓 1_Valutazione_Offline_Accuratezza.ipynb  # Notebook per Metodologia 1
├── 📓 2_Valutazione_Online_MAB.ipynb         # Notebook per Metodologia 2
├── 📁 src/                                  # Codice modulare
│   ├── recommender/
│   │   └── kbrs_engine.py                  # Logica KBRS (profilo utente, similarità)
│   ├── bandit/
│   │   ├── mab_algorithms.py               # Algoritmi (es. LinUCB, UCB1)
│   │   └── mab_simulator.py                # Logica per la simulazione Replay
│   ├── data/
│   │   ├── data_manager.py                 # Caricamento e arricchimento dati
│   │   ├── sparql_manager.py               # Query DBpedia
│   │   └── split_manager.py            # [SOLO PER MET. 1] Split train/val/test
│   ├── evaluation/
│   │   └── traditional_evaluator.py    # [SOLO PER MET. 1] Metriche (RMSE, MAE...)
│   └── ...
├── 📁 datasets/
│   ├── raw/                                # MovieLens dataset
│   ├── processed/                          # Dati arricchiti con DBpedia
│   └── splits/                         # [SOLO PER MET. 1] Train/Val/Test
│       ├── train_set.csv
│       ├── val_set.csv
│       └── test_set.csv
├── 📁 plots/                                # Grafici (RMSE, Cumulative Reward...)
├── 📄 requirements.txt
└── 📄 README.md
```

-----

## 🔄 Pipeline di Esecuzione - DUAL EVALUATION

L'approccio "Dual Mode" consiste nell'eseguire **due pipeline separate** per rispondere alle due domande del progetto.

### Modalità 1: Traditional ML (Valutazione Offline di Accuratezza)

```
Obiettivo: Valutare l'accuratezza del KBRS (Notebook 1)

Workflow:
1. Caricamento dati MovieLens + Dati DBpedia arricchiti.
2. Esecuzione `split_manager.py`:
   - Split dei rating in **Train (80%)**, **Validation (10%)** e **Test (10%)**.
3. Training Phase:
   - Costruzione profili utente usando *solo* il Train Set.
   - (Opzionale) Training di un modello Baseline (es. Popularity) *solo* sul Train Set.
4. Validation Phase (Tuning):
   - Si usano le interazioni nel Validation Set per testare diverse configurazioni
     (es. k_similar, pesi delle feature) e scegliere la migliore.
5. Test Phase (Valutazione Finale):
   - Si usa il modello con la configurazione migliore scelta nella fase 4.
   - Si predicono i rating per le interazioni nel Test Set (mai visto prima).
6. Valutazione:
   - Esecuzione `traditional_evaluator.py`:
   - Calcolo metriche: **RMSE, MAE, Precision@K**
7. Report: Confronto KBRS vs Baseline su queste metriche statiche.
```

### Modalità 2: MAB Simulation (Valutazione Online di Efficacia)

```
Obiettivo: Valutare l'efficacia della strategia MAB (Notebook 2)

Workflow:
1. Caricamento dati MovieLens + Dati DBpedia arricchiti (**TUTTI**).
2. **Ordinamento temporale** di tutti i rating (dal più vecchio al più recente).
3. Setup `mab_simulator.py`:
   - Inizializzazione degli algoritmi MAB (es. LinUCB, EpsilonGreedy) *senza* pre-training.
   - Definizione della "Ricompensa" (es. `1` se rating > 4, `0` altrimenti).
4. Loop di Simulazione (Replay):
   - Per ogni evento `(utente, film, rating)` nello stream ordinato:
     a. Fornisci il "contesto" (feature utente/film) al MAB.
     b. Il MAB **sceglie** un film `J` da raccomandare.
     c. **Valutazione**:
        - IF `J == film`:
            - Calcola la ricompensa (es. `1`).
            - Registra la ricompensa.
            - **Aggiorna il modello MAB** con questa nuova informazione.
        - ELSE:
            - Ignora l'evento (nessuna ricompensa, nessun aggiornamento).
5. Valutazione:
   - Plot del **Cumulative Reward** nel tempo per ogni algoritmo MAB.
   - Calcolo del **CTR (simulato)**.
6. Report: Confronto delle curve di apprendimento dei diversi algoritmi MAB.
```

-----

## 💾 Gestione Dataset - Implementazione Split

### Stato Attuale

  - **Dataset**: MovieLens ml-latest-small
  - **Approccio**: Dati usati in blocco.

### Implementazione NUOVA: `split_manager.py` (Solo per Modalità 1)

Questo modulo è **FONDAMENTALE** per la Valutazione Offline (Modalità 1) e **NON DEVE ESSERE USATO** per la Valutazione MAB (Modalità 2).

#### Funzione Split Manager

```python
# src/data/split_manager.py
import pandas as pd
from sklearn.model_selection import train_test_split

def create_traditional_split(
    ratings_df: pd.DataFrame,
    test_size: float = 0.1,
    val_size: float = 0.1,
    random_state: int = 42,
    min_ratings_per_user: int = 3
) -> tuple:
    """
    Crea split train/validation/test stratificato.
    Restituisce (train_df, val_df, test_df)
    """

    # Filtra utenti con pochi rating
    user_counts = ratings_df['userId'].value_counts()
    valid_users = user_counts[user_counts >= min_ratings_per_user].index
    filtered_df = ratings_df[ratings_df['userId'].isin(valid_users)]

    # Primo split: train + temp (val+test)
    # Calcola la dimensione totale di val+test
    val_test_size = val_size + test_size 
    
    train_df, temp_df = train_test_split(
        filtered_df,
        test_size=val_test_size,
        stratify=filtered_df['userId'],
        random_state=random_state
    )

    # Secondo split: val + test dal temp
    # Calcola la proporzione di test rispetto al set temporaneo
    relative_test_size = test_size / val_test_size

    val_df, test_df = train_test_split(
        temp_df,
        test_size=relative_test_size,
        stratify=temp_df['userId'],
        random_state=random_state
    )

    return train_df, val_df, test_df
```

-----

## 🔑 File Chiave

### File ESISTENTI (da riorganizzare)

#### 1\. `src/recommender/kbrs_engine.py`

  - **Cosa fa**: Logica KBRS. Funzioni per `build_user_profile()` e `get_similarity()`.
  - **Usato da**: Entrambe le modalità.

#### 2\. `src/bandit/mab_algorithms.py`

  - **Cosa fa**: Implementazione degli algoritmi MAB (es. **LinUCB**, EpsilonGreedy).
  - **Logica**: `choose_arm()` (decisione), `update()` (apprendimento).
  - **Usato da**: Modalità 2.

#### 3\. `src/data/data_manager.py`

  - **Cosa fa**: Caricamento dati MovieLens + arricchimento da DBpedia.
  - **Usato da**: Entrambe le modalità (come primo step).

### File NUOVI (da creare)

#### 1\. `src/data/split_manager.py`

  - **Cosa fa**: Gestione split train/val/test tradizionale (vedi sopra).
  - **Usato da**: **Solo Modalità 1**.

#### 2\. `src/evaluation/traditional_evaluator.py`

  - **Cosa fa**: Calcolo metriche ML standard.
  - **Metriche**: `rmse()`, `mae()`, `precision_at_k()`, `ndcg_at_k()`.
  - **Usato da**: **Solo Modalità 1**.

#### 3\. `src/bandit/mab_simulator.py`

  - **Cosa fa**: Contiene il loop di simulazione "Replay".
  - **Logica**: Ordina per tempo, itera, chiama `choose_arm()`, valuta la ricompensa, chiama `update()`.
  - **Usato da**: **Solo Modalità 2**.

-----

## ⚙️ Configurazioni

### Configurazione Traditional Split (per Modalità 1)

```python
# config.py
SPLIT_CONFIG = {
    "train_size": 0.8,
    "val_size": 0.1,
    "test_size": 0.1,
    "min_ratings_per_user": 5,
    "random_state": 42,
    "stratify_by": "userId",
}

EVALUATION_CONFIG = {
    "top_k": 10,
    "metrics": ["rmse", "mae", "precision@10", "ndcg@10"],
    "rating_threshold": 3.5, # Soglia per definire "rilevante"
}
```

### Configurazione MAB Simulation (per Modalità 2)

```python
# config.py
MAB_CONFIG = {
    "algorithms": ["LinUCB", "EpsilonGreedy", "Random"],
    "epsilon": 0.1, # Per EpsilonGreedy
    "alpha": 1.0,   # Per LinUCB
    "reward_threshold": 4.0, # Rating > 4.0 = Ricompensa '1'
}
```

-----

## 📊 Stato Attuale e Roadmap

### Stato Attuale

L'infrastruttura di base (caricamento dati, KBRS) è parzialmente presente. Manca una chiara separazione delle due metodologie di valutazione.

### Roadmap Implementazione (PIANO CORRETTO)

#### Fase 1: Sviluppo Componenti Core

  - [ ] Finalizzare `data_manager.py` per caricare MovieLens.
  - [ ] Finalizzare `sparql_manager.py` per arricchire i film da DBpedia.
  - [ ] Implementare `kbrs_engine.py` (costruzione profilo utente e calcolo similarità).

#### Fase 2: Implementazione Valutazione Offline (Notebook 1)

  - [ ] Creare `split_manager.py` per lo split train/val/test.
  - [ ] Implementare `traditional_evaluator.py` (RMSE, MAE, Precision@K).
  - [ ] Creare `1_Valutazione_Offline_Accuratezza.ipynb`:
      - Carica dati
      - Esegui split (train/val/test)
      - Allena KBRS su train
      - Fai tuning iperparametri su val
      - Valuta su test
      - Report metriche

#### Fase 3: Implementazione Valutazione Online (Notebook 2)

  - [ ] Implementare `mab_algorithms.py` (almeno LinUCB e EpsilonGreedy).
  - [ ] Implementare `mab_simulator.py` (logica di Replay e calcolo ricompensa).
  - [ ] Creare `2_Valutazione_Online_MAB.ipynb`:
      - Carica **tutti** i dati
      - Ordina per timestamp
      - Esegui la simulazione
      - Plotta il **Cumulative Reward** nel tempo

#### Fase 4: Analisi e Report Finale

  - [ ] Analizzare i risultati del Notebook 1: Il KBRS è accurato (basso RMSE)?
  - [ ] Analizzare i risultati del Notebook 2: Il LinUCB (che usa il KBRS) impara più velocemente di un MAB "stupido"?
  - [ ] Scrivere le conclusioni del progetto basandosi sui risultati di *entrambe* le valutazioni.

-----

## ✅ Vantaggi Dual-Evaluation

Questa struttura a due binari è il modo corretto di rispondere alla richiesta del progetto.

### Approccio MAB (Modalità 2)

**Pro:**

  - ✅ Valuta l'**adattività** e la velocità di apprendimento.
  - ✅ Bilanciamento exploration/exploitation.
  - ✅ Simula un ambiente "live" realistico.
  - ✅ Misura l'efficacia (es. CTR) nel tempo.

**Contro:**

  - ❌ Difficile da confrontare con benchmark accademici statici.
  - ❌ La valutazione "Replay" è *biased* (non può testare ciò che non è accaduto).

### Approccio Tradizionale (Modalità 1)

**Pro:**

  - ✅ Benchmarking standard con la letteratura (RMSE, Precision@K).
  - ✅ Robustezza statistica (train/val/test).
  - ✅ Misura l'**accuratezza** predittiva "a freddo".
  - ✅ Permette un tuning rigoroso degli iperparametri.

**Contro:**

  - ❌ Statico, non valuta l'apprendimento nel tempo.
  - ❌ Non misura l'impatto dell'esplorazione.

### Dual Evaluation = IL MEGLIO DEI DUE MONDI

Usandoli entrambi, dimostri:

1.  Che il tuo motore KBRS è **accurato** (dalla Modalità 1).
2.  Che la tua strategia MAB (che *usa* il KBRS come contesto) è **efficace** (dalla Modalità 2).

-----

## 🔧 Troubleshooting

### Problema 1: "RMSE/MAE sono troppo alti" (Modalità 1)

  - **Causa**: Le feature di DBpedia non sono informative o il profilo utente è troppo semplice.
  - **Soluzione**:
      - Fai tuning dei pesi delle feature (es. attori vs. registi) usando il **Validation Set**.
      - Arricchisci con più dati (es. `dbo:abstract` tramite embedding).

### Problema 2: "Il MAB non impara / Il Cumulative Reward è piatto" (Modalità 2)

  - **Causa**:
    1.  La ricompensa è troppo "sparsa" (es. pochi rating \> 4).
    2.  I parametri del MAB (alpha, epsilon) sono sbagliati.
    3.  Il contesto (feature KBRS) non è utile.
  - **Soluzione**:
      - Abbassa la soglia di ricompensa (es. `rating > 3.5`).
      - Fai un tuning dei parametri `alpha` (per LinUCB) o `epsilon`.
      - Assicurati che le feature passate al MAB siano normalizzate.

### Problema 3: "La simulazione MAB è troppo lenta" (Modalità 2)

  - **Causa**: L'aggiornamento del modello MAB (specie LinUCB con molte feature) è costoso ad ogni iterazione.
  - **Soluzione**:
      - Esegui la simulazione su un sottoinsieme di dati (es. solo 20k eventi).
      - Ottimizza il codice di aggiornamento (es. usando NumPy per calcoli matriciali).

-----

**Ultimo aggiornamento**: 2025-11-14
**Versione**: 2.1 (con Validation Set)
**Progetto**: CineWisdom - Dual-Mode Recommender System