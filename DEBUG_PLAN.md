# 🔍 Piano di Controllo e Debug Sistematico

## 🎯 Obiettivo
Dimostrare che KBRS (sistema personalizzato) è **superiore** al Popularity Baseline attraverso:
- Metriche di accuracy
- Metriche di diversità/novità
- Esperimenti MAB

---

## 📋 Checklist di Controllo

### ✅ **1. Verifica Predizioni KBRS**

**Problema Identificato**: KBRS predizioni sembrano sempre ≥4.0

**Test da Eseguire**:
```python
# Nel notebook, aggiungi:
user_id = 10  # Esempio
recs = kbrs.recommend(user_id=user_id, n_recommendations=10)
print(f"Raccomandazioni KBRS per utente {user_id}:")
for movie_id, pred_rating in recs:
    actual_rating = ratings_df[(ratings_df['userId']==user_id) & (ratings_df['movieId']==movie_id)]['rating']
    print(f"  Movie {movie_id}: Pred={pred_rating:.2f}, Actual={actual_rating.values if not actual_rating.empty else 'UNSEEN'}")
```

**Criterio di Successo**: Predizioni dovrebbero variare (non sempre 4.0-5.0)

---

### ✅ **2. Analisi Distribuzione Predizioni**

**Script di Analisi**:
```python
# Colleziona predizioni da 100 utenti
predictions = []
for user_id in sample_users[:100]:
    recs = kbrs.recommend(user_id=user_id, n_recommendations=10)
    predictions.extend([pred for _, pred in recs])

print(f"Predizioni KBRS: min={min(predictions):.2f}, max={max(predictions):.2f}")
print(f"Media: {np.mean(predictions):.2f}, Std: {np.std(predictions):.2f}")
```

**Problema da Identificare**: Se tutte le predizioni sono tra 4.0-5.0 → **BUG**

---

### ✅ **3. Verifica Calibrazione Rating**

**KBRS predice rating in che range?**
- Scale 1-5? Scale 0-1? Scale normalizzato?

**Check nel codice KBRS**:
```python
# Nel file src/recommender/kbrs.py, cerca:
self.rating_scale  # Che range usa?
```

---

### ✅ **4. Verifica Thompson Sampling**

**Problema**: Reward alto ma MAB sceglie sempre baseline?

**Debug Thompson Sampling**:
- Verifica che il reward del KBRS arrivi correttamente al MAB
- Verifica che le stime (means) si aggiornino

**Check nel simulator**:
```python
# Nel file src/simulation/simulator.py, cerca:
self.mab_manager.update(arm_index, reward)  # Il reward arriva?
```

---

### ✅ **5. Implementa Reward System Discriminativo**

**Problema Attuale**: R_A = 1.0 per entrambi i modelli sempre

**Soluzione**: Cambia criterio di valutazione

**Criterio Proposto**:
```python
# Nuovo criterio (più selettivo):
reward = 0.6 * quality + 0.3 * diversity + 0.1 * novelty

# Dove:
# quality: accuratezza predizioni vs rating reali
# diversity: varietà di generi/attori
# novelty: film non visti vs film popolari
```

---

### ✅ **6. Test Manuale: KBRS vs Baseline**

**Esegui raccomandazioni manuali**:

```python
# Test per 3 utenti
for user_id in [1, 50, 100]:
    print(f"\n=== Utente {user_id} ===")

    # KBRS
    kbrs_recs = kbrs.recommend(user_id=user_id, n_recommendations=5)

    # Baseline
    baseline_recs = popularity_top[:5]

    print(f"KBRS: {[mid for mid, _ in kbrs_recs]}")
    print(f"Baseline: {baseline_recs}")

    # Analizza sovrapposizione
    overlap = set([mid for mid, _ in kbrs_recs]) & set(baseline_recs)
    print(f"Sovrapposizione: {len(overlap)}/5")
```

**Risultato Atteso**:
- KBRS raccomandazioni diverse dal baseline
- Sovrapposizione < 50%

---

### ✅ **7. Verifica Dati Input**

**Check**: KBRS riceve dati corretti?
```python
# Nel KBRS __init__ o recommend():
print(f"User profile size: {len(user_profile)}")
print(f"Catalogo size: {len(self.catalog)}")
print(f"Similarity matrix shape: {self.cosine_sim_matrix.shape}")
```

---

## 🐛 Bug da Identificare

### **Bug 1: Predizioni Gonfiate**
**Sintomo**: KBRS predizioni sempre ≥4.0
**Causa Probabile**: Normalizzazione errata o scala sbagliata

### **Bug 2: Reward Non Discriminativo**
**Sintomo**: R_A = 1.0 per tutti
**Causa Probabile**: Criterio troppo permissivo

### **Bug 3: Thompson Sampling Non Aggiornato**
**Sintomo**: MAB sceglie baseline anche se KBRS ha reward alto
**Causa Probabile**: Reward non arriva al MAB manager

### **Bug 4: KBRS Non Personalizza**
**Sintomo**: Raccomandazioni simili per tutti gli utenti
**Causa Probabile**: User profile non corretto

---

## 🎯 Piano di Azione

### **Step 1: Diagnosi (15 min)**
1. ✅ Verifica predizioni KBRS
2. ✅ Analizza distribuzione predizioni
3. ✅ Identifica se predizioni sono realistiche

### **Step 2: Fix Predizioni (30 min)**
1. Se predizioni gonfiate → correggi normalizzazione
2. Se scala sbagliata → converte scale
3. Test manuale per 10 utenti

### **Step 3: Reward System (20 min)**
1. Implementa criterio più selettivo
2. Test con nuove metriche
3. Verifica discriminazione

### **Step 4: Verifica MAB (15 min)**
1. Debug Thompson Sampling
2. Verifica reward propagation
3. Test completo KBRS vs Baseline

### **Step 5: Esperimento Finale (30 min)**
1. Esegui 3 simulazioni parametriche
2. Confronta risultati
3. Dimostra superiorità KBRS

---

## ✅ Criteri di Successo

**KBRS DEVE vincere quando**:
1. **Accuracy**: Predizioni più accurate del baseline
2. **Diversity**: Raccomandazioni più variegate
3. **Personalization**: Diverso per ogni utente
4. **MAB**: Selezionato >70% delle volte dopo 1000 iterazioni

---

## 🚀 Note per l'Implementazione

**Tutti i test devono essere aggiunti al notebook**:
- Crea una sezione "Debug KBRS"
- Aggiungi script di analisi
- Documenta risultati

**Obiettivo**: Notebook che dimostra **oggettivamente** la superiorità di KBRS!
