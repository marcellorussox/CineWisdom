# CineWisdom

**CineWisdom** è un sistema di raccomandazione di film basato sulla conoscenza, che utilizza il dataset MovieLens Small e arricchisce i metadati dei film con informazioni estratte da DBpedia tramite query SPARQL. Il sistema integra inoltre tecniche di *Multi-Armed Bandit (MAB)* per bilanciare esplorazione e sfruttamento, migliorando le raccomandazioni.

## Funzionalità principali
- **Estrazione di conoscenze**: Recupera informazioni avanzate sui film (es. registi, attori, generi) da DBpedia.
- **Sistema di raccomandazione basato su conoscenza**: Suggerisce film in base alle caratteristiche e preferenze dell'utente.
- **Tecniche di Multi-Armed Bandit**: Implementa algoritmi come Epsilon-Greedy e UCB per ottimizzare le raccomandazioni.
- **Valutazione rigorosa**: Analizza la precisione e la qualità del sistema con metriche standard.

---

## Struttura del progetto
- **`data/`**: Contiene i dataset grezzi, processati e i risultati delle query DBpedia.
- **`src/`**: Include gli script principali per la gestione dei dati, le query SPARQL e l'implementazione del recommender system.
- **`kbrs.ipynb`**: Notebook Jupyter per analisi ed esperimenti.

---

## Prerequisiti
1. **Python 3.11+**: Assicurati di avere un ambiente Python aggiornato.
2. **Dipendenze**:
   Installa i pacchetti richiesti eseguendo:
   ```bash
   pip install -r requirements.txt
   ```
   
---

## Installazione e utilizzo
1. **Clona il repository**:
   git clone <URL-del-repo> cd CineWisdom
2. **Scarica il dataset MovieLens Small**:
- [Link al dataset](https://grouplens.org/datasets/movielens/).

3. **Organizza i dati nella cartella `data/raw/`**:
Inserisci i file CSV del dataset MovieLens in `data/raw/`.

4. **Avvia il progetto**:
- Esegui gli script nella cartella `src/`.
- Oppure usa il notebook `kbrs.ipynb`.

---

## Valutazione
Il sistema utilizza metriche come:
- **Precision@k**
- **Recall@k**
- **Normalized Discounted Cumulative Gain (NDCG)**

---

## Licenza
Questo progetto è distribuito sotto licenza MIT. Vedi il file `LICENSE` per i dettagli.

---

## Contributori
Progetto sviluppato per un esame universitario di **Informatica Magistrale** dell'Università degli Studi di Napoli Federico II.