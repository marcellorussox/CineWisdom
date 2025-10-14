# CineWisdom

**CineWisdom** è un sistema di raccomandazione di film basato sulla conoscenza, che utilizza il dataset MovieLens Small e arricchisce i metadati dei film con informazioni estratte da DBpedia/Wikidata tramite query SPARQL. Il sistema integra inoltre *Multi-Armed Bandit (MAB)* con Thompson Sampling per bilanciare esplorazione e sfruttamento nelle raccomandazioni.

## Funzionalità principali
- **Estrazione di conoscenze**: Recupera informazioni avanzate sui film (es. registi, attori, generi) da DBpedia.
- **Sistema di raccomandazione basato su conoscenza**: Suggerisce film in base alle caratteristiche e preferenze dell'utente.
- **Tecniche di Multi-Armed Bandit**: Implementa Thompson Sampling e una logica di reward personalizzata per ridurre il bias di popolarità.
- **Valutazione rigorosa**: Analizza la precisione e la qualità del sistema con metriche standard.

---

## Struttura del progetto
- **`datasets/`**: Contiene i dataset grezzi (`datasets/raw/`), i processati (`datasets/processed/`) e i risultati dell'arricchimento.
- **`src/`**: Script per gestione dati, query SPARQL, KBRS, MAB e simulazione/visualizzazione.
- **`plots/`**: Output dei grafici generati (es. `selection_rate.png`, `cumulative_reward.png`).
- **`kbrs.ipynb`**: Il flusso è replicato in un notebook o in script Python che richiamano i moduli in `src/`.

---

## Prerequisiti
1. **Python 3.11+**: Assicurati di avere un ambiente Python aggiornato.
2. **Dipendenze**:
   Installa i pacchetti richiesti eseguendo:
   ```bash
   pip install -r requirements.txt
   
---

## Installazione e utilizzo
1. **Clona il repository**:
   ```bash
   git clone <URL-del-repo>
   cd CineWisdom
   ```
2. **Scarica MovieLens (Small)**:
   - [https://grouplens.org/datasets/movielens/](https://grouplens.org/datasets/movielens/)
3. **Organizza i dati**: copia i CSV in `datasets/raw/` con questi nomi:
   - `datasets/raw/movies.csv`
   - `datasets/raw/links.csv`
   - `datasets/raw/ratings.csv`
4. **Installa le dipendenze**:
   ```bash
   pip install -r requirements.txt
   ```

5. **Output dei grafici**: i plot sono salvati in `plots/` come `selection_rate.png` e `cumulative_reward.png`.

---

## Logica di Reward (MAB)
Nel modulo `src/simulation/simulator.py`, la classe `MABSimulator` implementa un reward binario orientato all'esplorazione per ridurre il bias di popolarità:
- **Successo (1)** se tra i film raccomandati esiste almeno un film non ancora visto dall'utente che appartiene ai suoi film preferiti (preferenze derivate da rating ≥ 4.0 su altri film).
- **Fallimento (0)** se tutte le raccomandazioni sono già viste o non sovrapposte alle preferenze.

Questa scelta spinge il Thompson Sampling a favorire modelli che scoprono nuove raccomandazioni rilevanti, come `KBRS_Hybrid`, rispetto a semplici baseline di popolarità.

---

## Licenza
Questo progetto è distribuito sotto licenza MIT. Vedi il file `LICENSE` per i dettagli.

---

## Contributori
Progetto sviluppato per un esame universitario di **Informatica Magistrale** dell'Università degli Studi di Napoli Federico II.