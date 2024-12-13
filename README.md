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
- **`notebooks/`**: Contiene notebook Jupyter per analisi ed esperimenti.
- **`tests/`**: Test unitari e di integrazione per verificare il corretto funzionamento del progetto.
- **`docs/`**: Documentazione aggiuntiva (es. schema architetturale, riferimenti bibliografici).

---

## Prerequisiti
1. **Python 3.7+**: Assicurati di avere un ambiente Python aggiornato.
2. **Dipendenze**:
   Installa i pacchetti richiesti eseguendo:
   ```bash
   pip install -r requirements.txt