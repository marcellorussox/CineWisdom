import time

import pandas as pd
import os
from .sparql_queries import query_wikidata_for_imdbid, query_dbpedia_for_data
from tqdm import tqdm

OUTPUT_FOLDER = "data/processed"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "dbpedia_data.csv")


def load_data():
    """
    Carica i file movies.csv, ratings.csv e links.csv.

    Returns:
        tuple: Una tupla contenente i DataFrame (ratings_df, movies_df, links_df).
               Restituisce None se un file non viene trovato.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)

    ratings_path = os.path.join(project_root, 'data', 'raw', 'ratings.csv')
    movies_path = os.path.join(project_root, 'data', 'raw', 'movies.csv')
    links_path = os.path.join(project_root, 'data', 'raw', 'links.csv')

    try:
        ratings_df = pd.read_csv(ratings_path)
        movies_df = pd.read_csv(movies_path)
        links_df = pd.read_csv(links_path)
        print("Dati caricati con successo.")
        return ratings_df, movies_df, links_df
    except FileNotFoundError as e:
        print(f"Errore: File non trovato. Dettagli: {e}")
        return None, None, None


def join_dataframes(df1, df2, on='movieId', how='inner'):
    """
    Unisce due DataFrame su una colonna comune.

    Parameters:
    - df1: Il primo DataFrame.
    - df2: Il secondo DataFrame.
    - on: La colonna su cui unire.
    - how: Il tipo di join.

    Returns:
    - pd.DataFrame: Il DataFrame unito.
    """
    if df1 is None or df2 is None:
        return pd.DataFrame()
    return pd.merge(df1, df2, on=on, how=how)


def enrich_movies(movies_df, batch_size=25):
    """
    Crea un catalogo unico di film, lo arricchisce con dati da Wikidata e DBpedia,
    e salva i progressi in un file CSV per la resilienza ai guasti.

    Parameters:
    - movies_df: DataFrame dei film.
    - batch_size: Dimensione del batch per le query.

    Returns:
    - pd.DataFrame: Il catalogo di film arricchito completo (caricato dal file).
    """
    if movies_df.empty:
        return pd.DataFrame()

    # Crea la cartella di output se non esiste
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"Creata la cartella di output: '{OUTPUT_FOLDER}'.")

    # Logica di ripristino (checkpointing)
    if os.path.exists(OUTPUT_FILE):
        print(f"Trovato file di ripristino '{OUTPUT_FILE}'. Caricamento del progresso...")
        try:
            processed_df = pd.read_csv(OUTPUT_FILE)
            processed_imdb_ids = set(processed_df['imdbId'].tolist())
            df_to_process = movies_df[~movies_df['imdbId'].isin(processed_imdb_ids)]
            print(f"Ripresa dell'elaborazione. Saltati {len(processed_imdb_ids)} record già processati.")
        except Exception as e:
            print(f"Errore nel caricamento del file di ripristino: {e}. Riavvio l'elaborazione dall'inizio.")
            df_to_process = movies_df.copy()
    else:
        print("Nessun file di ripristino trovato. Avvio l'elaborazione dall'inizio.")
        df_to_process = movies_df.copy()

    imdb_ids = df_to_process['imdbId'].tolist()
    progress_bar = tqdm(total=len(imdb_ids), desc="Arricchimento dati film")

    for i in range(0, len(imdb_ids), batch_size):
        batch_imdb_ids = imdb_ids[i:i + batch_size]
        batch_df = df_to_process[df_to_process['imdbId'].isin(batch_imdb_ids)].copy()

        # Aggiungi colonne per i dati arricchiti
        batch_df['wikidataId'] = None
        batch_df['dbpediaAbstract'] = None
        batch_df['dbpediaDirector'] = None

        # Chiamate alle funzioni del modulo sparql_queries
        wikidata_mappings = query_wikidata_for_imdbid(batch_imdb_ids)
        if wikidata_mappings:
            dbpedia_data = query_dbpedia_for_data(list(wikidata_mappings.values()))

            # Aggiorna il DataFrame del batch
            for imdb_id, wikidata_id in wikidata_mappings.items():
                batch_df.loc[batch_df['imdbId'] == imdb_id, 'wikidataId'] = wikidata_id

                if wikidata_id in dbpedia_data:
                    data = dbpedia_data[wikidata_id]
                    batch_df.loc[batch_df['imdbId'] == imdb_id, 'dbpediaAbstract'] = data.get('abstract')
                    batch_df.loc[batch_df['imdbId'] == imdb_id, 'dbpediaDirector'] = data.get('director')

        # Salva il batch in modalità append
        if not os.path.exists(OUTPUT_FILE):
            batch_df.to_csv(OUTPUT_FILE, index=False, header=True)
        else:
            batch_df.to_csv(OUTPUT_FILE, mode='a', index=False, header=False)

        progress_bar.update(len(batch_imdb_ids))
        time.sleep(1)

    progress_bar.close()
    print(f"\nArricchimento dei dati completato. Risultati salvati in '{OUTPUT_FILE}'.")

    # Carica e restituisci l'intero DataFrame arricchito per l'utilizzo nel notebook
    final_enriched_df = pd.read_csv(OUTPUT_FILE)
    return final_enriched_df
