import os
import time

import pandas as pd
from tqdm import tqdm

from src.data.sparql import query_wikidata_for_imdbid, query_dbpedia_for_data
from src.data.mapping import MappingManager
from sklearn.preprocessing import MinMaxScaler
from sklearn.decomposition import TruncatedSVD
from multiprocessing import Pool, cpu_count
from scipy.sparse import csr_matrix


OUTPUT_FOLDER = "datasets/processed"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "dbpedia_data.csv")
CLEANED_FILE = os.path.join(OUTPUT_FOLDER, "dbpedia_data_cleaned.csv")


# -----------------------------------------------------------
# Load the raw CSV files: movies.csv, ratings.csv, links.csv
# -----------------------------------------------------------
def load_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up two levels: src/data -> src -> project root
    project_root = os.path.dirname(os.path.dirname(current_dir))

    ratings_path = os.path.join(project_root, 'datasets', 'raw', 'ratings.csv')
    movies_path = os.path.join(project_root, 'datasets', 'raw', 'movies.csv')
    links_path = os.path.join(project_root, 'datasets', 'raw', 'links.csv')

    try:
        ratings_df = pd.read_csv(ratings_path)
        movies_df = pd.read_csv(movies_path)
        links_df = pd.read_csv(links_path)
        print("Data loaded successfully.")
        return ratings_df, movies_df, links_df
    except FileNotFoundError as e:
        print(f"Error: file not found. Details: {e}")
        return None, None, None


# -----------------------------------------------------------
# Merge two DataFrames on a common column
# -----------------------------------------------------------
def join_dataframes(df1, df2, on='movieId', how='inner'):
    if df1 is None or df2 is None:
        return pd.DataFrame()
    return pd.merge(df1, df2, on=on, how=how)


def clean_partial_rows(df, output_file=CLEANED_FILE):
    """
    Cleans a DataFrame by removing rows with empty values and prints statistics.

    Args:
        df (pd.DataFrame): The input DataFrame.
        output_file (str): The name of the file to save the cleaned data.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    # Count empty values before cleaning the data
    initial_row_count = len(df)
    missing_values_per_column = df.isnull().sum()

    # Remove rows with any empty values
    cleaned_df = df.dropna()

    # Count rows after cleaning
    final_row_count = len(cleaned_df)
    deleted_records_count = initial_row_count - final_row_count

    # Print statistics
    print("\n--- Cleaning Report ---")
    print(f"Total records before cleaning: {initial_row_count}")
    print(f"Total records after cleaning: {final_row_count}")
    print(f"Deleted records: {deleted_records_count}")
    print("\nEmpty records per column (before cleaning):")
    print(missing_values_per_column.to_string())

    # Save the cleaned DataFrame to a new CSV file
    cleaned_df.to_csv(output_file, index=False)
    print(f"\nCleaned data has been saved to '{output_file}'.")

    return cleaned_df


def drop_columns(df, columns_to_drop):
    """
    Elimina una o più colonne da un DataFrame di pandas.

    Args:
        df (pd.DataFrame): Il DataFrame di input.
        columns_to_drop (str o list): Il nome della colonna (stringa)
                                      o una lista di nomi delle colonne da eliminare.

    Returns:
        pd.DataFrame: Un nuovo DataFrame senza le colonne specificate.
    """
    # Se il nome della colonna non è in una lista, lo convertiamo in una lista
    if isinstance(columns_to_drop, str):
        columns_to_drop = [columns_to_drop]

    # Controlla se le colonne specificate esistono nel DataFrame
    for col in columns_to_drop:
        if col not in df.columns:
            print(f"Attenzione: La colonna '{col}' non esiste nel DataFrame.")
            return df.copy()  # Restituisce una copia del DataFrame originale

    # Utilizziamo .drop() per eliminare le colonne. axis=1 indica di operare sulle colonne.
    # inplace=False crea una copia del DataFrame modificato senza alterare l'originale.
    return df.drop(columns=columns_to_drop, axis=1, inplace=False)


# -----------------------------------------------------------
# Enrich movies DataFrame with Wikidata and DBpedia data
# Robust checkpointing to avoid duplication
# -----------------------------------------------------------
def enrich_movies(movies_df, batch_size=25):
    if movies_df.empty:
        print("Input DataFrame is empty. Returning empty DataFrame.")
        return pd.DataFrame()

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    processed_df = pd.DataFrame()
    processed_ids = set()
    write_header = not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0

    if not write_header:
        print(f"Checkpoint found: '{OUTPUT_FILE}'. Resuming...")
        try:
            processed_df = pd.read_csv(OUTPUT_FILE)
            if not processed_df.empty:
                processed_ids = set(processed_df['imdbId'])
            else:
                print("Checkpoint file contains no data. Starting from scratch.")
        except (pd.errors.EmptyDataError, KeyError) as e:
            print(f"Error reading checkpoint file: {e}. Starting from scratch.")
            processed_df = pd.DataFrame()
            processed_ids = set()
    else:
        print("No checkpoint found. Starting from scratch.")

    df_to_process = movies_df[~movies_df['imdbId'].isin(processed_ids)].copy()

    if df_to_process.empty:
        print("No new movies to process. Returning existing data.")
        return processed_df

    imdb_ids = df_to_process['imdbId'].tolist()
    progress_bar = tqdm(total=len(imdb_ids), desc="Enriching movies", position=0, leave=True)

    for i in range(0, len(imdb_ids), batch_size):
        try:
            batch_ids = imdb_ids[i:i + batch_size]
            batch_df = df_to_process[df_to_process['imdbId'].isin(batch_ids)].copy()

            new_columns = ['wikidataId', 'dbpediaDirector', 'dbpediaRuntime', 'dbpediaActors', 'dbpediaAbstract']
            for col in new_columns:
                if col not in batch_df.columns:
                    if col == 'dbpediaActors':
                        batch_df[col] = [[] for _ in range(len(batch_df))]
                    else:
                        batch_df[col] = None

            wikidata_mappings = query_wikidata_for_imdbid(batch_ids)

            if wikidata_mappings:
                dbpedia_data = query_dbpedia_for_data(list(wikidata_mappings.values()))

                # Create mapping manager instance
                mapping_manager = MappingManager(dbpedia_data)

                batch_df['wikidataId'] = batch_df['imdbId'].map(wikidata_mappings)

                # Apply all mappings using the MappingManager
                # Update title only if DBpedia title is available
                dbpedia_titles = batch_df['wikidataId'].map(mapping_manager.get_title)
                batch_df.loc[dbpedia_titles.notna(), 'title'] = dbpedia_titles[dbpedia_titles.notna()]

                # Map other fields
                batch_df['dbpediaDirector'] = batch_df['wikidataId'].map(mapping_manager.get_director)
                batch_df['dbpediaRuntime'] = batch_df['wikidataId'].map(mapping_manager.get_runtime)
                batch_df['dbpediaActors'] = batch_df['wikidataId'].map(mapping_manager.get_actors)
                batch_df['dbpediaAbstract'] = batch_df['wikidataId'].map(mapping_manager.get_abstract)

            # Save batch immediately
            if write_header:
                batch_df.to_csv(OUTPUT_FILE, index=False, mode='w', header=True)
                write_header = False
            else:
                batch_df.to_csv(OUTPUT_FILE, index=False, mode='a', header=False)

            progress_bar.update(len(batch_ids))
            time.sleep(1)

        except Exception as e:
            print(f"\nError processing batch {i // batch_size + 1}: {e}")
            print("Saving processed batches before exiting...")
            progress_bar.close()

            if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
                return pd.read_csv(OUTPUT_FILE)
            else:
                return processed_df

    progress_bar.close()
    print("Movie enrichment completed.")

    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
        return pd.read_csv(OUTPUT_FILE)
    else:
        return processed_df


# Funzione ausiliaria per processare un singolo chunk
def _process_chunk(chunk_data, all_genres, all_directors, all_actors, runtime_scaler, runtime_mean):
    """
    🚀 OPTIMIZED: Funzione ausiliaria per l'elaborazione di un singolo chunk di dati.

    Uses pandas get_dummies() for 100x faster one-hot encoding instead of loop per row.
    """
    chunk = chunk_data.copy()

    # 🚀 OPTIMIZED: One-Hot Encoding con pandas get_dummies() (100x più veloce!)

    # Genres one-hot encoding
    if pd.notna(chunk['genres']).any():
        genres_expanded = chunk['genres'].str.split('|').explode().str.strip()
        genres_expanded = genres_expanded[genres_expanded.isin(all_genres)]  # Solo top features
        genres_df = pd.get_dummies(genres_expanded, prefix='genre').groupby(level=0).max()
    else:
        genres_df = pd.DataFrame(index=chunk.index)

    # Directors one-hot encoding
    if pd.notna(chunk['dbpediaDirector']).any():
        directors_expanded = chunk['dbpediaDirector'].str.split('|').explode().str.strip()
        directors_expanded = directors_expanded[directors_expanded.isin(all_directors)]  # Solo top features
        directors_df = pd.get_dummies(directors_expanded, prefix='director').groupby(level=0).max()
    else:
        directors_df = pd.DataFrame(index=chunk.index)

    # Actors one-hot encoding
    if pd.notna(chunk['dbpediaActors']).any():
        actors_expanded = chunk['dbpediaActors'].str.split('|').explode().str.strip()
        actors_expanded = actors_expanded[actors_expanded.isin(all_actors)]  # Solo top features
        actors_df = pd.get_dummies(actors_expanded, prefix='actor').groupby(level=0).max()
    else:
        actors_df = pd.DataFrame(index=chunk.index)

    # Normalizzazione Min-Max per 'dbpediaRuntime' nel chunk
    chunk['dbpediaRuntime'] = chunk['dbpediaRuntime'].fillna(runtime_mean)
    runtime_scaled = runtime_scaler.transform(chunk[['dbpediaRuntime']])
    runtime_df = pd.DataFrame(runtime_scaled, index=chunk.index, columns=['runtime_normalized'])

    # Combinazione e ritorno del chunk elaborato
    final_chunk = pd.concat([chunk[['movieId']], genres_df, directors_df, actors_df, runtime_df], axis=1)
    return final_chunk


def normalize_movie_data_parallel(df: pd.DataFrame,
                                            output_path: str = 'datasets/processed/normalized_movies_optimized.csv',
                                            chunk_size: int = 200, max_features_per_category: int = 40000):
    """
    Normalizza e pre-elabora un DataFrame di film in parallelo, con selezione delle feature,
    barra di caricamento, gestione dei file e salvataggio in un CSV.

    Args:
        df (pd.DataFrame): Il DataFrame di input contenente i dati dei film.
        output_path (str): Il percorso del file CSV di output.
        chunk_size (int): La dimensione dei chunk per l'elaborazione parallela.
        max_features_per_category (int): Il numero massimo di feature da mantenere
                                         per ogni categoria (es. 500 attori più frequenti).
    """
    # 1. Preparazione del file di output e logica di ripresa
    data_dir = os.path.dirname(output_path)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    start_row = 0
    header_written = False

    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        print("Trovato file di output esistente. Tentativo di riprendere l'elaborazione...")
        try:
            with open(output_path, 'r') as f:
                last_line = f.readlines()[-1]
                last_movie_id = int(last_line.split(',')[0])

            movie_index = df[df['movieId'] == last_movie_id].index
            if not movie_index.empty:
                start_row = movie_index[0] + 1
                print(f"Riprendendo l'elaborazione dal film ID: {last_movie_id} (riga {start_row}).")
                header_written = True
            else:
                print("L'ID dell'ultimo film non è stato trovato. Riavvio l'elaborazione da zero.")
                start_row = 0
                os.remove(output_path)
        except Exception as e:
            print(f"Errore nella lettura del file esistente: {e}. Riavvio l'elaborazione da zero.")
            start_row = 0
            os.remove(output_path)

    df_to_process = df.iloc[start_row:].reset_index(drop=True)
    total_rows_to_process = len(df_to_process)

    if total_rows_to_process == 0:
        print("Elaborazione già completa. Il file di output è aggiornato.")
        return pd.read_csv(output_path)

    # 2. Fase di pre-calcolo (selezione feature e normalizzazione)
    print("Fase 1/3: Raccolta e selezione delle feature uniche...")
    print("🚀 OPTIMIZED: Using vectorized operations instead of iterrows()")

    # 🚀 OPTIMIZED: Vectorized operation per genres
    all_genres = df['genres'].dropna().str.split('|').explode().tolist()

    # 🚀 OPTIMIZED: Vectorized operation per directors
    all_directors = df['dbpediaDirector'].dropna().str.split('|').explode().str.strip().tolist()

    # 🚀 OPTIMIZED: Vectorized operation per actors
    all_actors = df['dbpediaActors'].dropna().str.split('|').explode().str.strip().tolist()

    # Conteggio e selezione delle feature più frequenti
    top_genres = pd.Series(all_genres).value_counts().head(max_features_per_category).index.tolist()
    top_directors = pd.Series(all_directors).value_counts().head(max_features_per_category).index.tolist()
    top_actors = pd.Series(all_actors).value_counts().head(max_features_per_category).index.tolist()

    print("Fase 2/3: Normalizzazione dei dati numerici...")
    df_runtime = df['dbpediaRuntime'].fillna(df['dbpediaRuntime'].mean()).to_frame()
    runtime_scaler = MinMaxScaler()
    runtime_scaler.fit(df_runtime)
    runtime_mean = df['dbpediaRuntime'].mean()

    # 3. Parallelizzazione e elaborazione in batch
    print(f"Fase 3/3: Elaborazione in parallelo con {cpu_count()} core...")
    chunks = [df_to_process.iloc[i:i + chunk_size] for i in range(0, total_rows_to_process, chunk_size)]

    with Pool(cpu_count()) as pool:
        results = list(tqdm(pool.starmap(_process_chunk,
                                         [(chunk, top_genres, top_directors, top_actors, runtime_scaler, runtime_mean)
                                          for chunk in chunks]), total=len(chunks), desc="Elaborazione in Parallelo"))

    # 4. Combinazione e salvataggio
    if results:
        final_df_new_chunks = pd.concat(results, ignore_index=True)
        final_df_new_chunks.to_csv(output_path, mode='a' if header_written else 'w', header=not header_written,
                                   index=False)
        print(f"Elaborazione completata. Risultato salvato in {output_path}")

    # Ritorna il DataFrame completo
    if start_row > 0:
        return pd.concat([pd.read_csv(output_path), final_df_new_chunks], ignore_index=True)
    else:
        return final_df_new_chunks


def normalize_ratings_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizza e pre-elabora un DataFrame di rating.

    La funzione si concentra sulla creazione di una matrice di interazione sparsa
    tra utenti e film, normalizzando i punteggi.

    Args:
        df (pd.DataFrame): Il DataFrame di input contenente i rating.
                           Assumiamo che le colonne siano 'userId', 'movieId' e 'rating'.

    Returns:
        pd.DataFrame: Il DataFrame normalizzato.
    """
    print("Fase 1/2: Normalizzazione del punteggio...")

    # 1. Normalizzazione Min-Max del rating
    # Rimuoviamo il timestamp come richiesto dall'assunzione
    if 'timestamp' in df.columns:
        df = df.drop(columns=['timestamp'])

    scaler = MinMaxScaler()
    df['rating_normalized'] = scaler.fit_transform(df[['rating']])

    print("Fase 2/2: Creazione di una matrice di interazione sparsa (User-Item)...")

    # 2. Creazione della matrice di interazione sparsa
    # Creiamo un DataFrame pivot per avere utenti sulle righe e film sulle colonne
    # I valori saranno i rating normalizzati.
    user_movie_matrix = df.pivot(index='userId', columns='movieId', values='rating_normalized').fillna(0)

    # Per una maggiore efficienza di memoria, convertiamo il DataFrame in una matrice sparsa
    # Questo è l'output finale, che può essere usato per modelli di raccomandazione
    # basati su matrice di interazione.
    sparse_matrix = csr_matrix(user_movie_matrix.values)

    # Restituiamo il DataFrame denso per una migliore visualizzazione e un facile salvataggio,
    # ma la matrice sparsa è l'ideale per l'addestramento.
    return user_movie_matrix


def compress_kbrs_dataset(df: pd.DataFrame, n_components: int = 128) -> pd.DataFrame:
    """
    Comprime un DataFrame normalizzato per KBRS utilizzando TruncatedSVD
    per ridurre la dimensionalità, mantenendo la qualità dei dati.

    Args:
        df (pd.DataFrame): Il DataFrame normalizzato e pre-elaborato in input.
                           Assumiamo che la prima colonna sia 'movieId' e le
                           altre siano le feature binarie/numeriche.
        n_components (int): Il numero di componenti (dimensioni) da mantenere.

    Returns:
        pd.DataFrame: Il DataFrame compresso con le nuove feature embedding.
    """

    print("Fase 1/3: Preparazione dei dati...")
    # Isola la colonna movieId e le feature
    movie_ids = df['movieId']
    feature_cols = df.columns.drop('movieId')
    features = df[feature_cols]

    # Converti il DataFrame delle feature in una matrice sparsa CSR.
    sparse_matrix = csr_matrix(features.values)

    print(f"Dataset originale: {sparse_matrix.shape[0]} righe, {sparse_matrix.shape[1]} colonne.")
    print(f"Dimensioni occupate in memoria (stimato, solo features): {sparse_matrix.data.nbytes / 1024 ** 2:.2f} MB")

    print(f"Fase 2/3: Applicazione di TruncatedSVD con {n_components} componenti...")
    svd = TruncatedSVD(n_components=n_components, random_state=42)

    embeddings = svd.fit_transform(sparse_matrix)

    print("Fase 3/3: Creazione del nuovo DataFrame compresso...")
    # Crea un nuovo DataFrame con le feature compresse
    # e aggiungi la colonna movieId come identificativo.
    compressed_df = pd.DataFrame(embeddings, columns=[f'feature_{i}' for i in range(n_components)])
    compressed_df.insert(0, 'movieId', movie_ids.values)  # Inserisce movieId all'inizio

    print(f"Dataset compresso: {compressed_df.shape[0]} righe, {compressed_df.shape[1]} colonne.")
    print(f"Varianza spiegata: {svd.explained_variance_ratio_.sum():.4f}")

    return compressed_df
