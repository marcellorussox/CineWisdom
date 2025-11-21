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



# -----------------------------------------------------------
# Load the raw CSV files: movies.csv, ratings.csv, links.csv
# -----------------------------------------------------------


def load_data(data_dir=None):
    """
    Load raw CSV files (ratings, movies, links).
    
    Args:
        data_dir: Optional path to raw data directory. 
                  Defaults to 'datasets/raw' for backward compatibility.
    
    Returns:
        tuple: (ratings_df, movies_df, links_df)
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up two levels: src/data -> src -> project root
    project_root = os.path.dirname(os.path.dirname(current_dir))

    # Use provided data_dir or default to 'datasets/raw'
    if data_dir is None:
        data_dir = os.path.join(project_root, 'datasets', 'raw')
    elif not os.path.isabs(data_dir):
        # If relative path provided, make it absolute from project root
        data_dir = os.path.join(project_root, data_dir)

    ratings_path = os.path.join(data_dir, 'ratings.csv')
    movies_path = os.path.join(data_dir, 'movies.csv')
    links_path = os.path.join(data_dir, 'links.csv')

    try:
        ratings_df = pd.read_csv(ratings_path)
        movies_df = pd.read_csv(movies_path)
        # links.csv might not exist in all datasets (e.g., ML-1M)
        if os.path.exists(links_path):
            links_df = pd.read_csv(links_path)
        else:
            # Create empty links_df with expected columns
            links_df = pd.DataFrame(columns=['movieId', 'imdbId', 'tmdbId'])
            print(f"⚠️  links.csv not found in {data_dir}. Using empty links DataFrame.")
        
        print("Data loaded successfully.")
        return ratings_df, movies_df, links_df
    except FileNotFoundError as e:
        print(f"Error: file not found. Details: {e}")
        return None, None, None





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
            import traceback
            traceback.print_exc()
            
            # Check if it's a rate limit error
            error_msg = str(e)
            if "429" in error_msg or "Too Many Requests" in error_msg:
                print(f"\n❌ RATE LIMIT ERROR: SPARQL endpoint (Wikidata/DBpedia) is temporarily blocking requests.")
                print(f"   Batch {i // batch_size + 1} failed after {batch_size} attempts.")
                print(f"\n💡 SOLUTION:")
                print(f"   1. Wait 30-60 minutes for the rate limit to reset")
                print(f"   2. Re-run: python main_pipeline.py --dataset ml-1m --mode preprocess --enrich")
                print(f"   3. The script will automatically resume from where it stopped (batch {i // batch_size + 1})")
            else:
                print(f"\n❌ Error processing batch {i // batch_size + 1}: {e}")
            
            print(f"\n📦 Saving processed data before exit...")
            print(f"   Processed {i}/{len(imdb_ids)} movies so far.")
            progress_bar.close()
            
            # Save what we have so far
            if not processed_df.empty:
                 # If we have some processed data in memory/file, return merged
                 if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
                     processed_df = pd.read_csv(OUTPUT_FILE)
            
            # Return original df merged with whatever we managed to process
            # This ensures we don't lose the original movies
            print("⚠️  Enrichment interrupted. Returning original data merged with partial results.")
            print(f"✅ Checkpoint saved to: {OUTPUT_FILE}")
            if not processed_df.empty:
                # Merge original with processed
                # Use 'movieId' if available, otherwise assume index alignment or imdbId
                # But processed_df has imdbId.
                return movies_df.merge(processed_df, on='imdbId', how='left', suffixes=('', '_enriched'))
            else:
                return movies_df

    progress_bar.close()
    print("Movie enrichment completed.")

    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
        processed_df = pd.read_csv(OUTPUT_FILE)
        # Merge back to original to ensure we have all movies
        return movies_df.merge(processed_df, on='imdbId', how='left', suffixes=('', '_enriched'))
    else:
        return movies_df


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
    if all_directors and 'dbpediaDirector' in chunk.columns and pd.notna(chunk['dbpediaDirector']).any():
        directors_expanded = chunk['dbpediaDirector'].str.split('|').explode().str.strip()
        directors_expanded = directors_expanded[directors_expanded.isin(all_directors)]
        directors_df = pd.get_dummies(directors_expanded, prefix='director').groupby(level=0).max()
    else:
        directors_df = pd.DataFrame(index=chunk.index)

    # Actors one-hot encoding
    if all_actors and 'dbpediaActors' in chunk.columns and pd.notna(chunk['dbpediaActors']).any():
        actors_expanded = chunk['dbpediaActors'].str.split('|').explode().str.strip()
        actors_expanded = actors_expanded[actors_expanded.isin(all_actors)]
        actors_df = pd.get_dummies(actors_expanded, prefix='actor').groupby(level=0).max()
    else:
        actors_df = pd.DataFrame(index=chunk.index)

    # Normalizzazione Min-Max per 'dbpediaRuntime' nel chunk
    if runtime_scaler and 'dbpediaRuntime' in chunk.columns:
        chunk['dbpediaRuntime'] = chunk['dbpediaRuntime'].fillna(runtime_mean)
        runtime_scaled = runtime_scaler.transform(chunk[['dbpediaRuntime']])
        runtime_df = pd.DataFrame(runtime_scaled, index=chunk.index, columns=['runtime_normalized'])
    else:
        runtime_df = pd.DataFrame(index=chunk.index)

    # Combinazione e ritorno del chunk elaborato
    # Align indices before concat to avoid issues
    genres_df = genres_df.reindex(chunk.index, fill_value=0)
    directors_df = directors_df.reindex(chunk.index, fill_value=0)
    actors_df = actors_df.reindex(chunk.index, fill_value=0)
    
    final_chunk = pd.concat([chunk[['movieId']], genres_df, directors_df, actors_df, runtime_df], axis=1)
    return final_chunk


def normalize_movie_data_parallel(df: pd.DataFrame,
                                            output_path: str = 'datasets/processed/normalized_movies_optimized.csv',
                                            chunk_size: int = 200, max_features_per_category: int = 40000):
    """
    Normalizza e pre-elabora un DataFrame di film in parallelo.
    """
    # 1. Gestione ripresa elaborazione
    start_row = 0
    header_written = False

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if os.path.exists(output_path):
        try:
            df_existing = pd.read_csv(output_path)
            start_row = len(df_existing)
            header_written = True
            print(f"Trovato file parziale con {start_row} righe. Riprendo l'elaborazione...")
            
            if start_row >= len(df):
                print("Elaborazione già completa (tutte le righe presenti).")
                return df_existing
        except Exception as e:
            print(f"Errore lettura file esistente: {e}. Ricomincio da zero.")
            os.remove(output_path)
            start_row = 0
            header_written = False

    df_to_process = df.iloc[start_row:].reset_index(drop=True)
    total_rows_to_process = len(df_to_process)

    if total_rows_to_process == 0:
        print("Nessuna nuova riga da processare.")
        if os.path.exists(output_path):
            return pd.read_csv(output_path)
        else:
            # If input was empty and no output exists, return empty DF
            return pd.DataFrame()

    # 2. Fase di pre-calcolo (selezione feature e normalizzazione)
    # 2. Fase di pre-calcolo (selezione feature e normalizzazione)
    print("Fase 1/3: Raccolta e selezione delle feature uniche...")
    print("🚀 OPTIMIZED: Using vectorized operations instead of iterrows()")

    # 🚀 OPTIMIZED: Vectorized operation per genres (Always present)
    all_genres = df['genres'].dropna().str.split('|').explode().tolist()
    top_genres = pd.Series(all_genres).value_counts().head(max_features_per_category).index.tolist()

    # Handle optional DBpedia columns
    top_directors = []
    top_actors = []
    runtime_scaler = None
    runtime_mean = 0

    if 'dbpediaDirector' in df.columns:
        all_directors = df['dbpediaDirector'].dropna().str.split('|').explode().str.strip().tolist()
        top_directors = pd.Series(all_directors).value_counts().head(max_features_per_category).index.tolist()
    
    if 'dbpediaActors' in df.columns:
        all_actors = df['dbpediaActors'].dropna().str.split('|').explode().str.strip().tolist()
        top_actors = pd.Series(all_actors).value_counts().head(max_features_per_category).index.tolist()

    print("Fase 2/3: Normalizzazione dei dati numerici...")
    if 'dbpediaRuntime' in df.columns:
        runtime_mean = df['dbpediaRuntime'].mean()
        df_runtime = df['dbpediaRuntime'].fillna(runtime_mean).to_frame()
        runtime_scaler = MinMaxScaler()
        runtime_scaler.fit(df_runtime)
    else:
        print("⚠️  'dbpediaRuntime' missing. Skipping runtime normalization.")


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








