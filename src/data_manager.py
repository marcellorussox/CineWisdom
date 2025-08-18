import time
import re
import pandas as pd
import os
from .sparql_queries import query_wikidata_for_imdbid, query_dbpedia_for_data
from tqdm import tqdm

OUTPUT_FOLDER = "data/processed"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "dbpedia_data.csv")


# -----------------------------------------------------------
# Load the raw CSV files: movies.csv, ratings.csv, links.csv
# -----------------------------------------------------------
def load_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)

    ratings_path = os.path.join(project_root, 'data', 'raw', 'ratings.csv')
    movies_path = os.path.join(project_root, 'data', 'raw', 'movies.csv')
    links_path = os.path.join(project_root, 'data', 'raw', 'links.csv')

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


# -----------------------------------------------------------
# Clean an actor name by removing any text inside parentheses
# -----------------------------------------------------------
def clean_actor_name(name):
    return re.sub(r"\s*\(.*?\)", "", name).strip()


# -----------------------------------------------------------
# Enrich movies DataFrame with Wikidata and DBpedia data
# Robust checkpointing to avoid duplication
# -----------------------------------------------------------
def enrich_movies(movies_df, batch_size=25):
    if movies_df.empty:
        return pd.DataFrame()

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Load existing checkpoint if exists
    if os.path.exists(OUTPUT_FILE):
        print(f"Checkpoint found: '{OUTPUT_FILE}'. Resuming...")
        processed_df = pd.read_csv(OUTPUT_FILE)
        processed_ids = set(processed_df['imdbId'])
        df_to_process = movies_df[~movies_df['imdbId'].isin(processed_ids)].copy()
        print(f"Skipping {len(processed_ids)} already processed movies.")
    else:
        print("No checkpoint found. Starting from scratch.")
        processed_df = pd.DataFrame()
        df_to_process = movies_df.copy()

    imdb_ids = df_to_process['imdbId'].tolist()
    progress_bar = tqdm(total=len(imdb_ids), desc="Enriching movies")

    all_batches = []  # store processed batches to append at the end

    for i in range(0, len(imdb_ids), batch_size):
        batch_ids = imdb_ids[i:i + batch_size]
        batch_df = df_to_process[df_to_process['imdbId'].isin(batch_ids)].copy()

        # Initialize columns
        batch_df[['wikidataId', 'dbpediaAbstract', 'dbpediaDirector', 'dbpediaActor']] = None

        # Query Wikidata and DBpedia
        wikidata_mappings = query_wikidata_for_imdbid(batch_ids)
        if wikidata_mappings:
            dbpedia_data = query_dbpedia_for_data(list(wikidata_mappings.values()))

            batch_df['wikidataId'] = batch_df['imdbId'].map(wikidata_mappings)

            def get_abstract(wikidata_id):
                return dbpedia_data.get(wikidata_id, {}).get('abstract')

            def get_director(wikidata_id):
                return dbpedia_data.get(wikidata_id, {}).get('director')

            def get_actors(wikidata_id):
                actors = dbpedia_data.get(wikidata_id, {}).get('actors')
                if actors:
                    return "; ".join(clean_actor_name(a) for a in actors)
                return None

            batch_df['dbpediaAbstract'] = batch_df['wikidataId'].map(get_abstract)
            batch_df['dbpediaDirector'] = batch_df['wikidataId'].map(get_director)
            batch_df['dbpediaActor'] = batch_df['wikidataId'].map(get_actors)

        all_batches.append(batch_df)
        progress_bar.update(len(batch_ids))
        time.sleep(1)

    progress_bar.close()

    # Concatenate previous processed data with new batches
    if all_batches:
        new_data = pd.concat(all_batches, ignore_index=True)
        final_df = pd.concat([processed_df, new_data], ignore_index=True)
    else:
        final_df = processed_df

    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nEnrichment completed. Results saved in '{OUTPUT_FILE}'.")

    return final_df
