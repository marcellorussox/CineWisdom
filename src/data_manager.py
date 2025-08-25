import os
import time

import pandas as pd
from tqdm import tqdm

from .sparql_manager import query_wikidata_for_imdbid, query_dbpedia_for_data
from .mapping_manager import MappingManager

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
# Enrich movies DataFrame with Wikidata and DBpedia data
# Robust checkpointing to avoid duplication
# -----------------------------------------------------------
def enrich_movies(movies_df, batch_size=25):
    if movies_df.empty:
        print("Input DataFrame is empty. Returning empty DataFrame.")
        return pd.DataFrame()

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Load existing checkpoint if exists
    if os.path.exists(OUTPUT_FILE):
        print(f"Checkpoint found: '{OUTPUT_FILE}'. Resuming...")
        try:
            if os.path.getsize(OUTPUT_FILE) == 0:
                print("Checkpoint file is empty. Starting from scratch.")
                processed_df = pd.DataFrame()
                processed_ids = set()
            else:
                processed_df = pd.read_csv(OUTPUT_FILE)
                if processed_df.empty:
                    print("Checkpoint file contains no data. Starting from scratch.")
                    processed_ids = set()
                else:
                    processed_ids = set(processed_df['imdbId'])
        except (pd.errors.EmptyDataError, KeyError) as e:
            print(f"Error reading checkpoint file: {e}. Starting from scratch.")
            processed_df = pd.DataFrame()
            processed_ids = set()
    else:
        print("No checkpoint found. Starting from scratch.")
        processed_df = pd.DataFrame()
        processed_ids = set()

    # Filter already processed movies
    df_to_process = movies_df[~movies_df['imdbId'].isin(processed_ids)].copy()

    if df_to_process.empty:
        print("No new movies to process. Returning existing data.")
        return processed_df

    imdb_ids = df_to_process['imdbId'].tolist()
    progress_bar = tqdm(total=len(imdb_ids), desc="Enriching movies")

    # Determine if we need to write header
    write_header = not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0

    for i in range(0, len(imdb_ids), batch_size):
        try:
            batch_ids = imdb_ids[i:i + batch_size]
            batch_df = df_to_process[df_to_process['imdbId'].isin(batch_ids)].copy()

            # Initialize new columns for DBpedia data
            new_columns = ['wikidataId', 'dbpediaDirector', 'dbpediaActors',
                           'dbpediaGenres', 'dbpediaSubjects', 'dbpediaReleaseDate',
                           'dbpediaRuntime', 'dbpediaCountries', 'dbpediaLanguages',
                           'dbpediaStory', 'dbpediaTheme', 'dbpediaAbstract']
            batch_df[new_columns] = None

            # Query Wikidata and DBpedia
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
                batch_df['dbpediaActors'] = batch_df['wikidataId'].map(mapping_manager.get_actors)
                batch_df['dbpediaGenres'] = batch_df['wikidataId'].map(mapping_manager.get_genres)
                batch_df['dbpediaSubjects'] = batch_df['wikidataId'].map(mapping_manager.get_subjects)
                batch_df['dbpediaReleaseDate'] = batch_df['wikidataId'].map(mapping_manager.get_release_date)
                batch_df['dbpediaRuntime'] = batch_df['wikidataId'].map(mapping_manager.get_runtime)
                batch_df['dbpediaCountries'] = batch_df['wikidataId'].map(mapping_manager.get_countries)
                batch_df['dbpediaLanguages'] = batch_df['wikidataId'].map(mapping_manager.get_languages)
                batch_df['dbpediaStory'] = batch_df['wikidataId'].map(mapping_manager.get_story)
                batch_df['dbpediaTheme'] = batch_df['wikidataId'].map(mapping_manager.get_theme)
                batch_df['dbpediaAbstract'] = batch_df['wikidataId'].map(mapping_manager.get_abstract)

            # Save batch immediately
            if write_header:
                batch_df.to_csv(OUTPUT_FILE, index=False, mode='w')
                write_header = False
            else:
                batch_df.to_csv(OUTPUT_FILE, index=False, mode='a', header=False)

            progress_bar.update(len(batch_ids))
            time.sleep(1)

        except Exception as e:
            print(f"Error processing batch {i // batch_size + 1}: {e}")
            print("Saving processed batches before exiting...")
            progress_bar.close()

            # Read all processed data to return
            if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
                return pd.read_csv(OUTPUT_FILE)
            else:
                return processed_df

    progress_bar.close()

    print("Movie enrichment completed.")

    # Return all processed data
    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
        return pd.read_csv(OUTPUT_FILE)
    else:
        return processed_df
