import os
import time

import pandas as pd
from tqdm import tqdm

from .sparql_manager import query_wikidata_for_imdbid, query_dbpedia_for_data
from .mapping_manager import MappingManager

OUTPUT_FOLDER = "data/processed"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "dbpedia_data.csv")
CLEANED_FILE = os.path.join(OUTPUT_FOLDER, "dbpedia_data_cleaned.csv")


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
            return df.copy() # Restituisce una copia del DataFrame originale

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
