# src/data_loader.py

import pandas as pd
import os


def load_and_merge_data():
    """
    Loads and merges the MovieLens movies.csv and ratings.csv files.

    Returns:
        pd.DataFrame: A single DataFrame containing the merged data.
                      Returns an empty DataFrame if files are not found.
    """

    # 1. Definisci i percorsi dei file
    # Assumiamo che la cartella 'data/raw' si trovi nella directory principale del progetto
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)

    ratings_path = os.path.join(project_root, 'data', 'raw', 'ratings.csv')
    movies_path = os.path.join(project_root, 'data', 'raw', 'movies.csv')

    # 2. Carica i file in un blocco try-except per gestire gli errori
    try:
        ratings_df = pd.read_csv(ratings_path)
        movies_df = pd.read_csv(movies_path)
    except FileNotFoundError as e:
        print(f"Errore: File non trovato. Assicurati che i file CSV siano in 'data/raw/'. Dettagli: {e}")
        return pd.DataFrame()  # Restituisce un DataFrame vuoto in caso di errore

    # 3. Esegui il merge in base a 'movieId'
    merged_df = pd.merge(ratings_df, movies_df, on='movieId', how='inner')

    # 4. Gestisci i valori mancanti (se necessario)
    # Per questo dataset, i valori NaN non sono comuni nel merge, ma è buona prassi
    # Ad esempio, potresti voler eliminare righe con valori mancanti se necessario
    merged_df.dropna(inplace=True)

    print("Dati caricati e uniti con successo. Shape:", merged_df.shape)

    return merged_df


if __name__ == "__main__":
    # Esempio di utilizzo
    data = load_and_merge_data()
    if not data.empty:
        print("\nPrime 5 righe del DataFrame unito:")
        print(data.head())