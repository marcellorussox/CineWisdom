# src/pipeline/traditional_pipeline.py
import pandas as pd
import numpy as np
from pathlib import Path
import json

# Imports
from src.data.manager import DataManager
from src.data.split_manager import SplitManager
from src.recommender.kbrs import KBRSEngine
from src.evaluation.traditional_evaluator import TraditionalEvaluator

def run_offline_evaluation():
    """
    Pipeline 1: Valutazione Offline (Accuratezza)

    Obiettivo: Misurare QUANTO È ACCURATO il KBRS nel predire rating "a freddo"
    Metodologia: Split statico train/val/test → Valutazione su test set mai visto
    Metriche: RMSE, MAE, Precision@K, NDCG@K
    """

    print("=" * 80)
    print("PIPELINE 1: OFFLINE EVALUATION (ACCURATEZZA KBRS)")
    print("=" * 80)

    # Step 1: Carica dati
    print("\n[1/5] Caricamento dati...")
    data_manager = DataManager()
    movies_df, ratings_df = data_manager.load_movielens()
    movies_enriched = data_manager.enrich_with_dbpedia(movies_df)  # Se non già fatto
    print(f"  ✓ Caricati {len(movies_enriched)} film, {len(ratings_df)} rating")

    # Step 2: Crea split
    print("\n[2/5] Creazione split train/val/test...")
    split_manager = SplitManager()
    train_df, val_df, test_df = split_manager.create_traditional_split(
        ratings_df,
        test_size=0.1,
        val_size=0.1,
        min_ratings_per_user=5
    )
    print(f"  ✓ Train: {len(train_df)} | Val: {len(val_df)} | Test: {len(test_df)}")

    # Step 3: Inizializza KBRS
    print("\n[3/5] Inizializzazione motore KBRS...")
    kbrs_engine = KBRSEngine(movies_enriched)

    # Step 4: Valutazione su test set
    print("\n[4/5] Valutazione su test set...")
    predictions = []
    ground_truth = []

    # Per ogni utente nel test set
    test_users = test_df['userId'].unique()

    for user_id in test_users:
        # Storia dell'utente (SOLO training)
        user_history = train_df[train_df['userId'] == user_id]

        # Costruisci profilo utente
        user_profile = kbrs_engine.build_user_profile(user_history)

        # Film che l'utente ha valutato nel test set
        user_test_items = test_df[test_df['userId'] == user_id]

        for _, rating_event in user_test_items.iterrows():
            movie_id = rating_event['movieId']
            true_rating = rating_event['rating']

            # Predisci usando KBRS
            item_features = kbrs_engine.get_item_features(movie_id)
            predicted_rating = kbrs_engine.predict_score(user_profile, item_features)

            predictions.append(predicted_rating)
            ground_truth.append(true_rating)

        if len(predictions) % 1000 == 0:
            print(f"    Processati {len(predictions)} rating...")

    print(f"  ✓ Completate {len(predictions)} predizioni")

    # Step 5: Calcola metriche
    print("\n[5/5] Calcolo metriche di accuratezza...")
    evaluator = TraditionalEvaluator()

    results = {
        'rmse': evaluator.rmse(ground_truth, predictions),
        'mae': evaluator.mae(ground_truth, predictions),
        'num_predictions': len(predictions),
        'methodology': 'Offline Evaluation (Accuracy)',
        'dataset': 'MovieLens ml-latest-small',
        'split': '80% train, 10% val, 10% test'
    }

    # Opzionale: Precision@K e NDCG@K
    # (Richiede ragionamenti sui top-K, tralasciato per semplicità)

    # Salva risultati
    output_dir = Path('results/traditional')
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_dir / 'test_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*80}")
    print(f"RISULTATI OFFLINE EVALUATION:")
    print(f"  RMSE: {results['rmse']:.4f}")
    print(f"  MAE:  {results['mae']:.4f}")
    print(f"{'='*80}")
    print(f"Risultati salvati in: {output_dir / 'test_results.json'}")

    return results

if __name__ == "__main__":
    run_offline_evaluation()
