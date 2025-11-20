#!/usr/bin/env python3
"""
KBRS Pipeline for MovieLens Small

This script implements the complete KBRS workflow:
1. Load ML-Small data
2. Enrich with DBpedia (SPARQL queries)
3. Create cosine similarity matrix from knowledge features
4. Split data (offline/online)
5. Run MAB simulation with KBRS arms (exploration vs exploitation)
6. Evaluate and visualize results

Dataset: MovieLens Small (~100k ratings, 9.7k movies, 610 users)
"""

import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.recommender.kbrs import KBRS
from src.data.manager import load_data, enrich_movies, normalize_movie_data_parallel
from src.data.split_manager import create_global_split, create_train_val_test_split
from src.simulation.kbrs_simulator import OnlineKBRSSimulator
from src.evaluation.kbrs_evaluator import KBRSEvaluator

# Paths
DATASET = 'ml-small-100k'
RAW_DIR = f'datasets/{DATASET}/raw'
PROCESSED_DIR = f'datasets/{DATASET}/processed'
SPLITS_DIR = 'datasets/splits'
MODELS_DIR = f'models/kbrs/{DATASET}'
RESULTS_DIR = f'results/{DATASET}/kbrs'

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)


def step1_load_and_enrich():
    """Load ML-Small and enrich with DBpedia."""
    print("\n" + "="*60)
    print("STEP 1: LOAD & ENRICH DATA")
    print("="*60)
    
    # Load data
    print(f"\n📚 Loading data from {RAW_DIR}...")
    ratings_df, movies_df, links_df = load_data(data_dir=RAW_DIR)
    
    print(f"\n Dataset Info:")
    print(f"  Users:   {ratings_df['userId'].nunique():,}")
    print(f"  Movies:  {len(movies_df):,}")
    print(f"  Ratings: {len(ratings_df):,}")
    
    # Merge with links
    if not links_df.empty:
        movies_df = movies_df.merge(links_df, on='movieId', how='left')
    
    # Enrich with DBpedia
    print(f"\n✨ Enriching with DBpedia/Wikidata...")
    movies_enriched = enrich_movies(movies_df, batch_size=25)
    
    # Save enriched data
    enriched_path = f"{PROCESSED_DIR}/movies_enriched.csv"
    movies_enriched.to_csv(enriched_path, index=False)
    print(f"✅ Enriched data saved to {enriched_path}")
    
    return ratings_df, movies_enriched


def step2_create_features():
    """Create feature matrix and cosine similarity."""
    print("\n" + "="*60)
    print("STEP 2: CREATE KNOWLEDGE FEATURES")
    print("="*60)
    
    # Load enriched data
    enriched_path = f"{PROCESSED_DIR}/movies_enriched.csv"
    movies_df = pd.read_csv(enriched_path)
    
    # Normalize features (one-hot encode genres, directors, actors)
    print(f"\n🔬 Normalizing features...")
    normalized_path = f"{PROCESSED_DIR}/normalized_movies.csv"
    normalized_df = normalize_movie_data_parallel(
        movies_df,
        output_path=normalized_path,
        max_features_per_category=1000  # Top 1000 per category
    )
    
    print(f"✅ Normalized features: {normalized_df.shape}")
    
    # Create cosine similarity matrix
    print(f"\n📐 Computing cosine similarity matrix...")
    feature_cols = [col for col in normalized_df.columns if col != 'movieId']
    features = normalized_df[feature_cols].fillna(0).values
    
    # Compute cosine similarity on raw features (0/1)
    # NOTE: We do NOT use StandardScaler here because it creates negative values
    # which leads to negative cosine similarities. For weighted average prediction,
    # we need similarities in [0, 1].
    cosine_sim_matrix = cosine_similarity(features)
    
    # Save matrix and movie IDs
    np.save(f"{MODELS_DIR}/cosine_sim_matrix.npy", cosine_sim_matrix)
    normalized_df[['movieId']].to_csv(f"{MODELS_DIR}/movie_ids.csv", index=False)
    
    print(f"✅ Cosine similarity matrix: {cosine_sim_matrix.shape}")
    print(f"✅ Saved to {MODELS_DIR}/")
    
    return normalized_df, cosine_sim_matrix


def step3_split_data():
    """Split data into offline/online sets."""
    print("\n" + "="*60)
    print("STEP 3: SPLIT DATA")
    print("="*60)
    
    # Load ratings
    ratings_df, _, _ = load_data(data_dir=RAW_DIR)
    
    # Global split (80% offline, 20% online)
    print(f"\n🌍 Creating global split (80% offline, 20% online)...")
    offline_df, online_df = create_global_split(
        ratings_df,
        online_size=0.2,
        min_ratings_per_user=5
    )
    
    # Offline split (train/val/test)
    print(f"\n📉 Creating offline splits (60/20/20)...")
    train_df, val_df, test_df = create_train_val_test_split(
        offline_df,
        val_size=0.2,
        test_size=0.2
    )
    
    # Save splits
    os.makedirs(SPLITS_DIR, exist_ok=True)
    train_df.to_csv(f"{SPLITS_DIR}/train.csv", index=False)
    val_df.to_csv(f"{SPLITS_DIR}/val.csv", index=False)
    test_df.to_csv(f"{SPLITS_DIR}/test.csv", index=False)
    online_df.to_csv(f"{SPLITS_DIR}/online.csv", index=False)
    
    print(f"\n✅ Splits saved to {SPLITS_DIR}/")
    print(f"  Train:  {len(train_df):,}")
    print(f"  Val:    {len(val_df):,}")
    print(f"  Test:   {len(test_df):,}")
    print(f"  Online: {len(online_df):,}")
    
    return train_df, val_df, test_df, online_df


def step4_evaluate_kbrs(train_df):
    """Evaluate KBRS on test set."""
    print("\n" + "="*60)
    print("STEP 4: EVALUATE KBRS")
    print("="*60)
    
    # Load data
    test_df = pd.read_csv(f"{SPLITS_DIR}/test.csv")
    movies_df = pd.read_csv(f"{PROCESSED_DIR}/movies_enriched.csv")
    movie_ids = pd.read_csv(f"{MODELS_DIR}/movie_ids.csv")['movieId']
    cosine_sim_matrix = np.load(f"{MODELS_DIR}/cosine_sim_matrix.npy")
    
    # Initialize KBRS
    kbrs = KBRS(unique_movie_catalog=movies_df)
    
    # Initialize evaluator
    evaluator = KBRSEvaluator(results_dir=RESULTS_DIR)
    
    # Evaluate
    results = evaluator.evaluate_offline(
        kbrs_model=kbrs,
        train_df=train_df,
        test_df=test_df,
        cosine_sim_matrix=cosine_sim_matrix,
        movie_ids=movie_ids
    )
    
    return results


def step5_online_mab_simulation():
    """Run online MAB simulation with KBRS strategies."""
    print("\n" + "="*60)
    print("STEP 5: ONLINE MAB SIMULATION")
    print("="*60)
    
    # Load all required data
    print("\n📚 Loading data...")
    train_df = pd.read_csv(f"{SPLITS_DIR}/train.csv")
    online_df = pd.read_csv(f"{SPLITS_DIR}/online.csv")
    movies_df = pd.read_csv(f"{PROCESSED_DIR}/movies_enriched.csv")
    movie_ids = pd.read_csv(f"{MODELS_DIR}/movie_ids.csv")['movieId']
    cosine_sim_matrix = np.load(f"{MODELS_DIR}/cosine_sim_matrix.npy")
    
    # Initialize KBRS
    print("\n🤖 Initializing KBRS...")
    kbrs = KBRS(unique_movie_catalog=movies_df)
    
    # Initialize online simulator
    print("\n🎯 Starting Online MAB Simulation...")
    simulator = OnlineKBRSSimulator(
        kbrs_model=kbrs,
        cosine_sim_matrix=cosine_sim_matrix,
        movie_ids=movie_ids,
        movies_catalog=movies_df,
        rating_scale=(0.5, 5.0)
    )
    
    # Run simulation
    sim_results = simulator.simulate_online(
        online_df=online_df,
        offline_ratings_df=train_df,
        n_recommendations=10,
        verbose=True
    )
    
    print(f"\n✅ Online Simulation Complete!")
    print(f"  Interactions: {sim_results['summary']['total_interactions']:,}")
    print(f"  Final RMSE:   {sim_results['summary']['final_rmse']:.4f}")
    print(f"  Mean Reward:  {sim_results['summary']['mean_reward']:.4f}")
    
    return sim_results


def step6_comprehensive_evaluation(offline_results, online_sim_results):
    """Run comprehensive evaluation and create visualizations."""
    print("\n" + "="*60)
    print("STEP 6: COMPREHENSIVE EVALUATION")
    print("="*60)
    
    # Initialize evaluator
    evaluator = KBRSEvaluator(results_dir=RESULTS_DIR)
    
    # Evaluate online results
    print("\n📊 Analyzing online performance...")
    online_results, history = evaluator.evaluate_online(online_sim_results)
    
    # Create plots
    print("\n🎨 Creating visualizations...")
    evaluator.plot_results(
        history=history,
        mab_stats=online_sim_results['mab_stats']
    )
    
    # Generate report
    print("\n📝 Generating evaluation report...")
    report = evaluator.generate_report(
        offline_results=offline_results,
        online_results=online_results
    )
    
    print("\n✅ Evaluation Complete!")
    return online_results, history


def main():
    """Run complete KBRS pipeline."""
    print("\n🎬 KBRS Pipeline for MovieLens Small")
    print("Knowledge-Based Recommender System with MAB")
    
    # Check if enriched data exists
    enriched_path = f"{PROCESSED_DIR}/movies_enriched.csv"
    
    if not os.path.exists(enriched_path):
        print("\n⚠️  Enriched data not found. Running Step 1...")
        step1_load_and_enrich()
    
    # Check if features exist
    if not os.path.exists(f"{MODELS_DIR}/cosine_sim_matrix.npy"):
        print("\n⚠️  Feature matrix not found. Running Step 2...")
        step2_create_features()
    
    # Check if splits exist
    if not os.path.exists(f"{SPLITS_DIR}/train.csv"):
        print("\n⚠️  Splits not found. Running Step 3...")
        step3_split_data()
    
    # Load train_df for evaluation
    train_df = pd.read_csv(f"{SPLITS_DIR}/train.csv")
    
    # Offline evaluation
    print("\n📊 Running offline evaluation...")
    offline_results = step4_evaluate_kbrs(train_df)
    
    # Online MAB simulation
    print("\n🎯 Running online MAB simulation...")
    online_sim_results = step5_online_mab_simulation()
    
    # Comprehensive evaluation
    print("\n📈 Running comprehensive evaluation...")
    online_results, history = step6_comprehensive_evaluation(
        offline_results=offline_results,
        online_sim_results=online_sim_results
    )
    
    print("\n" + "="*60)
    print("✅ COMPLETE KBRS+MAB PIPELINE FINISHED!")
    print("="*60)
    print(f"\nResults saved to: {RESULTS_DIR}/")
    print(f"Models saved to: {MODELS_DIR}/")
    print(f"\nKey Findings:")
    print(f"  Offline RMSE: {offline_results['rmse']:.4f}")
    print(f"  Online RMSE:  {online_sim_results['summary']['final_rmse']:.4f}")
    print(f"  Exploration:  {online_sim_results['summary']['exploration_rate']:.1%}")
    print(f"  Exploitation: {online_sim_results['summary']['exploitation_rate']:.1%}")
    
    return offline_results, online_sim_results, online_results


if __name__ == "__main__":
    main()
