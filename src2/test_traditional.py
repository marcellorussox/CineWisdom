#!/usr/bin/env python
"""
Test Runner per Traditional Pipeline

Questo script testa tutti i componenti della traditional pipeline:
1. SplitManager
2. TraditionalEvaluator
3. TraditionalPipeline

Esegui con: python src/test_traditional.py
"""

import sys
from pathlib import Path

# Aggiungi src al path
sys.path.insert(0, str(Path(__file__).parent))

def test_split_manager():
    """Testa il SplitManager."""
    print("="*70)
    print("TEST 1: SPLIT MANAGER")
    print("="*70)

    from src.data.split_manager import SplitManager

    manager = SplitManager(
        ratings_path="datasets/raw/ratings.csv",
        movies_path="datasets/processed/normalized_movies.csv",
        output_dir="datasets/splits"
    )

    # Testa caricamento dati
    print("\nTest 1.1: Loading data...")
    df = manager.load_data()
    print(f"✓ Loaded {len(df)} ratings")

    # Testa split creation
    print("\nTest 1.2: Creating splits...")
    train_df, val_df, test_df = manager.create_traditional_split(
        test_size=0.2,
        val_size=0.1,
        random_state=42,
        min_ratings_per_user=5
    )
    print(f"✓ Created splits: Train={len(train_df)}, Val={len(val_df)}, Test={len(test_df)}")

    # Testa salvataggio
    print("\nTest 1.3: Saving splits...")
    file_paths = manager.save_splits(train_df, val_df, test_df)
    print(f"✓ Saved splits: {list(file_paths.values())}")

    print("\n✓ SplitManager tests passed!")
    return True


def test_evaluator():
    """Testa il TraditionalEvaluator."""
    print("\n" + "="*70)
    print("TEST 2: TRADITIONAL EVALUATOR")
    print("="*70)

    import pandas as pd
    import numpy as np
    from src.evaluation.traditional_evaluator import TraditionalEvaluator

    # Crea dati mock
    np.random.seed(42)
    n_ratings = 100

    predictions = pd.DataFrame({
        'userId': np.random.randint(1, 11, n_ratings),
        'movieId': np.random.randint(1, 101, n_ratings),
        'rating': np.random.uniform(1, 5, n_ratings),
        'prediction': np.random.uniform(1, 5, n_ratings)
    })

    test_data = predictions[['userId', 'movieId', 'rating']].copy()

    print(f"\nTest 2.1: Created mock data ({len(predictions)} predictions)")

    # Testa evaluator
    print("\nTest 2.2: Evaluating model...")
    evaluator = TraditionalEvaluator()
    results = evaluator.evaluate_model(
        predictions=predictions,
        test_data=test_data,
        recommended_items={
            user_id: list(np.random.choice(range(1, 101), 20, replace=False))
            for user_id in range(1, 11)
        },
        model_name="TestModel"
    )

    print(f"✓ Evaluator computed metrics: RMSE={results['rmse']:.4f}, MAE={results['mae']:.4f}")

    print("\n✓ TraditionalEvaluator tests passed!")
    return True


def test_pipeline():
    """Testa la TraditionalPipeline."""
    print("\n" + "="*70)
    print("TEST 3: TRADITIONAL PIPELINE")
    print("="*70)

    from src.pipeline.traditional_pipeline import TraditionalPipeline

    # Crea pipeline
    pipeline = TraditionalPipeline(
        data_dir="datasets",
        output_dir="results/traditional/test_run",
        random_state=42
    )

    print("\nTest 3.1: Pipeline initialized")

    # Testa loading data
    print("\nTest 3.2: Loading data...")
    pipeline.load_data()
    print(f"✓ Loaded {len(pipeline.movies_catalog)} movies")

    # Testa splits creation
    print("\nTest 3.3: Creating splits...")
    pipeline.create_splits()
    print(f"✓ Created splits")

    # Testa recommender initialization
    print("\nTest 3.4: Initializing recommender...")
    pipeline.initialize_recommender()
    print(f"✓ KBRS initialized")

    # Testa training (lazy learning)
    print("\nTest 3.5: Training model...")
    pipeline.train_model()
    print(f"✓ Model ready for predictions")

    print("\n✓ TraditionalPipeline tests passed!")
    return True


def run_all_tests():
    """Esegue tutti i test."""
    print("\n" + "="*70)
    print("TRADITIONAL PIPELINE - COMPREHENSIVE TEST SUITE")
    print("="*70)

    tests = [
        ("SplitManager", test_split_manager),
        ("TraditionalEvaluator", test_evaluator),
        ("TraditionalPipeline", test_pipeline),
    ]

    results = {}

    for test_name, test_func in tests:
        try:
            print(f"\n\nRunning {test_name} tests...")
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n✗ {test_name} FAILED: {e}")
            import traceback
            traceback.print_exc()
            results[test_name] = False

    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)

    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name:25s} {status}")

    all_passed = all(results.values())

    print("\n" + "="*70)
    if all_passed:
        print("✓ ALL TESTS PASSED!")
    else:
        print("✗ SOME TESTS FAILED")
    print("="*70)

    return all_passed


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
