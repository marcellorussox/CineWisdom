"""
CineWisdom Complete Pipeline: Preprocessing -> Splitting -> NCF Training -> Evaluation

This script orchestrates the entire lifecycle of the recommender system:
1. Data Loading & Preprocessing (Cleaning, Enrichment, Normalization)
2. Dataset Splitting (Offline vs Online, Train/Val/Test)
3. Feature Extraction (SVD for NCF)
4. Model Training (NCF on Offline Data)
5. Evaluation (Offline Metrics)
6. Online Simulation (MAB Learning)
"""

import os
import sys
import argparse
import torch
import pandas as pd
import numpy as np
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data.manager import (
    load_data,
    clean_partial_rows,
    enrich_movies,
    normalize_ratings_data,
    normalize_movie_data_parallel,
    extract_svd_features_for_ncf
)
from src.data.split_manager import (
    create_global_split,
    create_train_val_test_split,
    save_splits,
    load_splits
)
from src.data.dataset import create_data_loaders
from src.models.ncf import NCF
from src.training.trainer import NCFTrainer
from src.evaluation.metrics import evaluate_predictions
from src.simulation.simulator import OnlineSimulator
from src.bandit.policy import ThompsonSampling, EpsilonGreedy, UCB1


def get_dataset_paths(dataset_name):
    """Get dataset-specific paths."""
    base = f'datasets/{dataset_name}'
    return {
        'raw_dir': f'{base}/raw',
        'processed_dir': f'{base}/processed',
        'splits_dir': f'{base}/splits',
        'models_dir': f'models/{dataset_name}',
        'results_dir': f'results/{dataset_name}',
        'plots_dir': f'plots/{dataset_name}'
    }



def run_preprocessing(args):
    """Run data preprocessing pipeline."""
    print("\n🚀 STARTING PREPROCESSING")
    print("=" * 60)
    
    # Get dataset-specific paths
    paths = get_dataset_paths(args.dataset)

    # 1. Load Raw Data
    print(f"\n📚 Loading raw data from {paths['raw_dir']}...")
    ratings_df, movies_df, links_df = load_data(data_dir=paths['raw_dir'])
    
    if ratings_df is None or movies_df is None:
        print("❌ Error loading data. Exiting.")
        return None

    # 2. Clean Data
    print("\n🧹 Cleaning data...")
    
    # Only merge links if it contains data (ML-Small has it, ML-1M doesn't)
    if not links_df.empty and 'imdbId' in links_df.columns:
        print("  - Merging with links.csv...")
        movies_df = movies_df.merge(links_df, on='movieId', how='left')
    else:
        print("  - No links.csv available, proceeding without imdbId")
        # Add empty imdbId column for compatibility
        if 'imdbId' not in movies_df.columns:
            movies_df['imdbId'] = pd.NA
    
    # Clean only critical columns (don't drop rows with missing imdbId for ML-1M)
    # Only drop rows with missing movieId or title
    initial_count = len(movies_df)
    movies_df = movies_df.dropna(subset=['movieId', 'title'])
    final_count = len(movies_df)
    
    if initial_count > final_count:
        print(f"  - Dropped {initial_count - final_count} rows with missing critical data")
    
    print(f"  - {final_count} movies ready for processing")
    
   # 3. Enrich Movies (Optional but recommended for better features)
    if args.enrich:
        print("\n✨ Enriching movies with DBpedia/Wikidata (this may take a while)...")
        # Update OUTPUT_FILE and OUTPUT_FOLDER for enrichment
        import src.data.manager as manager_module
        manager_module.OUTPUT_FOLDER = paths['processed_dir']
        manager_module.OUTPUT_FILE = os.path.join(paths['processed_dir'], 'dbpedia_data.csv')
        
        movies_df = enrich_movies(movies_df)
    else:
        print("\n⏭️  Skipping enrichment (use --enrich to enable)")

    # Deduplicate movies before normalization to avoid explosion (e.g. from bad merges)
    print(f"\n🧹 Deduplicating movies (before: {len(movies_df)})...")
    movies_df = movies_df.drop_duplicates(subset='movieId')
    print(f"  - Movies after deduplication: {len(movies_df)}")

    # 4. Normalize and Extract Features
    print("\n🔬 Normalizing movie features...")
    normalized_path = os.path.join(paths['processed_dir'], 'normalized_movies.csv')
    normalized_df = normalize_movie_data_parallel(movies_df, output_path=normalized_path)
    
    print("\n🧬 Extracting SVD features for NCF...")
    svd_feature_path = os.path.join(paths['processed_dir'], 'movie_features_ncf_svd.csv')
    feature_df, svd_model = extract_svd_features_for_ncf(
        normalized_df,
        n_components=1024,
        output_path=svd_feature_path
    )

    print(f"\n✅ Preprocessing completed! Dataset: {args.dataset}")
    print(f"   - Processed data: {paths['processed_dir']}")
    
    return ratings_df, movies_df


def run_splitting(ratings_df, args):
    """Run data splitting pipeline."""
    print("\n✂️ STARTING DATA SPLITTING")
    print("=" * 60)
    
    # 1. Global Split: Offline vs Online
    print("\n🌍 Creating Global Split (Offline vs Online)...")
    offline_df, online_df = create_global_split(
        ratings_df,
        online_size=args.online_split,
        min_ratings_per_user=5
    )
    
    # 2. Offline Split: Train/Val/Test
    print("\n📉 Creating Offline Splits (Train/Val/Test)...")
    train_df, val_df, test_df = create_train_val_test_split(
        offline_df,
        test_size=0.1,
        val_size=0.1
    )
    
    # 3. Save Splits
    print("\n💾 Saving splits...")
    save_splits(train_df, val_df, test_df, online_df)
    
    return train_df, val_df, test_df, online_df


def run_training(train_df, val_df, test_df, args):
    """Run NCF training pipeline."""
    print("\n🏋️ STARTING NCF TRAINING")
    print("=" * 60)
    
    # 1. Load movie features if requested
    movie_features_df = None
    feature_dim = 0
    if args.use_features:
        print("\n🧬 Extracting SVD features for movies...")
        paths = get_dataset_paths(args.dataset)
        feature_path = os.path.join(paths['processed_dir'], 'movie_features_ncf_svd.csv')
        if os.path.exists(feature_path):
            print(f"Loading features from {feature_path}")
            movie_features_df = pd.read_csv(feature_path)
            # Check dimensions (exclude movieId)
            feature_dim = len(movie_features_df.columns) - 1
            print(f"✅ Loaded {feature_dim} features.")
        else:
            print("⚠️ Feature file not found. Skipping features for this run.")
            print("Run with --enrich to generate features.")
    
    # 2. Create DataLoaders
    print("\n📦 Creating DataLoaders...")
    train_loader, val_loader, test_loader, id_maps = create_data_loaders(
        train_df,
        val_df,
        test_df,
        movie_features_df=movie_features_df,
        feature_dim=feature_dim,
        batch_size=args.batch_size
    )
    
    # Parse MLP dims
    mlp_dims = [int(x) for x in args.mlp_dims.split(',')]
    
    # 3. Initialize Model
    print("\n🤖 Initializing NCF Model...")
    model = NCF(
        num_users=id_maps['num_users'],
        num_movies=id_maps['num_movies'],
        feature_dim=feature_dim,
        embedding_dim=args.embedding_dim,
        mlp_hidden_dims=mlp_dims,
        mlp_dropout=args.dropout,
        use_movie_features=(movie_features_df is not None)
    )
    
    # 4. Train
    print("\n🔥 Training...")
    trainer = NCFTrainer(
        model,
        learning_rate=args.lr,
        weight_decay=args.weight_decay
    )
    
    history = trainer.train(
        train_loader,
        val_loader,
        num_epochs=args.epochs,
        early_stopping_patience=args.patience,
        output_dir='models'
    )
    
    # 5. Test
    print("\n🧪 Testing...")
    test_metrics = trainer.test(test_loader)
    print(f"\n✅ Test Results:")
    print(f"  RMSE: {test_metrics['rmse']:.4f}")
    print(f"  MAE:  {test_metrics['mae']:.4f}")
    
    return trainer, test_metrics


def run_online_simulation(args):
    """Run Online MAB Simulation."""
    print("\n🎮 STARTING ONLINE MAB SIMULATION")
    print("=" * 60)
    
    # 1. Load Data
    print("\n📂 Loading data...")
    try:
        splits = load_splits()
        train_df = splits['train']
        online_df = splits['online']
    except FileNotFoundError:
        print("❌ Splits not found. Run with --mode split first.")
        return

    # 2. Load Model
    print("\n🤖 Loading NCF Model...")
    model_path = 'models/ncf_final.pt'
    if not os.path.exists(model_path):
        print(f"❌ Model not found at {model_path}. Run with --mode train first.")
        return
        
    # Initialize model structure (needed to load weights)
    # The split files contain original IDs (userId, movieId), not mapped indices.
    # We need num_users and num_movies to match what was used during training.
    # We get this from the unique count of IDs across all splits.
    
    all_user_ids = pd.concat([train_df['userId'], online_df['userId']]).unique()
    all_movie_ids = pd.concat([train_df['movieId'], online_df['movieId']]).unique()
    
    num_users = len(all_user_ids)
    num_movies = len(all_movie_ids)
    
    # Check if feature file exists to determine feature_dim
    feature_path = 'datasets/processed/movie_features_ncf_svd.csv'
    feature_dim = 0
    use_features = False
    if os.path.exists(feature_path):
        # Just read header
        header = pd.read_csv(feature_path, nrows=0)
        feature_dim = len(header.columns) - 1
        use_features = True
        print(f"  Found features: {feature_dim} dims")

    # Parse MLP dims
    mlp_dims = [int(x) for x in args.mlp_dims.split(',')]

    model = NCF(
        num_users=num_users,
        num_movies=num_movies,
        feature_dim=feature_dim,
        embedding_dim=args.embedding_dim,
        mlp_hidden_dims=mlp_dims,
        mlp_dropout=args.dropout,
        use_movie_features=use_features
    )
    
    # Load weights
    # Load weights with mismatch handling
    try:
        # Try standard load first
        model.load_model(model_path)
    except Exception as e:
        print(f"⚠️ Warning: Standard load failed ({e}). Attempting flexible load...")
        
        # Flexible load for size mismatches (Cold Start scenario)
        import torch
        device = next(model.parameters()).device
        checkpoint = torch.load(model_path, map_location=device)
        model_state = model.state_dict()
        pretrained_state = checkpoint['model_state_dict'] if 'model_state_dict' in checkpoint else checkpoint
        
        new_state = {}
        for k, v in pretrained_state.items():
            if k in model_state:
                if v.shape == model_state[k].shape:
                    new_state[k] = v
                else:
                    # Handle embedding mismatch: copy common part
                    if 'embedding' in k or 'bias' in k:
                        min_rows = min(v.shape[0], model_state[k].shape[0])
                        # If dimensions match (cols), copy rows
                        if len(v.shape) > 1 and v.shape[1] == model_state[k].shape[1]:
                             model_state[k][:min_rows] = v[:min_rows]
                             new_state[k] = model_state[k]
                             print(f"  - Partial load for {k}: copied {min_rows} rows")
                        elif len(v.shape) == 1: # Bias
                             model_state[k][:min_rows] = v[:min_rows]
                             new_state[k] = model_state[k]
                             print(f"  - Partial load for {k}: copied {min_rows} rows")
                    else:
                        print(f"  - Skipping {k}: shape mismatch {v.shape} vs {model_state[k].shape}")
        
        # Load the patched state
        model.load_state_dict(model_state, strict=False)
        print("✅ Model loaded with partial weights (Cold Start items initialized randomly)")

    # 3. Initialize Bandit
    print(f"\n🎰 Initializing Bandit Policy: {args.bandit}...")
    if args.bandit == 'thompson':
        policy = ThompsonSampling(num_arms=2)
    elif args.bandit == 'epsilon':
        policy = EpsilonGreedy(num_arms=2, config=type('Config', (), {'epsilon': 0.1})())
    elif args.bandit == 'ucb':
        policy = UCB1(num_arms=2)
    else:
        print(f"Unknown bandit: {args.bandit}")
        return

    # 4. Run Simulation
    print("\n▶️ Running Simulation...")
    simulator = OnlineSimulator(
        model=model,
        bandit_policy=policy,
        online_df=online_df,
        train_df=train_df
    )
    
    history_df = simulator.run(limit=args.limit)
    
    # 5. Results
    print("\n📊 Simulation Results:")
    print(history_df.groupby('arm_name')['reward'].mean())
    print(f"\nTotal Cumulative Reward: {history_df['reward'].sum():.2f}")
    
    # Save results
    os.makedirs('results', exist_ok=True)
    history_df.to_csv(f'results/online_simulation_{args.bandit}.csv', index=False)
    print(f"Results saved to results/online_simulation_{args.bandit}.csv")
    
    # 6. Visualize Learning Curve
    print("\n📈 Generating learning curve...")
    from src.viz.plot_manager import plot_online_learning_curve
    plot_online_learning_curve(
        history_df,
        output_path=f'plots/online_learning_{args.bandit}.png',
        window_size=500
    )


def main():
    parser = argparse.ArgumentParser(description="CineWisdom Pipeline")
    parser.add_argument('--dataset', type=str, default='ml-1m', choices=['ml-small-100k', 'ml-1m'], help='Dataset to use')
    parser.add_argument('--mode', type=str, default='all', choices=['all', 'preprocess', 'split', 'train', 'online'], help='Pipeline mode')
    parser.add_argument('--enrich', action='store_true', help='Enable DBpedia enrichment (slow)')
    parser.add_argument('--use_features', action='store_true', help='Use movie features for NCF')
    parser.add_argument('--online_split', type=float, default=0.2, help='Fraction of data for Online MAB')
    parser.add_argument('--epochs', type=int, default=20, help='Number of training epochs')
    parser.add_argument('--batch_size', type=int, default=512, help='Batch size')
    parser.add_argument('--embedding_dim', type=int, default=64, help='Embedding dimension')
    parser.add_argument('--lr', type=float, default=0.001, help='Learning rate')
    parser.add_argument('--weight_decay', type=float, default=1e-5, help='Weight decay (L2 regularization)')
    parser.add_argument('--dropout', type=float, default=0.2, help='Dropout probability')
    parser.add_argument('--mlp_dims', type=str, default='256,128,64', help='MLP hidden dimensions (comma-separated)')
    parser.add_argument('--patience', type=int, default=3, help='Early stopping patience')
    parser.add_argument('--bandit', type=str, default='thompson', choices=['thompson', 'epsilon', 'ucb'], help='Bandit algorithm')
    parser.add_argument('--limit', type=int, default=None, help='Limit online interactions')
    
    args = parser.parse_args()
    
    print("🎬 CineWisdom Pipeline Started")
    
    # Step 1: Preprocessing
    if args.mode in ['all', 'preprocess']:
        ratings_df, movies_df = run_preprocessing(args)
        if args.mode == 'preprocess':
            return

    # Step 2: Splitting
    if args.mode in ['all', 'split']:
        # If we skipped preprocess, we need to load data
        if 'ratings_df' not in locals():
            paths = get_dataset_paths(args.dataset)
            ratings_df, _, _ = load_data(data_dir=paths['raw_dir'])
            
        train_df, val_df, test_df, online_df = run_splitting(ratings_df, args)
        if args.mode == 'split':
            return

    # Step 3: Training
    if args.mode in ['all', 'train']:
        # If we skipped split, load splits
        if 'train_df' not in locals():
            try:
                splits = load_splits()
                train_df = splits['train']
                val_df = splits['val']
                test_df = splits['test']
            except FileNotFoundError:
                print("❌ Splits not found. Run with --mode split first.")
                return

        trainer, metrics = run_training(train_df, val_df, test_df, args)
        
        # Save final model
        trainer.save_model('models/ncf_final.pt')
        
    # Step 4: Online Simulation
    if args.mode == 'online':
        run_online_simulation(args)

    print("\n✨ Pipeline Completed Successfully!")


if __name__ == "__main__":
    main()
