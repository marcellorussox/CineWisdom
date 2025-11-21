"""
Convert MovieLens 1M .dat files to CSV format compatible with CineWisdom pipeline.
"""
import pandas as pd
import os

# Paths
RAW_DIR = 'datasets/ml-1m-raw'
OUT_DIR = 'datasets/ml-1m/raw'

os.makedirs(OUT_DIR, exist_ok=True)

print("🔄 Converting MovieLens 1M to CSV format...")

# 1. Convert ratings.dat
print("  - Converting ratings.dat...")
ratings = pd.read_csv(
    f'{RAW_DIR}/ratings.dat',
    sep='::',
    engine='python',
    names=['userId', 'movieId', 'rating', 'timestamp'],
    encoding='latin-1'
)
ratings.to_csv(f'{OUT_DIR}/ratings.csv', index=False)
print(f"    ✅ Saved {len(ratings):,} ratings to {OUT_DIR}/ratings.csv")

# 2. Convert movies.dat
print("  - Converting movies.dat...")
movies = pd.read_csv(
    f'{RAW_DIR}/movies.dat',
    sep='::',
    engine='python',
    names=['movieId', 'title', 'genres'],
    encoding='latin-1'
)
movies.to_csv(f'{OUT_DIR}/movies.csv', index=False)
print(f"    ✅ Saved {len(movies):,} movies to {OUT_DIR}/movies.csv")

# 3. Convert users.dat (optional, for future use)
print("  - Converting users.dat...")
users = pd.read_csv(
    f'{RAW_DIR}/users.dat',
    sep='::',
    engine='python',
    names=['userId', 'gender', 'age', 'occupation', 'zipCode'],
    encoding='latin-1'
)
users.to_csv(f'{OUT_DIR}/users.csv', index=False)
print(f"    ✅ Saved {len(users):,} users to {OUT_DIR}/users.csv")

print("\n✅ Conversion complete!")
print(f"\nDataset Statistics:")
print(f"  - Users: {ratings['userId'].nunique():,}")
print(f"  - Movies: {ratings['movieId'].nunique():,}")
print(f"  - Ratings: {len(ratings):,}")
print(f"  - Sparsity: {(1 - len(ratings) / (ratings['userId'].nunique() * ratings['movieId'].nunique())) * 100:.2f}%")
