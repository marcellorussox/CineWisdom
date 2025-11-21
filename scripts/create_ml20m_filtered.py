#!/usr/bin/env python3
"""
Create ML-20M-Filtered dataset:
- Download ML-20M ratings
- Filter by ML-100k movies (for high density)
- Setup as new dataset

Expected result: ~15-18M ratings on ~1.7k movies = 50%+ density!
"""

import pandas as pd
import os
import requests
import zipfile
import shutil
from pathlib import Path

# Paths
ml100k_movies = 'datasets/ml-small-100k/raw/movies.csv'
ml20m_url = 'https://files.grouplens.org/datasets/movielens/ml-20m.zip'
temp_dir = '/tmp/ml-20m-download'
output_dir = 'datasets/ml-20m-filtered/raw'

print("🎬 Creating ML-20M-Filtered Dataset")
print("=" * 60)

# 1. Load ML-100k movies as reference
print("\n1. Loading ML-100k movies as reference...")
df_100k = pd.read_csv(ml100k_movies)
print(f"   ML-100k: {len(df_100k)} movies")

# Create title lookup (case-insensitive, stripped)
titles_100k_lower = df_100k['title'].str.strip().str.lower().tolist()
print(f"   Reference titles: {len(titles_100k_lower)}")

# 2. Download ML-20M (if not already downloaded)
print("\n2. Downloading ML-20M (~200MB, may take a few minutes)...")
os.makedirs(temp_dir, exist_ok=True)
zip_path = f"{temp_dir}/ml-20m.zip"

if not os.path.exists(zip_path):
    print("   Downloading...")
    response = requests.get(ml20m_url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    
    with open(zip_path, 'wb') as f:
        downloaded = 0
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            progress = (downloaded / total_size) * 100 if total_size > 0 else 0
            print(f"\r   Progress: {progress:.1f}%", end='', flush=True)
    print("\n   ✅ Download complete!")
else:
    print("   ✅ Already downloaded")

# 3. Extract only what we need
print("\n3. Extracting ML-20M data...")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    # Extract only movies.csv and ratings.csv
    zip_ref.extract('ml-20m/movies.csv', temp_dir)
    zip_ref.extract('ml-20m/ratings.csv', temp_dir)
print("   ✅ Extracted")

# 4. Find common movies
print("\n4. Finding common movies...")
df_20m_movies = pd.read_csv(f"{temp_dir}/ml-20m/movies.csv")
print(f"   ML-20M total movies: {len(df_20m_movies)}")

# Match by title (case-insensitive)
df_20m_movies['title_lower'] = df_20m_movies['title'].str.strip().str.lower()
common_movies = df_20m_movies[df_20m_movies['title_lower'].isin(titles_100k_lower)]

print(f"   Common movies: {len(common_movies)}")
print(f"   Match rate: {len(common_movies)/len(df_100k)*100:.1f}%")

common_movie_ids = set(common_movies['movieId'].tolist())

# 5. Filter ratings (this is the big one!)
print("\n5. Filtering 20M ratings (this may take a minute)...")
chunk_size = 1_000_000
filtered_chunks = []
total_ratings = 0
kept_ratings = 0

for i, chunk in enumerate(pd.read_csv(f"{temp_dir}/ml-20m/ratings.csv", chunksize=chunk_size)):
    total_ratings += len(chunk)
    filtered = chunk[chunk['movieId'].isin(common_movie_ids)]
    kept_ratings += len(filtered)
    filtered_chunks.append(filtered)
    print(f"\r   Processed: {total_ratings:,} ratings, kept: {kept_ratings:,}", end='', flush=True)

print(f"\n   ✅ Kept {kept_ratings:,} / {total_ratings:,} ratings ({kept_ratings/total_ratings*100:.1f}%)")

filtered_ratings = pd.concat(filtered_chunks, ignore_index=True)

# 6. Calculate density
n_users = filtered_ratings['userId'].nunique()
n_movies = filtered_ratings['movieId'].nunique()
density = len(filtered_ratings) / (n_users * n_movies) * 100

print(f"\n📊 ML-20M-Filtered Dataset Stats:")
print(f"   Users:   {n_users:,}")
print(f"   Movies:  {n_movies:,}")
print(f"   Ratings: {len(filtered_ratings):,}")
print(f"   Density: {density:.2f}%")
print(f"   Avg ratings/user: {len(filtered_ratings)/n_users:.1f}")
print(f"   Avg ratings/movie: {len(filtered_ratings)/n_movies:.1f}")

# 7. Save to new dataset folder
print(f"\n6. Saving to {output_dir}...")
os.makedirs(output_dir, exist_ok=True)

# Save filtered ratings
filtered_ratings.to_csv(f"{output_dir}/ratings.csv", index=False)

# Save movies (drop temp column)
common_movies_clean = common_movies.drop(columns=['title_lower'])
common_movies_clean.to_csv(f"{output_dir}/movies.csv", index=False)

# Copy links.csv from ML-1M (since we generated it there)
if os.path.exists('datasets/ml-1m/raw/links.csv'):
    links_df = pd.read_csv('datasets/ml-1m/raw/links.csv')
    # Filter for our common movies
    links_filtered = links_df[links_df['movieId'].isin(common_movie_ids)]
    links_filtered.to_csv(f"{output_dir}/links.csv", index=False)
    print(f"   ✅ Saved {len(links_filtered)} links")

print("   ✅ All files saved")

# 8. Cleanup
print("\n7. Cleaning up temporary files...")
shutil.rmtree(temp_dir)
print("   ✅ Cleanup complete")

print("\n" + "=" * 60)
print("✅ ML-20M-Filtered dataset ready!")
print(f"   Location: {output_dir}")
print(f"\nNext steps:")
print("   1. python main_pipeline.py --dataset ml-20m-filtered --mode preprocess --enrich")
print("   2. python main_pipeline.py --dataset ml-20m-filtered --mode split --online_split 0.2")
print("   3. python main_pipeline.py --dataset ml-20m-filtered --mode train --use_features ...")
