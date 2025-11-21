#!/usr/bin/env python3
"""
Filter ML-1M to keep only movies that exist in ML-100k.
This reduces sparsity while keeping more ratings.
"""

import pandas as pd
import os

# Paths
ml100k_movies = 'datasets/raw/movies.csv'
ml1m_movies = 'datasets/ml-1m/raw/movies.csv'
ml1m_ratings = 'datasets/ml-1m/raw/ratings.csv'

# Output
output_ratings = 'datasets/ml-1m/raw/ratings_filtered.csv'

print("🎬 Filtering ML-1M by ML-100k movies...")

# 1. Load movie lists
print("\n1. Loading movie datasets...")
df_100k = pd.read_csv(ml100k_movies)
df_1m = pd.read_csv(ml1m_movies)

print(f"  ML-100k: {len(df_100k)} movies")
print(f"  ML-1M:   {len(df_1m)} movies")

# 2. Find common movies by title (case-insensitive, strip whitespace)
print("\n2. Finding common movies by title...")
titles_100k = set(df_100k['title'].str.strip().str.lower())
titles_1m_map = df_1m.set_index(df_1m['title'].str.strip().str.lower())['movieId'].to_dict()

common_titles = titles_100k.intersection(set(titles_1m_map.keys()))
common_movie_ids = [titles_1m_map[title] for title in common_titles]

print(f"  Common movies: {len(common_movie_ids)}")

# 3. Filter ratings
print("\n3. Filtering ratings...")
ratings_df = pd.read_csv(ml1m_ratings)
print(f"  Original ratings: {len(ratings_df):,}")

filtered_ratings = ratings_df[ratings_df['movieId'].isin(common_movie_ids)]
print(f"  Filtered ratings: {len(filtered_ratings):,}")

# 4. Calculate density
n_users = filtered_ratings['userId'].nunique()
n_movies = filtered_ratings['movieId'].nunique()
density = len(filtered_ratings) / (n_users * n_movies) * 100

print(f"\n📊 Filtered Dataset Stats:")
print(f"  Users:   {n_users:,}")
print(f"  Movies:  {n_movies:,}")
print(f"  Ratings: {len(filtered_ratings):,}")
print(f"  Density: {density:.2f}%")

# 5. Save
print(f"\n💾 Saving to {output_ratings}...")
filtered_ratings.to_csv(output_ratings, index=False)

print("\n✅ Done! Use 'ratings_filtered.csv' for training.")
