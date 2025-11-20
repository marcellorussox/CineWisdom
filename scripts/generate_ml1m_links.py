import pandas as pd
import os
import requests
import zipfile
import io
import shutil
import re

def download_and_extract_metadata():
    """Downloads ML-20M and extracts only movies.csv and links.csv."""
    url = "https://files.grouplens.org/datasets/movielens/ml-20m.zip"
    print(f"⬇️  Downloading ML-20M metadata from {url}...")
    
    response = requests.get(url, stream=True)
    z = zipfile.ZipFile(io.BytesIO(response.content))
    
    # Extract only needed files to a temp dir
    temp_dir = "temp_ml20m"
    os.makedirs(temp_dir, exist_ok=True)
    
    print("📦 Extracting movies.csv and links.csv...")
    z.extract("ml-20m/movies.csv", temp_dir)
    z.extract("ml-20m/links.csv", temp_dir)
    
    return temp_dir

def clean_title(title):
    """Removes year from title for better matching. e.g. 'Toy Story (1995)' -> 'Toy Story'"""
    return re.sub(r'\s*\(\d{4}\)\s*$', '', title).strip().lower()

def generate_links():
    ml1m_movies_path = "datasets/ml-1m/raw/movies.csv"
    output_path = "datasets/ml-1m/raw/links.csv"
    
    if not os.path.exists(ml1m_movies_path):
        print(f"❌ Error: {ml1m_movies_path} not found.")
        return

    # 1. Get Reference Data (ML-20M)
    temp_dir = download_and_extract_metadata()
    
    print("🔄 Loading reference data...")
    ref_movies = pd.read_csv(f"{temp_dir}/ml-20m/movies.csv")
    ref_links = pd.read_csv(f"{temp_dir}/ml-20m/links.csv")
    
    # Merge ref movies with links
    ref_data = ref_movies.merge(ref_links, on='movieId')
    
    # Create lookup dictionary: { "toy story": imdbId, ... }
    # We use title as key because movieIds are different between datasets
    print("🔑 Creating lookup dictionary...")
    ref_data['clean_title'] = ref_data['title'].apply(clean_title)
    # Drop duplicates, keeping the one with lower movieId (usually original)
    ref_data = ref_data.sort_values('movieId').drop_duplicates('clean_title')
    
    title_to_imdb = dict(zip(ref_data['clean_title'], ref_data['imdbId']))
    title_to_tmdb = dict(zip(ref_data['clean_title'], ref_data['tmdbId']))

    # 2. Process ML-1M Movies
    print("⚡ Processing ML-1M movies...")
    ml1m_movies = pd.read_csv(ml1m_movies_path, encoding='latin-1')
    
    links_data = []
    matched_count = 0
    
    for _, row in ml1m_movies.iterrows():
        clean_t = clean_title(row['title'])
        imdb_id = title_to_imdb.get(clean_t)
        tmdb_id = title_to_tmdb.get(clean_t)
        
        if imdb_id:
            matched_count += 1
            links_data.append({
                'movieId': row['movieId'],
                'imdbId': int(imdb_id),
                'tmdbId': int(tmdb_id) if pd.notna(tmdb_id) else None
            })
        else:
            # Fallback: try exact match without cleaning
            pass 

    # 3. Save Result
    links_df = pd.DataFrame(links_data)
    links_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Generated links.csv for ML-1M")
    print(f"   - Total Movies: {len(ml1m_movies)}")
    print(f"   - Matched Links: {matched_count}")
    print(f"   - Coverage: {matched_count / len(ml1m_movies) * 100:.2f}%")
    
    # 4. Cleanup
    print("🧹 Cleaning up temp files...")
    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    generate_links()
