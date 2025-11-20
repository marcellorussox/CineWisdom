#!/bin/bash
# Cleanup script to remove unused datasets and temporary files
# Keeps ml-small-100k and results

echo "🧹 Starting cleanup..."

# 1. Remove unused datasets
echo "  Removing ml-1m..."
rm -rf datasets/ml-1m
rm -rf datasets/ml-1m-raw
rm -f datasets/ml-1m.zip

echo "  Removing ml-20m-filtered..."
rm -rf datasets/ml-20m-filtered

# 2. Remove misplaced directories in datasets/ root (if any)
# Check content first to be safe
if [ -d "datasets/processed" ]; then
    echo "  Removing misplaced datasets/processed..."
    rm -rf datasets/processed
fi
if [ -d "datasets/raw" ]; then
    echo "  Removing misplaced datasets/raw..."
    rm -rf datasets/raw
fi

# 3. Clean archived if needed (optional, keeping for now as requested "results")
# rm -rf datasets/archived 

echo "✅ Cleanup complete. Remaining datasets:"
ls -F datasets/
