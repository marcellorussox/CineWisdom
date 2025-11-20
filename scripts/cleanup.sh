#!/bin/bash
# Cleanup script for CineWisdom project
# Removes intermediate files while preserving important results

echo "🧹 Cleaning up intermediate files..."

# Remove checkpoint files (can be regenerated)
echo "  Removing DBpedia checkpoints..."
find datasets/*/processed -name "dbpedia_data.csv" -delete

# Remove old optimized files
echo "  Removing old optimized files..."
find datasets/*/processed -name "*_optimized.csv" -delete

# Clean __pycache__
echo "  Cleaning Python cache..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null

# Clean .pyc files  
find . -name "*.pyc" -delete

# Remove empty directories
echo "  Removing empty directories..."
find datasets -type d -empty -delete 2>/dev/null

echo "✅ Cleanup complete!"
echo ""
echo "Preserved:"
echo "  - All enriched/normalized data"
echo "  - All trained models"
echo "  - All results and plots"
echo "  - All split data"
