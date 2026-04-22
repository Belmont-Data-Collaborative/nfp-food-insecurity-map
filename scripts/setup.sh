#!/usr/bin/env bash
set -euo pipefail
echo "=== NFP Food Insecurity Map — One-Command Setup ==="

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Import shapefiles (downloads from Census TIGER/Line)
echo "Downloading and processing Davidson County shapefiles..."
python scripts/import_shapefiles.py

# Generate mock data
echo "Generating mock data..."
python scripts/generate_mock_data.py

echo "=== Setup complete! Run: streamlit run app.py ==="
