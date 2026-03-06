# Nashville Food Project — Food Insecurity Map

An interactive web map of Davidson County, Tennessee, overlaying Nashville Food Project (NFP) partner locations with census-tract-level poverty, income, and diabetes data. Built for NFP staff and city policymakers to explore food insecurity geography across Nashville.

**Live app:** [Launch on Streamlit Cloud](https://belmont-data-collaborative-nfp-food-insecurity-map-app.streamlit.app)

---

## Features

- **Interactive Folium map** centered on Davidson County with census tract boundaries
- **NFP partner markers** — 30 locations color-coded by partner type (8 categories)
- **Choropleth overlays** — Poverty Rate, Median Household Income, Diabetes Prevalence
- **Click popups** — tract-level data values and partner details on click
- **Category filter legend** — colored swatches with plain-English labels
- **HTML map export** — download the full map as a self-contained HTML file
- **Mock data mode** — runs fully without AWS credentials for demos and development

---

## Quickstart (local)

```bash
# 1. Clone the repo
git clone https://github.com/Belmont-Data-Collaborative/nfp-food-insecurity-map.git
cd nfp-food-insecurity-map

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Copy the env file and enable mock data mode
cp .env.example .env
# .env already has USE_MOCK_DATA=true — no changes needed for local dev

# 5. Run the app
streamlit run app.py
```

The app opens at `http://localhost:8501`.

---

## Data Modes

| Mode | Config | Data Source |
|------|--------|-------------|
| **Mock (default)** | `USE_MOCK_DATA=true` | `data/mock/` — synthetic data, no credentials needed |
| **Production** | `USE_MOCK_DATA=false` | AWS S3 bucket (`BDAIC_BUCKET`) |

For production mode, set `BDAIC_BUCKET` to the S3 bucket name and ensure AWS credentials are available via environment variables or IAM role.

---

## Streamlit Cloud Deployment

1. Fork or connect this repo at [share.streamlit.io](https://share.streamlit.io)
2. Set **Main file**: `app.py`
3. Under **Advanced settings → Secrets**, add:

```toml
USE_MOCK_DATA = "true"
```

4. Click **Deploy** — no other configuration needed for mock/demo mode.

---

## Project Structure

```
app.py                    # Streamlit entry point
src/
  config.py               # Constants, colors, layer definitions
  data_loader.py          # S3 / mock CSV loading with caching
  geocoder.py             # Nominatim geocoding with rate limiting
  layer_manager.py        # Folium layer builders (markers, choropleth)
  map_builder.py          # Assembles the full Folium map
data/
  shapefiles/             # Davidson County census tract GeoJSON
  mock/                   # Synthetic CSV data for development
scripts/
  import_shapefiles.py    # Download and process TIGER/Line shapefiles
  generate_mock_data.py   # Generate reproducible synthetic mock data
tests/                    # Unit and integration tests (pytest)
```

---

## Running Utility Scripts

These scripts are for data preparation and are **not** required to run the app (mock data is pre-generated).

```bash
# Install additional script dependencies
pip install geopandas pyogrio faker numpy

# Download and process Davidson County shapefiles
python scripts/import_shapefiles.py

# Regenerate mock data (deterministic with --seed)
python scripts/generate_mock_data.py --seed 42
```

---

## Running Tests

```bash
pip install pytest pytest-cov geopandas pyogrio faker numpy
USE_MOCK_DATA=true pytest tests/ -v
```

---

## Built With

- [Streamlit](https://streamlit.io) — web app framework
- [Folium](https://python-visualization.github.io/folium/) — interactive maps
- [streamlit-folium](https://folium.streamlit.app) — Folium ↔ Streamlit bridge
- [geopy](https://geopy.readthedocs.io) — Nominatim geocoding
- [branca](https://python-visualization.github.io/branca/) — map legend colormaps
- [pandas](https://pandas.pydata.org) — data loading and manipulation

---

*Built by [BDAIC — Belmont Data Analytics & Intelligence Center](https://www.belmont.edu) for the Nashville Food Project.*
