# Module Contract — NFP Food Insecurity Map v3

All teammates MUST use the EXACT function names, signatures, constants, and exception classes
defined below. Do NOT rename, reorder parameters, or change return types.

---

## Data Flow

```
app.py (entrypoint)
  ├── load_dotenv()               # BEFORE any src/ imports
  ├── src/config.py               # Constants & secrets (zero internal imports)
  ├── src/data_loader.py          # Load CSVs + GeoJSON from S3 or mock
  │     ├── load_geojson()
  │     ├── load_partners()
  │     ├── load_census()
  │     ├── load_cdc_places()
  │     ├── load_geocode_cache()
  │     └── save_geocode_cache()
  ├── src/geocoder.py             # Geocode partner addresses
  │     ├── get_geolocator()
  │     └── geocode_partners()
  ├── src/layer_manager.py        # Build Folium layers
  │     ├── build_choropleth_layer()
  │     ├── build_partner_markers()
  │     └── build_empty_tract_layer()
  └── src/map_builder.py          # Assemble final map
        ├── build_base_map()
        └── build_map()
```

Flow:
1. `app.py` calls `load_dotenv()`, then imports from `src/`.
2. `app.py.main()` calls `st.set_page_config()` as the FIRST Streamlit call.
3. Load GeoJSON via `data_loader.load_geojson()`.
4. Load partner, census, CDC DataFrames via `data_loader.load_partners()`, etc.
5. Load geocode cache via `data_loader.load_geocode_cache()`.
6. Geocode partners via `geocoder.geocode_partners()` → returns `(geocoded_df, updated_cache)`.
7. Save updated cache via `data_loader.save_geocode_cache()`.
8. Build map via `map_builder.build_map()` which internally uses `layer_manager` functions.
9. Render with `streamlit_folium.st_folium()`.

---

## Module: `src/config.py`

**Purpose**: Source of truth for all constants and configuration. ZERO internal imports
(no imports from other `src/` modules). Does NOT define exception classes.

### Private Functions

```python
def _get_secret(key: str, default: str | None = None) -> str | None:
    """Check os.environ.get(key) FIRST, then fall back to st.secrets.
    MUST check os.environ before st.secrets to avoid StreamlitAPIException at import time."""
```

### Constants — Secrets / Environment

| Name | Type | Value |
|------|------|-------|
| `BDAIC_BUCKET` | `str` | `_get_secret("BDAIC_BUCKET", "")` |
| `USE_MOCK_DATA` | `bool` | `_get_secret("USE_MOCK_DATA", "false").lower() == "true"` |
| `MOCK_DATA_DIR` | `str` | `_get_secret("MOCK_DATA_DIR", "data/mock")` |

### Constants — S3 Paths

| Name | Type | Value |
|------|------|-------|
| `S3_PREFIX` | `str` | `"nfp-mapping/"` |
| `S3_PARTNERS_KEY` | `str` | `S3_PREFIX + "nfp_partners.csv"` |
| `S3_CENSUS_KEY` | `str` | `S3_PREFIX + "census_tract_data.csv"` |
| `S3_CDC_KEY` | `str` | `S3_PREFIX + "cdc_places_data.csv"` |
| `S3_GEOCODE_CACHE_KEY` | `str` | `S3_PREFIX + "nfp_partners_geocoded_cache.csv"` |

### Constants — CSV Column Names

| Name | Type | Value |
|------|------|-------|
| `PARTNERS_REQUIRED_COLUMNS` | `list[str]` | `["organization_name", "address", "partner_type"]` |
| `CENSUS_REQUIRED_COLUMNS` | `list[str]` | `["GEOID", "poverty_rate", "median_household_income"]` |
| `CDC_REQUIRED_COLUMNS` | `list[str]` | `["GEOID", "DIABETES_CrudePrev"]` |

### Constants — Partner Types

| Name | Type | Description |
|------|------|-------------|
| `PARTNER_TYPE_COLORS` | `dict[str, str]` | Maps partner_type key to hex color |
| `PARTNER_TYPE_LABELS` | `dict[str, str]` | Maps partner_type key to plain-English label |

```python
PARTNER_TYPE_COLORS: dict[str, str] = {
    "school_summer": "#E41A1C",
    "medical_health": "#377EB8",
    "transitional_housing": "#4DAF4A",
    "senior_services": "#984EA3",
    "community_development": "#FF7F00",
    "homeless_outreach": "#A65628",
    "workforce_development": "#F781BF",
    "after_school": "#999999",
}

PARTNER_TYPE_LABELS: dict[str, str] = {
    "school_summer": "School / Summer Programs",
    "medical_health": "Medical & Health Services",
    "transitional_housing": "Transitional Housing",
    "senior_services": "Senior Services",
    "community_development": "Community Development",
    "homeless_outreach": "Homeless Outreach",
    "workforce_development": "Workforce Development",
    "after_school": "After-School Programs",
}
```

### Constants — Choropleth Layers

| Name | Type | Description |
|------|------|-------------|
| `CHOROPLETH_LAYERS` | `list[dict]` | List of layer config dicts |

```python
CHOROPLETH_LAYERS: list[dict] = [
    {
        "id": "poverty_rate",
        "display_name": "Poverty Rate (Census)",
        "csv_column": "poverty_rate",
        "unit": "%",
        "format_string": "{:.1f}%",
        "data_vintage": "American Community Survey 2022",
    },
    {
        "id": "median_income",
        "display_name": "Median Household Income (Census)",
        "csv_column": "median_household_income",
        "unit": "$",
        "format_string": "${:,.0f}",
        "data_vintage": "American Community Survey 2022",
    },
    {
        "id": "diabetes",
        "display_name": "Diabetes Prevalence (CDC)",
        "csv_column": "DIABETES_CrudePrev",
        "unit": "%",
        "format_string": "{:.1f}%",
        "data_vintage": "CDC PLACES 2023",
    },
]
```

### Constants — Map Defaults

| Name | Type | Value |
|------|------|-------|
| `DAVIDSON_COUNTY_CENTER` | `tuple[float, float]` | `(36.1627, -86.7816)` |
| `DEFAULT_ZOOM_LEVEL` | `int` | `11` |
| `DEFAULT_CHOROPLETH_LAYER` | `str` | `"median_income"` |
| `CDC_PLACES_INDICATOR_COLUMN` | `str` | `"DIABETES_CrudePrev"` |
| `NOMINATIM_USER_AGENT` | `str` | `"BDAIC-NFP-Mapping-Tool/1.0 (belmont.edu)"` |
| `GEOJSON_PATH` | `str` | `"data/shapefiles/davidson_county_tracts.geojson"` |

### Constants — User-Facing Messages

| Name | Type | Value |
|------|------|-------|
| `ERROR_DATA_LOAD` | `str` | `"Map data could not be loaded. Please refresh the page or contact BDAIC support."` |
| `WARNING_GEOCODE_FAILURES` | `str` | `"{count} partner location(s) could not be mapped due to address lookup errors."` |
| `WARNING_NO_PARTNERS` | `str` | `"No partner locations found in the data file."` |
| `WARNING_NO_CENSUS` | `str` | `"No census data available for Davidson County."` |
| `WARNING_GEOID_MISMATCH` | `str` | `"No data could be matched to map boundaries."` |
| `ERROR_MISSING_COLUMN` | `str` | `"Partner data is missing required column: {col}. Please check the data file."` |

---

## Module: `src/data_loader.py`

**Purpose**: Load data from S3 (production) or local mock files (development).
Defines custom exception classes `DataLoadError` and `DataSchemaError`.

### Exception Classes (defined HERE, not in config.py)

```python
class DataLoadError(Exception):
    """Raised when data cannot be loaded from S3 or local files."""

class DataSchemaError(Exception):
    """Raised when loaded data is missing required columns."""
```

### Public Functions

```python
@st.cache_resource
def get_s3_client() -> boto3.client:
    """Return a cached boto3 S3 client."""

@st.cache_data
def load_geojson(path: str) -> dict:
    """Load and return a GeoJSON file as a dict.
    Normalizes GEOID values to 11-char zero-padded strings.
    Raises DataLoadError if file not found or invalid."""

@st.cache_data
def load_partners(use_mock: bool, mock_dir: str) -> pd.DataFrame:
    """Load NFP partner CSV from S3 or mock directory.
    Validates required columns per config.PARTNERS_REQUIRED_COLUMNS.
    Raises DataLoadError on fetch failure.
    Raises DataSchemaError on missing columns."""

@st.cache_data
def load_census(use_mock: bool, mock_dir: str) -> pd.DataFrame:
    """Load census tract CSV from S3 or mock directory.
    Zero-pads GEOID to 11 chars immediately after loading.
    Validates required columns per config.CENSUS_REQUIRED_COLUMNS.
    Raises DataLoadError on fetch failure.
    Raises DataSchemaError on missing columns."""

@st.cache_data
def load_cdc_places(use_mock: bool, mock_dir: str) -> pd.DataFrame:
    """Load CDC PLACES CSV from S3 or mock directory.
    Zero-pads GEOID to 11 chars immediately after loading.
    Validates required columns per config.CDC_REQUIRED_COLUMNS.
    Raises DataLoadError on fetch failure.
    Raises DataSchemaError on missing columns."""

@st.cache_data
def load_geocode_cache(use_mock: bool, mock_dir: str) -> pd.DataFrame:
    """Load geocode cache CSV. Returns empty DataFrame if not found.
    Columns: organization_name, address, latitude, longitude."""

def save_geocode_cache(df: pd.DataFrame, use_mock: bool, mock_dir: str) -> None:
    """Save geocode cache to S3 or local mock directory.
    Skips S3 write when use_mock is True."""
```

---

## Module: `src/geocoder.py`

**Purpose**: Geocode partner addresses using Nominatim via geopy.
Appends "Davidson County, TN, USA" to each query. Rate limited to 1 req/sec.

### Public Functions

```python
@st.cache_resource
def get_geolocator() -> Nominatim:
    """Return a cached Nominatim geolocator with config.NOMINATIM_USER_AGENT."""

def geocode_partners(
    partners_df: pd.DataFrame,
    cache_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Geocode partner addresses, using cache where available.

    Returns:
        tuple of (geocoded_partners_df, updated_cache_df)
        - geocoded_partners_df: partners_df with 'latitude' and 'longitude' columns added.
          Failed/blank addresses have NaN for lat/lon.
        - updated_cache_df: cache_df with any new geocode results appended.

    IMPORTANT: This function NEVER raises for individual address failures.
    All per-address exceptions are caught silently; NaN is set for failed lookups.
    The caller counts NaN rows to determine how many failed and shows st.warning()."""
```

---

## Module: `src/layer_manager.py`

**Purpose**: Create Folium map layers. All public functions use the `build_` prefix.

### Public Functions

```python
def build_choropleth_layer(
    geojson: dict,
    data_df: pd.DataFrame,
    layer_config: dict,
) -> tuple[folium.GeoJson, branca.colormap.LinearColormap]:
    """Build a choropleth GeoJson layer joined on GEOID and its legend colormap.

    Args:
        geojson: GeoJSON dict with GEOID in feature properties.
        data_df: DataFrame with 'GEOID' column and the column specified in layer_config['csv_column'].
        layer_config: One of config.CHOROPLETH_LAYERS dicts.

    Returns:
        tuple of (geojson_layer, colormap)
        - geojson_layer: folium.GeoJson with style_function and popup.
          Tracts with no data styled neutral gray (#EEEEEE) with popup 'Data not available for this tract.'
          Tracts with data show: 'Census Tract [number]', formatted value, data vintage.
        - colormap: branca.colormap.LinearColormap (YlOrRd) positioned bottom-right."""

def build_partner_markers(partners_df: pd.DataFrame) -> folium.FeatureGroup:
    """Build a FeatureGroup of CircleMarkers for geocoded partners.

    Args:
        partners_df: DataFrame with columns: organization_name, partner_type, latitude, longitude.
            Rows with NaN lat/lon are skipped.

    Returns:
        folium.FeatureGroup containing CircleMarkers colored by partner_type
        per config.PARTNER_TYPE_COLORS. Unrecognized types get #CCCCCC.
        Popup shows: organization name, plain-English type label, 'Nashville Food Project Partner'."""

def build_empty_tract_layer(geojson: dict) -> folium.GeoJson:
    """Build a GeoJson layer showing tract boundaries without choropleth data.

    Returns:
        folium.GeoJson with neutral styling and popup:
        'Census Tract [number] — Select a data layer to see values.'"""
```

---

## Module: `src/map_builder.py`

**Purpose**: Assemble the complete Folium map from layers.

### Public Functions

```python
def build_base_map() -> folium.Map:
    """Create a base Folium Map centered on Davidson County.
    Uses config.DAVIDSON_COUNTY_CENTER and config.DEFAULT_ZOOM_LEVEL."""

def build_map(
    geojson: dict,
    partners_df: pd.DataFrame | None,
    selected_layer: dict | None,
    show_partners: bool,
) -> folium.Map:
    """Assemble the complete map with optional choropleth and partner markers.

    Args:
        geojson: GeoJSON dict for census tract boundaries.
        partners_df: Geocoded partners DataFrame (may be None if loading failed).
        selected_layer: One of config.CHOROPLETH_LAYERS dicts, or None for 'None' selection.
        show_partners: Whether to show partner markers.

    Returns:
        folium.Map ready for rendering via st_folium.

    Behavior:
        - Always starts with build_base_map().
        - If selected_layer is not None: adds choropleth via layer_manager.build_choropleth_layer()
          and adds the colormap legend to the map.
        - If selected_layer is None: adds empty tract boundaries via layer_manager.build_empty_tract_layer().
        - If show_partners is True and partners_df is not None: adds partner markers
          via layer_manager.build_partner_markers()."""
```

---

## Module: `app.py`

**Purpose**: Streamlit entrypoint. Orchestrates loading, geocoding, UI, and rendering.

### Startup Order (CRITICAL)

```python
from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()  # MUST be before any src/ imports

import streamlit as st
import folium  # explicit import for folium type references

from src import config
from src.data_loader import (
    DataLoadError,
    DataSchemaError,
    load_geojson,
    load_partners,
    load_census,
    load_cdc_places,
    load_geocode_cache,
    save_geocode_cache,
)
from src.geocoder import geocode_partners
from src.map_builder import build_map
```

### Main Function Outline

```python
def main() -> None:
    st.set_page_config(...)  # FIRST Streamlit call

    # Sidebar: title, description, controls
    # Load data (wrap in try/except DataLoadError, DataSchemaError)
    # Geocode partners
    # Build map
    # Render with st_folium
    # Export button
```

### Import Sources (CRITICAL for avoiding phantom imports)

| Import | Source Module |
|--------|-------------|
| `DataLoadError` | `src.data_loader` |
| `DataSchemaError` | `src.data_loader` |
| `load_geojson` | `src.data_loader` |
| `load_partners` | `src.data_loader` |
| `load_census` | `src.data_loader` |
| `load_cdc_places` | `src.data_loader` |
| `load_geocode_cache` | `src.data_loader` |
| `save_geocode_cache` | `src.data_loader` |
| `geocode_partners` | `src.geocoder` |
| `build_map` | `src.map_builder` |
| `config` (module) | `src` |
| All constants | via `config.CONSTANT_NAME` |

---

## Scripts (standalone — NO imports from src/)

### `scripts/import_shapefiles.py`

- Downloads TIGER/Line shapefile from Census Bureau.
- Filters to Davidson County (COUNTYFP == "037").
- Reprojects to EPSG:4326.
- Retains GEOID, NAME, NAMELSAD, geometry. Zero-pads GEOID to 11 chars.
- Outputs to `data/shapefiles/davidson_county_tracts.geojson`.
- Asserts 150-300 tracts.
- Dependencies: geopandas, requests, pyogrio.
- MUST NOT import from `src/`.

### `scripts/generate_mock_data.py`

- Reads GEOIDs from `data/shapefiles/davidson_county_tracts.geojson`.
- Generates: `mock_nfp_partners.csv` (30 rows, 2 blank addresses),
  `mock_census_tract_data.csv`, `mock_cdc_places_data.csv`, `mock_geocode_cache.csv`
  in `data/mock/`.
- Partner names: `[Neighborhood] [Type] [Org]` pattern.
- `--seed` flag (default 42).
- Dependencies: faker, numpy, json.
- MUST NOT import from `src/`.

---

## Test Files

| Test File | Tests Functions From |
|-----------|---------------------|
| `tests/unit/test_data_loader.py` | `src.data_loader`: `load_geojson`, `load_partners`, `load_census`, `load_cdc_places`, `load_geocode_cache`, `save_geocode_cache`, `DataLoadError`, `DataSchemaError` |
| `tests/unit/test_geocoder.py` | `src.geocoder`: `geocode_partners`, `get_geolocator` |
| `tests/unit/test_map_builder.py` | `src.map_builder`: `build_base_map`, `build_map` |
| `tests/unit/test_layer_manager.py` | `src.layer_manager`: `build_choropleth_layer`, `build_partner_markers`, `build_empty_tract_layer` |
| `tests/unit/test_scripts.py` | Tests that scripts are standalone (no src/ imports) and produce expected outputs |
| `tests/integration/test_full_pipeline.py` | End-to-end: load mock data → geocode → build map → verify HTML output |

### Test Fixtures (`tests/fixtures/`)

| File | Description |
|------|-------------|
| `sample_partners.csv` | 5 rows: organization_name, address, partner_type |
| `sample_census.csv` | 5 rows: GEOID, poverty_rate, median_household_income |
| `sample_cdc_places.csv` | 5 rows: GEOID, DIABETES_CrudePrev |
| `sample_geocode_cache.csv` | 3 rows: organization_name, address, latitude, longitude |
