from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import folium
import pandas as pd
import pytest

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture
def mock_data_dir(tmp_path):
    """Set up a temporary mock data directory with fixture files."""
    mock_dir = tmp_path / "mock"
    mock_dir.mkdir()

    # Copy fixture files with mock naming convention
    shutil.copy(FIXTURES_DIR / "sample_partners.csv", mock_dir / "mock_nfp_partners.csv")
    shutil.copy(FIXTURES_DIR / "sample_census.csv", mock_dir / "mock_census_tract_data.csv")
    shutil.copy(FIXTURES_DIR / "sample_cdc_places.csv", mock_dir / "mock_cdc_places_data.csv")
    shutil.copy(FIXTURES_DIR / "sample_geocode_cache.csv", mock_dir / "mock_geocode_cache.csv")

    return str(mock_dir)


@pytest.fixture
def sample_geojson():
    """Load sample GeoJSON fixture."""
    with open(FIXTURES_DIR / "sample_tracts.geojson") as f:
        return json.load(f)


@patch("src.map_builder.st")
@patch("src.data_loader.st")
@patch("src.geocoder.st")
def test_full_pipeline_mock_mode(
    mock_geocoder_st, mock_dl_st, mock_mb_st, mock_data_dir, sample_geojson
):
    """Full pipeline: load mock data -> geocode -> build map -> verify output."""
    # Disable Streamlit decorators
    for mock_st in (mock_dl_st, mock_geocoder_st, mock_mb_st):
        mock_st.cache_data = lambda *a, **kw: (lambda f: f)
        mock_st.cache_resource = lambda *a, **kw: (lambda f: f)
        mock_st.spinner = MagicMock(
            return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock())
        )

    # Mock session_state for build_map
    mock_mb_st.session_state = {}

    from src.data_loader import load_partners, load_census, load_cdc_places, load_geocode_cache
    from src.geocoder import geocode_partners
    from src.map_builder import build_map

    # Step 1: Load data in mock mode
    partners_df = load_partners.__wrapped__(use_mock=True, mock_dir=mock_data_dir)
    assert isinstance(partners_df, pd.DataFrame)
    assert len(partners_df) > 0

    census_df = load_census.__wrapped__(use_mock=True, mock_dir=mock_data_dir)
    assert isinstance(census_df, pd.DataFrame)

    cdc_df = load_cdc_places.__wrapped__(use_mock=True, mock_dir=mock_data_dir)
    assert isinstance(cdc_df, pd.DataFrame)

    cache_df = load_geocode_cache.__wrapped__(use_mock=True, mock_dir=mock_data_dir)
    assert isinstance(cache_df, pd.DataFrame)

    # Step 2: Geocode (mock the actual geocoder to avoid network calls)
    with patch("src.geocoder.get_geolocator") as mock_geolocator:
        mock_loc = MagicMock()
        mock_loc.latitude = 36.16
        mock_loc.longitude = -86.78
        geolocator_instance = MagicMock()
        geolocator_instance.geocode.return_value = mock_loc
        mock_geolocator.return_value = geolocator_instance

        geocoded_df, updated_cache = geocode_partners(partners_df, cache_df)

    assert isinstance(geocoded_df, pd.DataFrame)
    assert "latitude" in geocoded_df.columns
    assert "longitude" in geocoded_df.columns

    # Step 3: Build map with a choropleth layer
    income_layer = {
        "id": "median_income",
        "display_name": "Median Household Income (Census)",
        "csv_column": "median_household_income",
        "unit": "$",
        "format_string": "${:,.0f}",
        "data_vintage": "American Community Survey 2022",
    }

    # Set choropleth data in mock session_state
    mock_mb_st.session_state["choropleth_data"] = census_df

    m = build_map(
        geojson=sample_geojson,
        partners_df=geocoded_df,
        selected_layer=income_layer,
        show_partners=True,
    )
    assert isinstance(m, folium.Map)

    # Step 4: Verify the map can render to HTML
    html = m.get_root().render()
    assert isinstance(html, str)
    assert len(html) > 100, "Map HTML should be substantial"
    assert "folium" in html.lower() or "leaflet" in html.lower()
