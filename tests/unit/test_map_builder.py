from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import folium
import pandas as pd
import pytest

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture
def sample_geojson():
    """Load sample GeoJSON fixture."""
    with open(FIXTURES_DIR / "sample_tracts.geojson") as f:
        return json.load(f)


@pytest.fixture
def sample_partners_df():
    """Sample geocoded partners DataFrame."""
    return pd.DataFrame({
        "organization_name": ["Antioch Community Org"],
        "partner_type": ["community_development"],
        "latitude": [36.053],
        "longitude": [-86.654],
    })


@pytest.fixture
def income_layer_config():
    """Layer config for median income choropleth."""
    return {
        "id": "median_income",
        "display_name": "Median Household Income (Census)",
        "csv_column": "median_household_income",
        "unit": "$",
        "format_string": "${:,.0f}",
        "data_vintage": "American Community Survey 2022",
    }


def test_build_base_map_returns_folium_map():
    """build_base_map should return a folium.Map centered on Davidson County."""
    from src.map_builder import build_base_map

    m = build_base_map()
    assert isinstance(m, folium.Map)

    # Verify center is near Davidson County (36.16, -86.78)
    location = m.location
    assert abs(location[0] - 36.1627) < 0.1, f"Latitude {location[0]} not near Davidson County"
    assert abs(location[1] - (-86.7816)) < 0.1, f"Longitude {location[1]} not near Davidson County"


@patch("src.map_builder.st")
def test_build_map_with_no_data(mock_st, sample_geojson):
    """build_map should work with None partners_df and None selected_layer."""
    mock_st.session_state = {}

    from src.map_builder import build_map

    m = build_map(
        geojson=sample_geojson,
        partners_df=None,
        selected_layer=None,
        show_partners=False,
    )
    assert isinstance(m, folium.Map)


@patch("src.map_builder.st")
def test_build_map_with_partners(mock_st, sample_geojson, sample_partners_df):
    """build_map should work with partner data and no choropleth."""
    mock_st.session_state = {}

    from src.map_builder import build_map

    m = build_map(
        geojson=sample_geojson,
        partners_df=sample_partners_df,
        selected_layer=None,
        show_partners=True,
    )
    assert isinstance(m, folium.Map)
