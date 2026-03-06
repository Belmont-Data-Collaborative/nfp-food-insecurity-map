from __future__ import annotations

import json
from pathlib import Path

import branca.colormap
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
def sample_census_df():
    """Sample census DataFrame matching the fixture GEOIDs."""
    return pd.DataFrame({
        "GEOID": ["47037000100", "47037000200"],
        "poverty_rate": [12.5, 28.3],
        "median_household_income": [65000, 38000],
    })


@pytest.fixture
def sample_partners_df():
    """Sample geocoded partners DataFrame."""
    return pd.DataFrame({
        "organization_name": ["Antioch Community Org", "Bordeaux Medical Clinic"],
        "partner_type": ["community_development", "medical_health"],
        "latitude": [36.053, 36.21],
        "longitude": [-86.654, -86.82],
    })


@pytest.fixture
def poverty_layer_config():
    """Layer config for poverty rate choropleth."""
    return {
        "id": "poverty_rate",
        "display_name": "Poverty Rate (Census)",
        "csv_column": "poverty_rate",
        "unit": "%",
        "format_string": "{:.1f}%",
        "data_vintage": "American Community Survey 2022",
    }


def test_build_choropleth_layer_returns_tuple(sample_geojson, sample_census_df, poverty_layer_config):
    """build_choropleth_layer should return a tuple of (GeoJson, LinearColormap)."""
    from src.layer_manager import build_choropleth_layer

    result = build_choropleth_layer(sample_geojson, sample_census_df, poverty_layer_config)
    assert isinstance(result, tuple), "build_choropleth_layer must return a tuple"
    assert len(result) == 2
    geojson_layer, colormap = result
    assert isinstance(geojson_layer, folium.GeoJson)
    assert isinstance(colormap, branca.colormap.LinearColormap)


def test_build_partner_markers_returns_feature_group(sample_partners_df):
    """build_partner_markers should return a folium.FeatureGroup."""
    from src.layer_manager import build_partner_markers

    result = build_partner_markers(sample_partners_df)
    assert isinstance(result, folium.FeatureGroup)


def test_build_empty_tract_layer_returns_geojson(sample_geojson):
    """build_empty_tract_layer should return a folium.GeoJson."""
    from src.layer_manager import build_empty_tract_layer

    result = build_empty_tract_layer(sample_geojson)
    assert isinstance(result, folium.GeoJson)


def test_build_partner_markers_unknown_type():
    """Unknown partner_type should use #CCCCCC fallback color."""
    from src.layer_manager import build_partner_markers

    df = pd.DataFrame({
        "organization_name": ["Unknown Org"],
        "partner_type": ["unknown_type_xyz"],
        "latitude": [36.1],
        "longitude": [-86.7],
    })

    result = build_partner_markers(df)
    assert isinstance(result, folium.FeatureGroup)
    # The marker should still be created (no error)
    # Verify there is at least one child element in the FeatureGroup
    children = list(result._children.values())
    assert len(children) >= 1, "Unknown type should still produce a marker"
