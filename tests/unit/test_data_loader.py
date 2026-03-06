from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture
def sample_geojson_path():
    return str(FIXTURES_DIR / "sample_tracts.geojson")


@pytest.fixture
def sample_partners_path():
    return str(FIXTURES_DIR / "sample_partners.csv")


@pytest.fixture
def sample_census_path():
    return str(FIXTURES_DIR / "sample_census.csv")


@pytest.fixture
def sample_cdc_path():
    return str(FIXTURES_DIR / "sample_cdc_places.csv")


@patch("src.data_loader.st")
def test_load_geojson_normalizes_geoid(mock_st, sample_geojson_path):
    """load_geojson should normalize GEOID values to 11-char zero-padded strings."""
    # Clear any Streamlit cache decorator effects
    mock_st.cache_data = lambda *a, **kw: (lambda f: f)
    mock_st.cache_resource = lambda *a, **kw: (lambda f: f)

    from src.data_loader import load_geojson

    # Call with patched st so decorators are no-ops
    with patch("src.data_loader.load_geojson.__wrapped__", load_geojson):
        result = load_geojson.__wrapped__(sample_geojson_path)

    for feature in result["features"]:
        geoid = feature["properties"]["GEOID"]
        assert len(geoid) == 11, f"GEOID {geoid} is not 11 chars"
        assert geoid == geoid.zfill(11)


@patch("src.data_loader.st")
def test_load_partners_mock_mode(mock_st, sample_partners_path, tmp_path):
    """load_partners in mock mode should return DataFrame with required columns."""
    mock_st.cache_data = lambda *a, **kw: (lambda f: f)
    mock_st.cache_resource = lambda *a, **kw: (lambda f: f)

    # Copy fixture to tmp mock dir
    import shutil
    mock_dir = str(tmp_path / "mock")
    os.makedirs(mock_dir, exist_ok=True)
    shutil.copy(sample_partners_path, os.path.join(mock_dir, "mock_nfp_partners.csv"))

    from src.data_loader import load_partners

    result = load_partners.__wrapped__(use_mock=True, mock_dir=mock_dir)
    assert isinstance(result, pd.DataFrame)
    for col in ["organization_name", "address", "partner_type"]:
        assert col in result.columns, f"Missing required column: {col}"


@patch("src.data_loader.st")
def test_load_census_geoid_normalization(mock_st, sample_census_path, tmp_path):
    """load_census should zero-pad GEOID to 11 characters."""
    mock_st.cache_data = lambda *a, **kw: (lambda f: f)
    mock_st.cache_resource = lambda *a, **kw: (lambda f: f)

    import shutil
    mock_dir = str(tmp_path / "mock")
    os.makedirs(mock_dir, exist_ok=True)
    shutil.copy(sample_census_path, os.path.join(mock_dir, "mock_census_tract_data.csv"))

    from src.data_loader import load_census

    result = load_census.__wrapped__(use_mock=True, mock_dir=mock_dir)
    for geoid in result["GEOID"]:
        assert len(str(geoid)) == 11, f"GEOID {geoid} not zero-padded to 11 chars"


@patch("src.data_loader.st")
def test_data_schema_error_raised(mock_st, tmp_path):
    """DataSchemaError should be raised when required columns are missing."""
    mock_st.cache_data = lambda *a, **kw: (lambda f: f)
    mock_st.cache_resource = lambda *a, **kw: (lambda f: f)

    from src.data_loader import DataSchemaError, load_partners

    mock_dir = str(tmp_path / "mock")
    os.makedirs(mock_dir, exist_ok=True)

    # Write CSV with missing required columns
    bad_csv = os.path.join(mock_dir, "mock_nfp_partners.csv")
    pd.DataFrame({"wrong_col": ["a", "b"]}).to_csv(bad_csv, index=False)

    with pytest.raises(DataSchemaError):
        load_partners.__wrapped__(use_mock=True, mock_dir=mock_dir)


@patch("src.data_loader.st")
def test_load_partners_empty_dataframe(mock_st, tmp_path):
    """load_partners should return an empty DataFrame (not raise) for empty CSV with correct headers."""
    mock_st.cache_data = lambda *a, **kw: (lambda f: f)
    mock_st.cache_resource = lambda *a, **kw: (lambda f: f)

    from src.data_loader import load_partners

    mock_dir = str(tmp_path / "mock")
    os.makedirs(mock_dir, exist_ok=True)

    # Write CSV with correct headers but no data rows
    empty_csv = os.path.join(mock_dir, "mock_nfp_partners.csv")
    pd.DataFrame(columns=["organization_name", "address", "partner_type"]).to_csv(
        empty_csv, index=False
    )

    result = load_partners.__wrapped__(use_mock=True, mock_dir=mock_dir)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0
