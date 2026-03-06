from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def sample_partners_df():
    """Sample partners DataFrame for geocoding tests."""
    return pd.DataFrame({
        "organization_name": [
            "Antioch Community Org",
            "Bordeaux Medical Clinic",
            "Madison Outreach",
        ],
        "partner_type": [
            "community_development",
            "medical_health",
            "homeless_outreach",
        ],
        "address": [
            "100 Main St Nashville TN 37013",
            "200 Oak Ave Nashville TN 37218",
            "",  # blank address
        ],
    })


@pytest.fixture
def sample_cache_df():
    """Cache with one pre-cached address."""
    return pd.DataFrame({
        "address": ["100 Main St Nashville TN 37013"],
        "latitude": [36.0530],
        "longitude": [-86.6540],
    })


@patch("src.geocoder.st")
@patch("src.geocoder.get_geolocator")
def test_geocode_partners_uses_cache(mock_geolocator, mock_st, sample_partners_df, sample_cache_df):
    """Cached addresses should not be re-geocoded."""
    mock_st.cache_resource = lambda *a, **kw: (lambda f: f)
    mock_st.cache_data = lambda *a, **kw: (lambda f: f)
    mock_st.spinner = MagicMock(return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()))

    mock_loc = MagicMock()
    mock_loc.latitude = 36.21
    mock_loc.longitude = -86.82
    geolocator_instance = MagicMock()
    geolocator_instance.geocode.return_value = mock_loc
    mock_geolocator.return_value = geolocator_instance

    from src.geocoder import geocode_partners

    geocoded_df, updated_cache = geocode_partners(sample_partners_df, sample_cache_df)

    # The cached address (100 Main St) should not trigger a geocode call
    # Only the non-blank, non-cached address (200 Oak Ave) should be geocoded
    assert geolocator_instance.geocode.call_count <= 1


@patch("src.geocoder.st")
@patch("src.geocoder.get_geolocator")
def test_geocode_partners_returns_tuple(mock_geolocator, mock_st, sample_partners_df, sample_cache_df):
    """geocode_partners must return a tuple of (DataFrame, DataFrame)."""
    mock_st.cache_resource = lambda *a, **kw: (lambda f: f)
    mock_st.cache_data = lambda *a, **kw: (lambda f: f)
    mock_st.spinner = MagicMock(return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()))

    geolocator_instance = MagicMock()
    geolocator_instance.geocode.return_value = None
    mock_geolocator.return_value = geolocator_instance

    from src.geocoder import geocode_partners

    result = geocode_partners(sample_partners_df, sample_cache_df)
    assert isinstance(result, tuple), "geocode_partners must return a tuple"
    assert len(result) == 2, "geocode_partners must return a 2-tuple"
    geocoded_df, updated_cache = result
    assert isinstance(geocoded_df, pd.DataFrame)
    assert isinstance(updated_cache, pd.DataFrame)


@patch("src.geocoder.st")
@patch("src.geocoder.get_geolocator")
def test_geocode_partners_skips_blank_addresses(mock_geolocator, mock_st, sample_partners_df, sample_cache_df):
    """Blank addresses should produce NaN lat/lon, no exception raised."""
    mock_st.cache_resource = lambda *a, **kw: (lambda f: f)
    mock_st.cache_data = lambda *a, **kw: (lambda f: f)
    mock_st.spinner = MagicMock(return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()))

    geolocator_instance = MagicMock()
    geolocator_instance.geocode.return_value = None
    mock_geolocator.return_value = geolocator_instance

    from src.geocoder import geocode_partners

    geocoded_df, _ = geocode_partners(sample_partners_df, sample_cache_df)

    # The blank address row (Madison Outreach) should have NaN lat/lon
    blank_row = geocoded_df[geocoded_df["organization_name"] == "Madison Outreach"]
    assert len(blank_row) == 1
    assert pd.isna(blank_row.iloc[0]["latitude"])
    assert pd.isna(blank_row.iloc[0]["longitude"])


@patch("src.geocoder.st")
@patch("src.geocoder.get_geolocator")
def test_geocode_partners_handles_geocoding_failure(mock_geolocator, mock_st, sample_partners_df):
    """When geocoder raises for an address, no exception should propagate."""
    mock_st.cache_resource = lambda *a, **kw: (lambda f: f)
    mock_st.cache_data = lambda *a, **kw: (lambda f: f)
    mock_st.spinner = MagicMock(return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()))

    geolocator_instance = MagicMock()
    geolocator_instance.geocode.side_effect = Exception("Geocoding service unavailable")
    mock_geolocator.return_value = geolocator_instance

    from src.geocoder import geocode_partners

    empty_cache = pd.DataFrame(columns=["address", "latitude", "longitude"])

    # Should NOT raise even though geocoder throws for every address
    geocoded_df, updated_cache = geocode_partners(sample_partners_df, empty_cache)
    assert isinstance(geocoded_df, pd.DataFrame)
    assert "latitude" in geocoded_df.columns
    assert "longitude" in geocoded_df.columns
