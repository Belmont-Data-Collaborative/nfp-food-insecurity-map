"""Unit tests for spec_updates_2.md §6.2 — MSA geography.

Verifies that all 14 Nashville MSA county FIPS codes are present in
project.yml, that get_county_fips_set() returns the correct set, that
pipeline filtering accepts a county_fips_set parameter, and that the
map_display config carries the values needed for MSA-wide rendering.
County-boundary layer rendering is tested in test_county_boundaries.py.
"""
from __future__ import annotations

from unittest.mock import MagicMock
import sys

import pandas as pd
import pytest

# boto3 is imported transitively via pipeline.load_source but not needed here
sys.modules.setdefault("boto3", MagicMock())

from src.config_loader import (
    get_county_fips_set,
    get_geography,
)


# ---------------------------------------------------------------------------
# Test: MSA config helpers in config_loader
# ---------------------------------------------------------------------------
class TestMSAConfigHelpers:
    """get_county_fips_set() must return the full set of state+county FIPS
    prefixes from the msa_counties config."""

    def test_get_county_fips_set_returns_14_counties(self):
        fips_set = get_county_fips_set()
        assert len(fips_set) == 14, (
            f"Expected 14 MSA county FIPS, got {len(fips_set)}: {fips_set}"
        )

    def test_get_county_fips_set_includes_davidson(self):
        fips_set = get_county_fips_set()
        assert "47037" in fips_set, "Davidson County (47037) must be in MSA set"

    def test_get_county_fips_set_includes_all_msa_counties(self):
        expected = {
            "47015", "47021", "47037", "47043", "47081",
            "47111", "47119", "47147", "47149", "47159",
            "47165", "47169", "47187", "47189",
        }
        fips_set = get_county_fips_set()
        assert fips_set == expected

    def test_get_county_fips_set_values_are_5_chars(self):
        for fips in get_county_fips_set():
            assert len(fips) == 5, f"FIPS {fips} must be 5 chars (state + county)"

    def test_geography_has_msa_counties(self):
        geo = get_geography()
        assert "msa_counties" in geo, "project.yml must have msa_counties list"
        assert len(geo["msa_counties"]) == 14

    def test_geography_msa_map_center(self):
        geo = get_geography()
        assert geo["map_center"] == [36.05, -86.60]

    def test_geography_msa_default_zoom(self):
        geo = get_geography()
        assert geo["default_zoom"] == 9


# ---------------------------------------------------------------------------
# Test: multi-county pipeline filtering
# ---------------------------------------------------------------------------
class TestMultiCountyFiltering:
    """filter_by_county must support a county_fips_set parameter for
    filtering GEOIDs from multiple counties."""

    @pytest.fixture
    def multi_county_df(self):
        """DataFrame with GEOIDs from 3 different counties."""
        return pd.DataFrame({
            "GEOID": [
                "47037001700",  # Davidson
                "47037001800",  # Davidson
                "47149060100",  # Rutherford
                "47187040100",  # Williamson
                "47001000100",  # NOT in MSA (Anderson County)
            ],
            "value": [1, 2, 3, 4, 5],
        })

    def test_filter_by_county_fips_set(self, multi_county_df):
        from pipeline.load_source import filter_by_county

        msa_set = {"47037", "47149", "47187"}
        result = filter_by_county(
            multi_county_df,
            state_fips="47",
            county_fips="037",
            county_fips_set=msa_set,
        )
        assert len(result) == 4, "Should keep Davidson + Rutherford + Williamson rows"
        assert "47001000100" not in result["GEOID"].values

    def test_filter_by_county_fips_set_empty(self, multi_county_df):
        from pipeline.load_source import filter_by_county

        result = filter_by_county(
            multi_county_df,
            state_fips="47",
            county_fips="037",
            county_fips_set={"47999"},  # No match
        )
        assert len(result) == 0

    def test_filter_by_county_legacy_fallback(self, multi_county_df):
        """When county_fips_set is None, fall back to single-county prefix."""
        from pipeline.load_source import filter_by_county

        result = filter_by_county(
            multi_county_df,
            state_fips="47",
            county_fips="037",
            county_fips_set=None,
        )
        assert len(result) == 2, "Legacy mode: only Davidson County rows"


# ---------------------------------------------------------------------------
# Test: map_display config
# ---------------------------------------------------------------------------
class TestMapDisplayConfig:
    def test_simplify_tolerance_in_config(self):
        from src.config_loader import get_map_display

        display = get_map_display()
        assert "simplify_tolerance" in display
        assert display["simplify_tolerance"] == 0.001

    def test_min_zoom_for_msa(self):
        from src.config_loader import get_map_display

        display = get_map_display()
        assert display["min_zoom"] <= 9, (
            "min_zoom must be <= 9 to accommodate MSA-wide view"
        )
