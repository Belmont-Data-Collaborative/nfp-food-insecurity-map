"""Tests for the process_food_insecurity pipeline step wiring.

Covers the map-specific logic in process_food_insecurity.py:
  - _build_county_fips_set  (FIPS derivation from geography config)
  - MSA filtering           (only MSA tracts survive)
  - GEOID normalization     (rename geo_id → GEOID, 11-char zero-fill)
  - Multiplier application  (0-1 rate → 0-100 percent)
  - Column selection        (only GEOID + declared variables in output)
  - Parquet write           (data/{prefix}_tract.parquet created)
  - Error handling          (S3 load failure → returns None)

The vendored math (predictors / compute / validate) is covered separately
in test_food_insecurity_compute.py; these tests mock the two S3 loads and
only exercise the wiring in process_food_insecurity.py.
"""

from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import patch

from pipeline.process_food_insecurity import _build_county_fips_set, process_food_insecurity


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# 3 tracts: 2 in county 47001 (in MSA) and 1 in county 47999 (outside MSA).
# All predictor values are equal across tracts so every county deviation = 0
# and FI(t) = fi_county exactly — making the expected output hand-checkable.
_ACS_EQUAL = {
    "tract_geoid": ["47001000001", "47001000002", "47999000001"],
    "DP03_0128PE": [20.0, 20.0, 20.0],
    "DP03_0009PE": [5.0, 5.0, 5.0],
    "DP03_0062E": [50000.0, 50000.0, 50000.0],
    "DP05_0076PE": [5.0, 5.0, 5.0],
    "DP05_0038PE": [20.0, 20.0, 20.0],
    "DP04_0046PE": [60.0, 60.0, 60.0],
    "DP02_0072PE": [10.0, 10.0, 10.0],
    "DP05_0001E": [1000.0, 1000.0, 1000.0],
}


@pytest.fixture
def acs_profile_df() -> pd.DataFrame:
    return pd.DataFrame(_ACS_EQUAL)


@pytest.fixture
def fa_county_df() -> pd.DataFrame:
    return pd.DataFrame({
        "county_fips": ["47001", "47999"],
        "fi_county": [0.15, 0.12],
    })


@pytest.fixture
def msa_geography() -> dict:
    """Single-county MSA (47001 only)."""
    return {
        "state_fips": "47",
        "msa_counties": [{"fips": "001", "name": "Davidson"}],
    }


@pytest.fixture
def source_config() -> dict:
    return {
        "output_prefix": "food_insecurity",
        "variables": [
            {"column": "food_insecurity_rate", "multiplier": 100},
            {"column": "food_insecure_count"},
        ],
    }


@pytest.fixture
def _patched_loaders(acs_profile_df, fa_county_df):
    """Patch both S3 loads so no network access is needed."""
    with (
        patch(
            "pipeline.process_food_insecurity.fsi_io.load_acs_profile",
            return_value=acs_profile_df,
        ),
        patch(
            "pipeline.process_food_insecurity.fsi_io.load_fa_county",
            return_value=fa_county_df,
        ),
    ):
        yield


# ---------------------------------------------------------------------------
# _build_county_fips_set
# ---------------------------------------------------------------------------


def test_build_county_fips_set_multi_county() -> None:
    geo = {"state_fips": "47", "msa_counties": [{"fips": "001"}, {"fips": "037"}]}
    assert _build_county_fips_set(geo) == {"47001", "47037"}


def test_build_county_fips_set_single_county_fallback() -> None:
    geo = {"state_fips": "47", "county_fips": "037"}
    assert _build_county_fips_set(geo) == {"47037"}


# ---------------------------------------------------------------------------
# process_food_insecurity — happy path
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_patched_loaders")
def test_msa_filter_keeps_only_msa_tracts(
    msa_geography, source_config, tmp_path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    result = process_food_insecurity(source_config, msa_geography)
    assert result is not None
    assert len(result) == 2
    assert all(result["GEOID"].str[:5] == "47001")


@pytest.mark.usefixtures("_patched_loaders")
def test_geoid_is_11_chars(
    msa_geography, source_config, tmp_path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    result = process_food_insecurity(source_config, msa_geography)
    assert result is not None
    assert all(result["GEOID"].str.len() == 11)


@pytest.mark.usefixtures("_patched_loaders")
def test_multiplier_scales_rate_to_percent(
    msa_geography, source_config, tmp_path, monkeypatch
) -> None:
    """Equal predictors → deviations = 0 → FI = fi_county × 100 = 15.0."""
    monkeypatch.chdir(tmp_path)
    result = process_food_insecurity(source_config, msa_geography)
    assert result is not None
    assert result["food_insecurity_rate"].between(0, 100).all()
    assert (result["food_insecurity_rate"] - 15.0).abs().max() < 1e-6


@pytest.mark.usefixtures("_patched_loaders")
def test_output_columns_match_declared_variables(
    msa_geography, source_config, tmp_path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    result = process_food_insecurity(source_config, msa_geography)
    assert result is not None
    assert set(result.columns) == {"GEOID", "food_insecurity_rate", "food_insecure_count"}


@pytest.mark.usefixtures("_patched_loaders")
def test_parquet_written_to_data_dir(
    msa_geography, source_config, tmp_path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    process_food_insecurity(source_config, msa_geography)
    output = tmp_path / "data" / "food_insecurity_tract.parquet"
    assert output.exists(), "Parquet file was not written"
    written = pd.read_parquet(output)
    assert "GEOID" in written.columns
    assert len(written) == 2


# ---------------------------------------------------------------------------
# process_food_insecurity — error path
# ---------------------------------------------------------------------------


def test_returns_none_when_s3_load_fails(msa_geography, source_config) -> None:
    with patch(
        "pipeline.process_food_insecurity.fsi_io.load_acs_profile",
        side_effect=Exception("network error"),
    ):
        result = process_food_insecurity(source_config, msa_geography)
    assert result is None
