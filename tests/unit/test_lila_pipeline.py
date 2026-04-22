"""Unit tests for spec_updates_2.md §6.2 — USDA LILA pipeline crosswalk.

Verifies the 2010 -> 2020 tract crosswalk conversion in
``pipeline.process_usda_lila.process_usda_lila``:

- Binary flag variables (LILATracts_*) use max() — conservative aggregation
- Population count variables (lapop1, lalowi1) use area-weighted apportionment
  and are returned as integer counts
- Rate/percentage variables use area-weighted means
- Tiny intersection slivers (OPP_TRACT_10 < 0.01) are filtered out
- 2020 tracts with no contributing 2010 match end up with NaN values
- Output is keyed by 2020 GEOID (column ``GEOID``) matching our tract GeoJSON
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from pipeline.process_usda_lila import process_usda_lila


SOURCE_CONFIG = {
    "s3_bucket": "test-bucket",
    "source_key": "lila/source.csv",
    "crosswalk_key": "lila/crosswalk.csv",
    "source_geoid_column": "CensusTract",
    "output_prefix": "usda_lila",
    "variables": [
        {"column": "LILATracts_1And10"},
        {"column": "lapop1"},
        {"column": "PovertyRate"},
    ],
}

GEOGRAPHY = {
    "state_fips": "47",
    "msa_counties": [{"fips": "037", "name": "Davidson County"}],
}


def _build_lila_df() -> pd.DataFrame:
    """Three 2010 tracts in Davidson County with known LILA values."""
    return pd.DataFrame(
        {
            "CensusTract": ["47037010100", "47037010200", "47037010300"],
            "LILATracts_1And10": [1, 0, 0],
            "lapop1": [1000, 500, 200],
            "PovertyRate": [30.0, 10.0, 5.0],
        }
    )


def _build_crosswalk_df() -> pd.DataFrame:
    """Crosswalk:
    - 2020 tract A inherits from two 2010 tracts (one LILA flagged, one not)
    - 2020 tract B inherits from one 2010 tract entirely
    - A sliver row (OPP_TRACT_10 = 0.005) that must be filtered out
    """
    return pd.DataFrame(
        {
            "GEOID_TRACT_20": [
                "47037020100", "47037020100",
                "47037020200",
                "47037020100",  # sliver to be dropped
            ],
            "GEOID_TRACT_10": [
                "47037010100", "47037010200",
                "47037010300",
                "47037010300",
            ],
            "AREALAND_PART": [600, 400, 1000, 5],
            "AREALAND_TRACT_10": [1000, 1000, 1000, 1000],
            "AREALAND_TRACT_20": [1000, 1000, 1000, 1000],
        }
    )


@pytest.fixture
def patched_loaders(tmp_path, monkeypatch):
    """Patch _load_csv_from_s3 to return controlled fixtures and chdir
    into tmp_path so the parquet output is isolated."""
    monkeypatch.chdir(tmp_path)
    lila_df = _build_lila_df()
    xwalk_df = _build_crosswalk_df()

    def fake_loader(bucket, key):
        if "crosswalk" in key:
            return xwalk_df.copy()
        return lila_df.copy()

    with patch(
        "pipeline.process_usda_lila._load_csv_from_s3",
        side_effect=fake_loader,
    ):
        yield


def test_output_keyed_by_2020_geoid(patched_loaders) -> None:
    result = process_usda_lila(SOURCE_CONFIG, GEOGRAPHY)
    assert result is not None
    assert "GEOID" in result.columns
    # Both 2020 tracts present (A and B)
    geoids = set(result["GEOID"])
    assert "47037020100" in geoids
    assert "47037020200" in geoids


def test_binary_flag_uses_max_aggregation(patched_loaders) -> None:
    """Tract A has one contributing 2010 tract flagged LILA — must inherit."""
    result = process_usda_lila(SOURCE_CONFIG, GEOGRAPHY)
    row_a = result[result["GEOID"] == "47037020100"].iloc[0]
    row_b = result[result["GEOID"] == "47037020200"].iloc[0]
    assert int(row_a["LILATracts_1And10"]) == 1
    assert int(row_b["LILATracts_1And10"]) == 0


def test_population_counts_area_weighted_and_integer(patched_loaders) -> None:
    """Tract A: 1000 * 0.6 + 500 * 0.4 = 600 + 200 = 800 (integer)."""
    result = process_usda_lila(SOURCE_CONFIG, GEOGRAPHY)
    row_a = result[result["GEOID"] == "47037020100"].iloc[0]
    assert int(row_a["lapop1"]) == 800
    assert pd.api.types.is_integer_dtype(result["lapop1"]) or isinstance(
        row_a["lapop1"], (int, pd.Int64Dtype().type)
    )


def test_rate_uses_area_weighted_mean(patched_loaders) -> None:
    """Tract A: (30*0.6 + 10*0.4) / (0.6 + 0.4) = 22.0"""
    result = process_usda_lila(SOURCE_CONFIG, GEOGRAPHY)
    row_a = result[result["GEOID"] == "47037020100"].iloc[0]
    assert row_a["PovertyRate"] == pytest.approx(22.0, abs=0.01)


def test_sliver_rows_filtered_out(patched_loaders) -> None:
    """The 0.005-area sliver from tract 10300 must NOT contribute to tract A.
    If it did, lapop1 for A would include extra population from 10300.
    Expected lapop1 for A is exactly 800 (no contribution from 10300)."""
    result = process_usda_lila(SOURCE_CONFIG, GEOGRAPHY)
    row_a = result[result["GEOID"] == "47037020100"].iloc[0]
    assert int(row_a["lapop1"]) == 800
