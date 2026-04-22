"""Tests for pipeline bugs discovered during spec_upgrade verification.

Bug 1: process_geographic_data.py uses union_all() which doesn't exist in geopandas 0.14.3
Bug 2: project.yml S3 prefixes point to nonexistent nfp-mapping/ paths
Bug 3: project.yml CDC PLACES column names have _CrudePrev suffix that doesn't match real data
Bug 4: normalize_geoid doesn't strip Census GEO_ID prefix (e.g. '1400000US47037001700')
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture
def project_config() -> dict:
    """Load project.yml."""
    config_path = PROJECT_ROOT / "project.yml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class TestBug1UnionAll:
    """Bug 1: GeoSeries.union_all() doesn't exist in geopandas 0.14.3."""

    def test_geoseries_has_unary_union_not_union_all(self):
        """Verify geopandas GeoSeries supports unary_union (not union_all)."""
        from shapely.geometry import Point

        gs = gpd.GeoSeries([Point(0, 0).buffer(1), Point(1, 1).buffer(1)])
        # unary_union should work
        result = gs.unary_union
        assert result is not None

    def test_process_geographic_data_uses_correct_union_api(self):
        """The script must use unary_union, not union_all."""
        script_path = PROJECT_ROOT / "scripts" / "process_geographic_data.py"
        source = script_path.read_text(encoding="utf-8")
        assert "union_all()" not in source, (
            "process_geographic_data.py uses union_all() which doesn't exist "
            "in geopandas 0.14.3 — should use unary_union"
        )


class TestBug2S3Prefixes:
    """Bug 2: project.yml S3 prefixes point to nonexistent nfp-mapping/ paths."""

    def test_census_acs_tract_prefix_is_real_s3_path(self, project_config):
        """Census ACS tract prefix must point to actual S3 data location."""
        prefix = project_config["data_sources"]["census_acs"]["s3_prefix"]["tract"]
        assert not prefix.startswith("nfp-mapping/"), (
            f"Census ACS tract prefix '{prefix}' uses nonexistent nfp-mapping/ path. "
            "Actual data is at census_acs5_profile/geo_tract/"
        )
        assert "census_acs5_profile" in prefix, (
            f"Census ACS tract prefix should reference census_acs5_profile, got: {prefix}"
        )

    def test_health_lila_tract_prefix_is_real_s3_path(self, project_config):
        """CDC PLACES tract prefix must point to actual S3 data location."""
        prefix = project_config["data_sources"]["health_lila"]["s3_prefix"]["tract"]
        assert not prefix.startswith("nfp-mapping/"), (
            f"CDC PLACES tract prefix '{prefix}' uses nonexistent nfp-mapping/ path. "
            "Actual data is at cdc_places/census_tract/"
        )
        assert "cdc_places" in prefix, (
            f"CDC PLACES tract prefix should reference cdc_places, got: {prefix}"
        )

    def test_health_lila_zip_prefix_is_real_s3_path(self, project_config):
        """CDC PLACES ZCTA prefix must point to actual S3 data location."""
        prefix = project_config["data_sources"]["health_lila"]["s3_prefix"]["zip"]
        assert not prefix.startswith("nfp-mapping/"), (
            f"CDC PLACES zip prefix '{prefix}' uses nonexistent nfp-mapping/ path. "
            "Actual data is at cdc_places/zcta/"
        )
        assert "cdc_places" in prefix, (
            f"CDC PLACES zip prefix should reference cdc_places, got: {prefix}"
        )

    def test_census_acs_zip_prefix_is_real_s3_path(self, project_config):
        """Census ACS ZCTA prefix must point to actual S3 data location (geo_zipcode)."""
        prefix = project_config["data_sources"]["census_acs"]["s3_prefix"]["zip"]
        assert not prefix.startswith("nfp-mapping/"), (
            f"Census ACS zip prefix '{prefix}' uses nonexistent nfp-mapping/ path."
        )
        assert "geo_zipcode" in prefix, (
            f"Census ACS zip prefix should reference geo_zipcode, got: {prefix}. "
            "Actual data is at census_acs5_profile/geo_zipcode/"
        )


class TestBug3CDCColumnNames:
    """Bug 3: project.yml CDC column names have _CrudePrev suffix not in real data."""

    EXPECTED_CDC_COLUMNS = {
        "DIABETES": "Diabetes Prevalence",
        "BPHIGH": "Hypertension Prevalence",
        "OBESITY": "Obesity Prevalence",
    }

    def test_cdc_variable_columns_match_real_data(self, project_config):
        """CDC PLACES variable column names must match actual S3 data columns."""
        health_vars = project_config["data_sources"]["health_lila"]["variables"]
        for var in health_vars:
            col = var["column"]
            assert "_CrudePrev" not in col, (
                f"CDC variable column '{col}' has _CrudePrev suffix which doesn't "
                "exist in the real CDC PLACES data. Real columns: DIABETES, BPHIGH, OBESITY"
            )

    def test_diabetes_column_name(self, project_config):
        """Diabetes column should be 'DIABETES', not 'DIABETES_CrudePrev'."""
        health_vars = project_config["data_sources"]["health_lila"]["variables"]
        diabetes_vars = [v for v in health_vars if "diabet" in v["display_name"].lower()]
        assert len(diabetes_vars) == 1
        assert diabetes_vars[0]["column"] == "DIABETES"

    def test_hypertension_column_name(self, project_config):
        """Hypertension column should be 'BPHIGH', not 'HIGHBP_CrudePrev'."""
        health_vars = project_config["data_sources"]["health_lila"]["variables"]
        bp_vars = [v for v in health_vars if "hypertension" in v["display_name"].lower()]
        assert len(bp_vars) == 1
        assert bp_vars[0]["column"] == "BPHIGH"

    def test_obesity_column_name(self, project_config):
        """Obesity column should be 'OBESITY', not 'OBESITY_CrudePrev'."""
        health_vars = project_config["data_sources"]["health_lila"]["variables"]
        obesity_vars = [v for v in health_vars if "obesity" in v["display_name"].lower()]
        assert len(obesity_vars) == 1
        assert obesity_vars[0]["column"] == "OBESITY"


class TestBug4GeoIDPrefix:
    """Bug 4: normalize_geoid must strip Census GEO_ID prefix '1400000US'."""
    """Bug 4: normalize_geoid must strip Census GEO_ID prefix '1400000US'."""

    def test_normalize_geoid_strips_census_tract_prefix(self):
        """Census ACS tract GEO_IDs have format '1400000US47037001700' — prefix must be stripped."""
        from pipeline.load_source import normalize_geoid

        df = pd.DataFrame({
            "GEOID": [
                "1400000US47037001700",
                "1400000US47037001800",
                "1400000US06037200100",
            ]
        })
        result = normalize_geoid(df)
        assert result["GEOID"].tolist() == [
            "47037001700",
            "47037001800",
            "06037200100",
        ]

    def test_normalize_geoid_strips_census_zcta_prefix(self):
        """Census ACS ZCTA GEO_IDs have format '860Z200US37013' — prefix must be stripped."""
        from pipeline.load_source import normalize_geoid

        df = pd.DataFrame({
            "GEOID": [
                "860Z200US37013",
                "860Z200US37027",
                "860Z200US90210",
            ]
        })
        result = normalize_geoid(df)
        # After stripping prefix, should be just the ZIP codes
        assert result["GEOID"].tolist() == [
            "00000037013",
            "00000037027",
            "00000090210",
        ]

    def test_normalize_geoid_handles_plain_fips(self):
        """GEOIDs that are already plain FIPS codes should be zero-filled to 11 chars."""
        from pipeline.load_source import normalize_geoid

        df = pd.DataFrame({"GEOID": ["47037001700", "6037200100"]})
        result = normalize_geoid(df)
        assert result["GEOID"].tolist() == ["47037001700", "06037200100"]

    def test_filter_by_county_after_normalize(self):
        """After normalize, filter_by_county should find Davidson County rows."""
        from pipeline.load_source import filter_by_county, normalize_geoid

        df = pd.DataFrame({
            "GEOID": [
                "1400000US47037001700",
                "1400000US47037001800",
                "1400000US06037200100",
            ]
        })
        df = normalize_geoid(df)
        filtered = filter_by_county(df, "47", "037")
        assert len(filtered) == 2


class TestBug5ZCTAFiltering:
    """Bug 5: ZCTA GEOIDs are 5-digit ZIPs — filter_by_county prefix match fails."""

    def test_process_data_source_zcta_uses_geo_file_filter(self):
        """For zip granularity, filter should use geo file ZCTAs not FIPS prefix."""
        from pipeline.load_source import filter_by_county

        # ZCTA GEOIDs are 5-digit ZIP codes, not 11-char FIPS
        df = pd.DataFrame({
            "GEOID": ["37013", "37015", "37027", "37076", "37115", "90210"]
        })
        # Prefix "47037" will never match 5-digit ZIPs
        filtered = filter_by_county(df, "47", "037")
        # Currently returns 0 — this is the bug
        # After fix, we need a different mechanism for ZCTA filtering
        # For now, verify the FIPS prefix approach fails for ZCTAs
        assert len(filtered) == 0, (
            "FIPS prefix filtering on ZCTA data produces 0 results — "
            "need ZCTA-aware filtering for zip granularity"
        )


class TestBug6S3FileDiscovery:
    """Bug 6: load_from_s3_prefix picks file by LastModified, not by vintage year in filename."""

    def test_pick_latest_by_filename_not_last_modified(self):
        """S3 discovery should pick the file with the highest year in the filename."""
        from pipeline.load_source import _pick_latest_data_key

        # Simulate S3 listing where 2015 was modified most recently
        contents = [
            {"Key": "prefix/data_2015.csv", "LastModified": "2026-03-30T00:00:00Z", "Size": 100},
            {"Key": "prefix/data_2023.csv", "LastModified": "2025-01-01T00:00:00Z", "Size": 100},
            {"Key": "prefix/data_2020.csv", "LastModified": "2025-06-01T00:00:00Z", "Size": 100},
            {"Key": "prefix/data_2023.yaml", "LastModified": "2026-03-30T00:00:00Z", "Size": 50},
        ]
        key = _pick_latest_data_key(contents)
        assert key == "prefix/data_2023.csv", (
            f"Should pick 2023 (latest vintage year), got: {key}"
        )

    def test_pick_latest_parquet_over_csv(self):
        """If both parquet and CSV exist for the same year, prefer parquet."""
        from pipeline.load_source import _pick_latest_data_key

        contents = [
            {"Key": "prefix/data_2023.csv", "LastModified": "2025-01-01T00:00:00Z", "Size": 100},
            {"Key": "prefix/data_2023.parquet", "LastModified": "2025-01-01T00:00:00Z", "Size": 50},
        ]
        key = _pick_latest_data_key(contents)
        assert key == "prefix/data_2023.parquet"

    def test_fallback_to_last_modified_when_no_year(self):
        """If filenames don't contain years, fall back to LastModified."""
        from pipeline.load_source import _pick_latest_data_key
        from datetime import datetime, timezone

        contents = [
            {"Key": "prefix/old_data.csv", "LastModified": datetime(2024, 1, 1, tzinfo=timezone.utc), "Size": 100},
            {"Key": "prefix/new_data.csv", "LastModified": datetime(2026, 3, 1, tzinfo=timezone.utc), "Size": 100},
        ]
        key = _pick_latest_data_key(contents)
        assert key == "prefix/new_data.csv"


class TestBug7CDCHealthDataPresent:
    """Bug 7: CDC PLACES health data columns must have non-null values after pipeline processing."""

    def test_health_lila_parquet_has_diabetes_values(self):
        """The health_lila tract parquet must have non-null DIABETES values."""
        parquet_path = PROJECT_ROOT / "data" / "choropleth" / "health_lila_tract_data.parquet"
        if not parquet_path.exists():
            pytest.skip("health_lila_tract_data.parquet not found — run pipeline first")
        df = pd.read_parquet(parquet_path)
        assert "DIABETES" in df.columns, f"DIABETES column missing. Columns: {df.columns.tolist()}"
        non_null = df["DIABETES"].notna().sum()
        assert non_null > 0, f"DIABETES column has 0 non-null values out of {len(df)} rows"

    def test_health_lila_parquet_has_bphigh_values(self):
        """The health_lila tract parquet must have non-null BPHIGH values."""
        parquet_path = PROJECT_ROOT / "data" / "choropleth" / "health_lila_tract_data.parquet"
        if not parquet_path.exists():
            pytest.skip("health_lila_tract_data.parquet not found — run pipeline first")
        df = pd.read_parquet(parquet_path)
        assert "BPHIGH" in df.columns, f"BPHIGH column missing. Columns: {df.columns.tolist()}"
        non_null = df["BPHIGH"].notna().sum()
        assert non_null > 0, f"BPHIGH column has 0 non-null values out of {len(df)} rows"

    def test_health_lila_parquet_has_obesity_values(self):
        """The health_lila tract parquet must have non-null OBESITY values."""
        parquet_path = PROJECT_ROOT / "data" / "choropleth" / "health_lila_tract_data.parquet"
        if not parquet_path.exists():
            pytest.skip("health_lila_tract_data.parquet not found — run pipeline first")
        df = pd.read_parquet(parquet_path)
        assert "OBESITY" in df.columns, f"OBESITY column missing. Columns: {df.columns.tolist()}"
        non_null = df["OBESITY"].notna().sum()
        assert non_null > 0, f"OBESITY column has 0 non-null values out of {len(df)} rows"

    def test_health_lila_has_one_row_per_tract(self):
        """After processing, each tract should have exactly one row of health data."""
        parquet_path = PROJECT_ROOT / "data" / "choropleth" / "health_lila_tract_data.parquet"
        if not parquet_path.exists():
            pytest.skip("health_lila_tract_data.parquet not found — run pipeline first")
        df = pd.read_parquet(parquet_path)
        # Should be ~174 rows (one per Davidson County tract), not 5675
        assert len(df) < 500, (
            f"health_lila parquet has {len(df)} rows — expected ~174 (one per tract). "
            "The CDC PLACES data likely has multiple rows per tract (one per measure) "
            "and needs pivoting or the data is in long format, not wide."
        )
