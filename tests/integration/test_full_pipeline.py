"""Integration test: pipeline outputs match the contract the frontend expects.

Skips when the pipeline hasn't been run in this working tree yet.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest


DATA = Path("data")


def _skip_if_missing(path: Path) -> None:
    if not path.exists():
        pytest.skip(f"{path} not generated yet — run `python -m pipeline` first")


class TestGeoJsonOutputs:
    def test_tracts_geojson_exists(self):
        path = DATA / "tracts.geojson"
        _skip_if_missing(path)
        gj = json.loads(path.read_text())
        assert gj["type"] == "FeatureCollection"
        assert len(gj["features"]) > 100
        for feat in gj["features"][:5]:
            geoid = feat["properties"].get("GEOID", "")
            assert len(str(geoid)) == 11, f"GEOID not 11-padded: {geoid!r}"

    def test_zipcodes_geojson_exists(self):
        path = DATA / "zipcodes.geojson"
        _skip_if_missing(path)
        gj = json.loads(path.read_text())
        assert gj["type"] == "FeatureCollection"
        assert len(gj["features"]) > 0

    def test_counties_and_msa_geojsons(self):
        for name in ("counties.geojson", "msa.geojson"):
            path = DATA / name
            _skip_if_missing(path)
            gj = json.loads(path.read_text())
            assert gj["type"] == "FeatureCollection"
            assert len(gj["features"]) > 0

    def test_partners_geojson(self):
        path = DATA / "partners.geojson"
        _skip_if_missing(path)
        gj = json.loads(path.read_text())
        assert gj["type"] == "FeatureCollection"
        for feat in gj["features"][:5]:
            props = feat["properties"]
            assert "partner_name" in props
            assert "partner_type" in props

    def test_giving_matters_geojson_uses_unified_keys(self):
        path = DATA / "giving_matters.geojson"
        if not path.exists():
            pytest.skip("giving_matters step is disabled or was skipped")
        gj = json.loads(path.read_text())
        assert gj["type"] == "FeatureCollection"
        for feat in gj["features"][:5]:
            props = feat["properties"]
            assert "partner_name" in props, "giving_matters must use partner_name (not name)"
            assert "partner_type" in props, "giving_matters must use partner_type (not category)"


class TestParquetAndCsvContracts:
    REQUIRED_COLUMNS = {
        "acs_tract": {"GEOID", "DP03_0062E", "DP03_0119PE", "DP05_0001E"},
        "acs_zip": {"GEOID", "DP03_0062E", "DP03_0119PE", "DP05_0001E"},
        "health_lila_tract": {"GEOID", "DIABETES", "BPHIGH", "OBESITY"},
        "health_lila_zip": {"GEOID", "DIABETES", "BPHIGH", "OBESITY"},
        "usda_lila_tract": {"GEOID", "LILATracts_1And10", "lapop1", "lalowi1"},
    }

    CSV_RENAMES = {"health_lila_tract": "health_tract", "health_lila_zip": "health_zip"}

    @pytest.mark.parametrize("stem", list(REQUIRED_COLUMNS.keys()))
    def test_parquet_has_required_columns(self, stem: str):
        path = DATA / f"{stem}.parquet"
        _skip_if_missing(path)
        df = pd.read_parquet(path)
        missing = self.REQUIRED_COLUMNS[stem] - set(df.columns)
        assert not missing, f"{path} missing: {missing}"

    @pytest.mark.parametrize("stem", list(REQUIRED_COLUMNS.keys()))
    def test_csv_mirrors_parquet(self, stem: str):
        csv_stem = self.CSV_RENAMES.get(stem, stem)
        path = DATA / f"{csv_stem}.csv"
        _skip_if_missing(path)
        df = pd.read_csv(path, dtype={"GEOID": str})
        missing = self.REQUIRED_COLUMNS[stem] - set(df.columns)
        assert not missing, f"{path} missing: {missing}"


class TestConfigJson:
    def test_config_json_shape(self):
        path = DATA / "config.json"
        _skip_if_missing(path)
        cfg = json.loads(path.read_text())
        for key in ("project", "geography", "indicators", "partner_types", "palettes"):
            assert key in cfg, f"config.json missing top-level key: {key}"
        assert isinstance(cfg["indicators"], list) and cfg["indicators"]
        for ind in cfg["indicators"]:
            for field in ("id", "label", "col", "src", "granularities", "palette", "fmt"):
                assert field in ind, f"indicator missing {field}: {ind}"
