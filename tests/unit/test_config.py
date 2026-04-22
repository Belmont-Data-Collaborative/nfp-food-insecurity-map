"""Unit tests for src/config.py and src/config_loader.py."""
from __future__ import annotations

import os

from src import config
from src import config_loader


# ---------------------------------------------------------------------------
# Test: config module
# ---------------------------------------------------------------------------
def test_config_imports():
    assert config is not None


class TestConfigConstants:
    def test_app_env_exists(self):
        assert isinstance(config.APP_ENV, str)

    def test_is_production_flag(self):
        assert isinstance(config.IS_PRODUCTION, bool)

    def test_log_level_set(self):
        assert isinstance(config.LOG_LEVEL, int)

    def test_aws_bucket_name_type(self):
        assert isinstance(config.AWS_BUCKET_NAME, str)

    def test_mock_data_settings(self):
        assert isinstance(config.MOCK_DATA_DIR, str)
        assert isinstance(config.USE_MOCK_DATA, bool)

    def test_error_message_constants(self):
        assert "could not be loaded" in config.ERROR_DATA_LOAD
        assert "{column_name}" in config.ERROR_MISSING_COLUMN
        assert "{count}" in config.WARNING_GEOCODE_FAILURES
        assert "map boundaries" in config.WARNING_NO_GEOID_MATCH


# ---------------------------------------------------------------------------
# Test: config_loader module (YAML)
# ---------------------------------------------------------------------------
class TestConfigLoader:
    def test_get_project(self):
        project = config_loader.get_project()
        assert project["name"] == "NFP Food Insecurity Map"
        assert project["primary_org"] == "Nashville Food Project"
        assert project["secondary_org"] == "BDAIC"

    def test_get_geography(self):
        geo = config_loader.get_geography()
        assert geo["state_fips"] == "47"
        assert "msa_counties" in geo
        assert len(geo["msa_counties"]) == 14
        assert geo["map_center"] == [36.05, -86.60]
        assert geo["default_zoom"] == 9

    def test_get_data_sources(self):
        sources = config_loader.get_data_sources()
        assert "census_acs" in sources
        assert "health_lila" in sources
        assert sources["census_acs"]["s3_bucket"] == "bdaic-public-transform"

    def test_get_all_layer_configs_returns_9_layers(self):
        layers = config_loader.get_all_layer_configs()
        assert len(layers) == 9  # 3 census + 3 health + 3 USDA LILA
        columns = [layer["column"] for layer in layers]
        assert "DP03_0062E" in columns
        assert "DP03_0119PE" in columns
        assert "DP05_0001E" in columns
        assert "DIABETES" in columns
        assert "BPHIGH" in columns
        assert "OBESITY" in columns
        assert "LILATracts_1And10" in columns
        assert "lapop1" in columns
        assert "lalowi1" in columns

    def test_layer_configs_have_required_fields(self):
        required = {"column", "display_name", "colormap", "format_str"}
        for layer in config_loader.get_all_layer_configs():
            missing = required - set(layer.keys())
            assert not missing, f"Layer {layer.get('column')} missing: {missing}"

    def test_get_partner_config(self):
        cfg = config_loader.get_partner_config()
        assert "types" in cfg
        assert len(cfg["types"]) == 10
        for key in ["school_summer", "medical_health", "transitional_housing",
                     "senior_services", "community_development", "homeless_outreach",
                     "workforce_development", "after_school", "community_meals",
                     "other"]:
            assert key in cfg["types"]
            assert "color" in cfg["types"][key]
            assert "label" in cfg["types"][key]
            assert "icon" in cfg["types"][key]

    def test_partner_type_colors_match_spec(self):
        cfg = config_loader.get_partner_config()
        expected = {
            "school_summer": "#E41A1C",
            "medical_health": "#377EB8",
            "transitional_housing": "#4DAF4A",
            "senior_services": "#984EA3",
            "community_development": "#FF7F00",
            "homeless_outreach": "#A65628",
            "workforce_development": "#F781BF",
            "after_school": "#999999",
        }
        for key, color in expected.items():
            assert cfg["types"][key]["color"] == color

    def test_get_map_display(self):
        display = config_loader.get_map_display()
        assert display["tiles"] == "cartodbpositron"
        assert "tile_attribution" in display
        assert display["min_zoom"] == 8
        assert display["max_zoom"] == 16

    def test_get_granularities(self):
        grans = config_loader.get_granularities()
        assert len(grans) == 2
        ids = [g["id"] for g in grans]
        assert "tract" in ids
        assert "zip" in ids
        for g in grans:
            assert "label" in g
            assert "geo_file" in g
