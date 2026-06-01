"""Tests for phase2-frontend-p1 Item 6.3: data_year flows into data/config.json.

These are integration-level unit tests — they call export_config() directly
(no S3, no full pipeline run) and inspect the in-memory output.

Also covers:
- All indicators in the exported JSON have non-empty data_year
- data_year values match expected vintages per source
- lila indicators have caption text present in export
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture(scope="module")
def config_json(tmp_path_factory) -> dict:
    """Run export_config() against a temp output directory and return parsed JSON."""
    tmp = tmp_path_factory.mktemp("export_config_test")
    output_path = tmp / "config.json"

    # Patch the output path and disable S3 upload
    with (
        patch("pipeline.export_config.OUTPUT_PATH", output_path),
        patch("pipeline.export_config.app_config.S3_OUTPUT_BUCKET", None),
    ):
        from pipeline.export_config import export_config
        export_config()

    assert output_path.exists(), "export_config() did not write output file"
    return json.loads(output_path.read_text(encoding="utf-8"))


# ── AC 6.3: data_year present in exported JSON ───────────────────────────────

class TestConfigJsonDataYear:
    """AC 6.3: indicators array in config.json must include data_year for every entry."""

    def test_indicators_key_present(self, config_json):
        assert "indicators" in config_json, "config.json missing 'indicators' key"

    def test_every_indicator_has_data_year_key(self, config_json):
        """AC 6.3: every indicator exported to config.json must have a data_year key."""
        missing = [
            ind["id"] for ind in config_json["indicators"]
            if "data_year" not in ind
        ]
        assert not missing, f"Indicators missing data_year key: {missing}"

    def test_every_indicator_has_non_empty_data_year(self, config_json):
        """data_year must not be an empty string for any indicator."""
        empty = [
            ind["id"] for ind in config_json["indicators"]
            if not ind.get("data_year", "").strip()
        ]
        assert not empty, f"Indicators with empty data_year: {empty}"

    @pytest.mark.parametrize("indicator_id,expected_year", [
        ("lila_flag",  "2019"),
        ("lapop1",     "2019"),
        ("lalowi1",    "2019"),
    ])
    def test_usda_lila_indicators_year(self, config_json, indicator_id, expected_year):
        """USDA LILA indicators must export data_year '2019'."""
        ind = next((i for i in config_json["indicators"] if i["id"] == indicator_id), None)
        if ind is None:
            pytest.skip(f"{indicator_id} not in exported indicators")
        assert ind["data_year"] == expected_year, (
            f"{indicator_id}: expected data_year={expected_year!r}, got {ind['data_year']!r}"
        )

    @pytest.mark.parametrize("col", ["DIABETES", "BPHIGH", "OBESITY"])
    def test_cdc_health_indicators_year(self, config_json, col):
        """CDC PLACES health indicators must export data_year '2024'."""
        ind = next((i for i in config_json["indicators"] if i["col"] == col), None)
        if ind is None:
            pytest.skip(f"Column {col} not in exported indicators")
        assert ind["data_year"] == "2024", (
            f"Column {col}: expected data_year='2024', got {ind['data_year']!r}"
        )


# ── Indicator shape: caption and lila src ────────────────────────────────────

class TestConfigJsonIndicatorShape:
    """Verify the exported indicator shape contains all fields map.js expects."""

    def test_all_indicators_have_required_keys(self, config_json):
        required = {"id", "label", "col", "src", "granularities", "palette", "fmt", "caption", "data_year"}
        for ind in config_json["indicators"]:
            missing = required - ind.keys()
            assert not missing, f"Indicator {ind.get('id')} missing keys: {missing}"

    def test_lila_indicators_have_caption_text(self, config_json):
        """lila-src indicators must have non-empty caption (used for ⓘ tooltip)."""
        lila_inds = [i for i in config_json["indicators"] if i.get("src") == "lila"]
        assert lila_inds, "No lila-src indicators found in config.json"
        for ind in lila_inds:
            assert ind.get("caption", "").strip(), (
                f"lila indicator {ind['id']} has empty caption — ⓘ tooltip will be blank"
            )

    def test_lila_flag_granularities_is_tract_only(self, config_json):
        """lila_flag must be tract-only so the geo-toggle auto-switch fires for ZIP."""
        ind = next((i for i in config_json["indicators"] if i["id"] == "lila_flag"), None)
        if ind is None:
            pytest.skip("lila_flag not in exported indicators")
        assert ind["granularities"] == ["tract"], (
            f"lila_flag must be tract-only; got granularities={ind['granularities']}"
        )

    def test_partner_types_key_present(self, config_json):
        """partner_types must be present for the sidebar org list."""
        assert "partner_types" in config_json
        assert len(config_json["partner_types"]) > 0
