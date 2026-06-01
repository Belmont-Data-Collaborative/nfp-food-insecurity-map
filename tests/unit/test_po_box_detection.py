"""Tests for PO Box detection and geocoding skip (spec phase2-data-pipeline-p1, Item 7).

Covers:
- 7.6: regex matches common PO Box formats / rejects street addresses
- 7.1/7.2: geocode_partners skips PO Box records and logs a warning
- 7.3: non-PO-Box records are unaffected
- 7.4: summary log includes PO Box skip count
- 7.5: same detection runs in process_giving_matters (_is_po_box shared logic)
"""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ── 7.6: regex correctness ──────────────────────────────────────────────────

class TestIsPOBox:
    def setup_method(self):
        from pipeline.process_partners import _is_po_box
        self.fn = _is_po_box

    @pytest.mark.parametrize("addr", [
        "P.O. Box 123",
        "PO Box 123",
        "P O Box 456",
        "Post Office Box 789",
        "po box 99",
        "P.O.Box 5",
        "PO BOX 1000",
    ])
    def test_po_box_returns_true(self, addr):
        assert self.fn(addr) is True, f"Expected True for: {addr!r}"

    @pytest.mark.parametrize("addr", [
        "123 Main St",
        "306 Jackson St",
        "333 Welshwood Dr, Nashville, TN",
        "",
        "Apt 4B, 55 Broad St",
    ])
    def test_street_address_returns_false(self, addr):
        assert self.fn(addr) is False, f"Expected False for: {addr!r}"

    def test_giving_matters_version_matches(self):
        """Both modules must use identical logic."""
        from pipeline.process_partners import _is_po_box as partners_fn
        from pipeline.process_giving_matters import _is_po_box as gm_fn
        for addr in ("PO Box 1", "P.O. Box 2", "Post Office Box 3", "123 Main St", ""):
            assert partners_fn(addr) == gm_fn(addr), (
                f"Mismatch for {addr!r}: partners={partners_fn(addr)}, gm={gm_fn(addr)}"
            )


# ── 7.1/7.2/7.3/7.4: geocode_partners behavior ──────────────────────────────

def _make_partners_df(rows):
    return pd.DataFrame(rows, columns=["partner_name", "address", "partner_type"])


def _minimal_partner_config():
    return {
        "s3_bucket": "test-bucket",
        "s3_key": "partners.csv",
        "geocode_cache_key": "cache.csv",
    }


class TestGeocodePOBoxSkip:
    """geocode_partners must skip PO Box records without calling Nominatim."""

    def _run(self, rows, caplog):
        from pipeline.process_partners import geocode_partners

        partners_df = _make_partners_df(rows)
        config = _minimal_partner_config()

        with (
            patch("pipeline.process_partners.load_geocode_cache") as mock_cache,
            patch("pipeline.process_partners.save_geocode_cache"),
            patch("pipeline.process_partners.Nominatim") as mock_nominatim,
        ):
            mock_cache.return_value = pd.DataFrame(
                columns=["address", "latitude", "longitude"]
            )
            mock_geolocator = MagicMock()
            mock_nominatim.return_value = mock_geolocator

            with caplog.at_level(logging.WARNING, logger="pipeline.process_partners"):
                result = geocode_partners(partners_df, config, use_mock=True)

        return result, mock_geolocator

    def test_po_box_record_has_nan_coordinates(self, caplog):
        rows = [{"partner_name": "Acme Food", "address": "PO Box 999", "partner_type": "food_pantry"}]
        result, _ = self._run(rows, caplog)
        row = result.iloc[0]
        assert pd.isna(row["latitude"])
        assert pd.isna(row["longitude"])

    def test_po_box_record_status_is_po_box(self, caplog):
        rows = [{"partner_name": "Acme Food", "address": "PO Box 999", "partner_type": "food_pantry"}]
        result, _ = self._run(rows, caplog)
        assert result.iloc[0]["geocode_status"] == "po_box"

    def test_warning_logged_for_po_box(self, caplog):
        rows = [{"partner_name": "Acme Food", "address": "PO Box 999", "partner_type": "food_pantry"}]
        self._run(rows, caplog)
        assert any("PO Box" in r.message for r in caplog.records), (
            "Expected a warning containing 'PO Box'"
        )

    def test_nominatim_not_called_for_po_box(self, caplog):
        rows = [{"partner_name": "Acme Food", "address": "PO Box 999", "partner_type": "food_pantry"}]
        _, mock_geolocator = self._run(rows, caplog)
        mock_geolocator.geocode.assert_not_called()

    def test_street_address_record_proceeds_to_geocode(self, caplog):
        """Non-PO-Box record should attempt geocoding (7.3)."""
        rows = [{"partner_name": "Main St Pantry", "address": "123 Main St", "partner_type": "food_pantry"}]
        from pipeline.process_partners import geocode_partners

        with (
            patch("pipeline.process_partners.load_geocode_cache") as mock_cache,
            patch("pipeline.process_partners.save_geocode_cache"),
            patch("pipeline.process_partners.Nominatim") as mock_nominatim,
        ):
            mock_cache.return_value = pd.DataFrame(
                columns=["address", "latitude", "longitude"]
            )
            mock_geolocator = MagicMock()
            mock_geolocator.geocode.return_value = None  # avoids coordinate validation
            mock_nominatim.return_value = mock_geolocator

            geocode_partners(
                _make_partners_df(rows), _minimal_partner_config(), use_mock=True
            )

        mock_geolocator.geocode.assert_called_once()

    def test_summary_log_includes_po_box_count(self, caplog):
        """Summary line must include the PO Box skip count (7.4)."""
        rows = [
            {"partner_name": "Real Partner", "address": "456 Oak Ave", "partner_type": "food_pantry"},
            {"partner_name": "PO Partner", "address": "PO Box 1", "partner_type": "food_pantry"},
        ]
        with (
            patch("pipeline.process_partners.load_geocode_cache") as mock_cache,
            patch("pipeline.process_partners.save_geocode_cache"),
            patch("pipeline.process_partners.Nominatim") as mock_nominatim,
        ):
            mock_cache.return_value = pd.DataFrame(
                columns=["address", "latitude", "longitude"]
            )
            mock_geolocator = MagicMock()
            mock_geolocator.geocode.return_value = None
            mock_nominatim.return_value = mock_geolocator

            from pipeline.process_partners import geocode_partners
            with caplog.at_level(logging.INFO, logger="pipeline.process_partners"):
                geocode_partners(
                    _make_partners_df(rows), _minimal_partner_config(), use_mock=True
                )

        summary_records = [r for r in caplog.records if "PO Box" in r.message and "skipped" in r.message]
        assert summary_records, "Expected a summary log line mentioning 'PO Box' and 'skipped'"
        assert "1" in summary_records[0].message
