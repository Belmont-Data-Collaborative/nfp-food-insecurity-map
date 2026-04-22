"""Tests for spec_updates_2.md §3.3: Giving Matters pipeline graceful skip.

Verifies:
- enabled=False → returns None immediately, no S3 calls
- S3 NoSuchKey → returns None, logs warning, no crash
- Missing required columns → returns None
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from pipeline.process_giving_matters import process_giving_matters


def _disabled_cfg() -> dict:
    return {
        "enabled": False,
        "s3_bucket": "bdaic-public-transform",
        "s3_key": "nfp-mapping/partners/giving_matters.csv",
    }


def _enabled_cfg() -> dict:
    return {
        "enabled": True,
        "s3_bucket": "bdaic-public-transform",
        "s3_key": "nfp-mapping/partners/giving_matters.csv",
        "geocode_cache_key": "nfp-mapping/partners/giving_matters_geocode_cache.csv",
    }


def test_disabled_returns_none_without_s3_calls() -> None:
    with patch("pipeline.process_giving_matters.boto3") as mock_boto3:
        result = process_giving_matters(_disabled_cfg())
    assert result is None
    mock_boto3.client.assert_not_called()


def test_missing_s3_key_returns_none_gracefully() -> None:
    mock_s3 = MagicMock()
    err = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
    mock_s3.get_object.side_effect = ClientError(err, "GetObject")

    with patch("pipeline.process_giving_matters.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3
        result = process_giving_matters(_enabled_cfg())

    assert result is None
    mock_s3.get_object.assert_called_once()


def test_missing_required_columns_returns_none() -> None:
    """If the loaded CSV does not contain required_columns, exit cleanly."""
    import io

    mock_s3 = MagicMock()
    csv_bytes = b"foo,bar\n1,2\n3,4\n"
    mock_s3.get_object.return_value = {"Body": io.BytesIO(csv_bytes)}

    cfg = _enabled_cfg() | {
        "required_columns": {
            "name_column": "Org Name",
            "address_column": "Address",
        }
    }

    with patch("pipeline.process_giving_matters.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3
        result = process_giving_matters(cfg)

    assert result is None
