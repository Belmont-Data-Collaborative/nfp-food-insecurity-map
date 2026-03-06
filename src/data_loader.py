from __future__ import annotations

import io
import json
import logging
import os

import boto3
import pandas as pd
import streamlit as st

from src import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom Exceptions (defined HERE, not in config.py)
# ---------------------------------------------------------------------------


class DataLoadError(Exception):
    """Raised when data cannot be loaded from S3 or local files."""


class DataSchemaError(Exception):
    """Raised when loaded data is missing required columns."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_columns(df: pd.DataFrame, required: list[str], source: str) -> None:
    """Raise DataSchemaError if any required column is missing."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise DataSchemaError(
            config.ERROR_MISSING_COLUMN.format(col=missing[0])
        )


def _read_csv_from_s3(key: str) -> pd.DataFrame:
    """Download a CSV from S3 and return a DataFrame."""
    try:
        client = get_s3_client()
        obj = client.get_object(Bucket=config.BDAIC_BUCKET, Key=key)
        return pd.read_csv(io.BytesIO(obj["Body"].read()))
    except Exception as exc:
        raise DataLoadError(config.ERROR_DATA_LOAD) from exc


# ---------------------------------------------------------------------------
# Public Functions
# ---------------------------------------------------------------------------


@st.cache_resource
def get_s3_client() -> boto3.client:
    """Return a cached boto3 S3 client."""
    return boto3.client("s3")


@st.cache_data
def load_geojson(path: str) -> dict:
    """Load and return a GeoJSON file as a dict.

    Normalizes GEOID values to 11-char zero-padded strings.
    Raises DataLoadError if file not found or invalid.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            geojson = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise DataLoadError(
            f"Could not load GeoJSON from {path}: {exc}"
        ) from exc

    # Normalize GEOID to 11-char zero-padded strings
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        if "GEOID" in props and props["GEOID"] is not None:
            props["GEOID"] = str(props["GEOID"]).zfill(11)

    return geojson


@st.cache_data
def load_partners(use_mock: bool, mock_dir: str) -> pd.DataFrame:
    """Load NFP partner CSV from S3 or mock directory.

    Validates required columns per config.PARTNERS_REQUIRED_COLUMNS.
    Raises DataLoadError on fetch failure.
    Raises DataSchemaError on missing columns.
    """
    if use_mock:
        path = os.path.join(mock_dir, "mock_nfp_partners.csv")
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            raise DataLoadError(config.ERROR_DATA_LOAD) from exc
    else:
        df = _read_csv_from_s3(config.S3_PARTNERS_KEY)

    _validate_columns(df, config.PARTNERS_REQUIRED_COLUMNS, "partners")
    return df


@st.cache_data
def load_census(use_mock: bool, mock_dir: str) -> pd.DataFrame:
    """Load census tract CSV from S3 or mock directory.

    Zero-pads GEOID to 11 chars immediately after loading.
    Validates required columns per config.CENSUS_REQUIRED_COLUMNS.
    Raises DataLoadError on fetch failure.
    Raises DataSchemaError on missing columns.
    """
    if use_mock:
        path = os.path.join(mock_dir, "mock_census_tract_data.csv")
        try:
            df = pd.read_csv(path, dtype={"GEOID": str})
        except Exception as exc:
            raise DataLoadError(config.ERROR_DATA_LOAD) from exc
    else:
        df = _read_csv_from_s3(config.S3_CENSUS_KEY)

    _validate_columns(df, config.CENSUS_REQUIRED_COLUMNS, "census")

    # Zero-pad GEOID to 11 chars
    df["GEOID"] = df["GEOID"].astype(str).str.zfill(11)
    return df


@st.cache_data
def load_cdc_places(use_mock: bool, mock_dir: str) -> pd.DataFrame:
    """Load CDC PLACES CSV from S3 or mock directory.

    Zero-pads GEOID to 11 chars immediately after loading.
    Validates required columns per config.CDC_REQUIRED_COLUMNS.
    Raises DataLoadError on fetch failure.
    Raises DataSchemaError on missing columns.
    """
    if use_mock:
        path = os.path.join(mock_dir, "mock_cdc_places_data.csv")
        try:
            df = pd.read_csv(path, dtype={"GEOID": str})
        except Exception as exc:
            raise DataLoadError(config.ERROR_DATA_LOAD) from exc
    else:
        df = _read_csv_from_s3(config.S3_CDC_KEY)

    _validate_columns(df, config.CDC_REQUIRED_COLUMNS, "cdc_places")

    # Zero-pad GEOID to 11 chars
    df["GEOID"] = df["GEOID"].astype(str).str.zfill(11)
    return df


@st.cache_data
def load_geocode_cache(use_mock: bool, mock_dir: str) -> pd.DataFrame:
    """Load geocode cache CSV. Returns empty DataFrame if not found.

    Columns: organization_name, address, latitude, longitude.
    """
    empty = pd.DataFrame(
        columns=["organization_name", "address", "latitude", "longitude"]
    )

    if use_mock:
        path = os.path.join(mock_dir, "mock_geocode_cache.csv")
        try:
            df = pd.read_csv(path)
            return df
        except FileNotFoundError:
            return empty
        except Exception:
            return empty
    else:
        try:
            client = get_s3_client()
            obj = client.get_object(
                Bucket=config.BDAIC_BUCKET, Key=config.S3_GEOCODE_CACHE_KEY
            )
            return pd.read_csv(io.BytesIO(obj["Body"].read()))
        except Exception:
            return empty


def save_geocode_cache(df: pd.DataFrame, use_mock: bool, mock_dir: str) -> None:
    """Save geocode cache to S3 or local mock directory.

    Skips S3 write when use_mock is True.
    """
    if use_mock:
        path = os.path.join(mock_dir, "mock_geocode_cache.csv")
        os.makedirs(mock_dir, exist_ok=True)
        df.to_csv(path, index=False)
    else:
        try:
            client = get_s3_client()
            buf = io.BytesIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            client.put_object(
                Bucket=config.BDAIC_BUCKET,
                Key=config.S3_GEOCODE_CACHE_KEY,
                Body=buf.getvalue(),
            )
        except Exception:
            logger.warning("Failed to save geocode cache to S3")
