"""Generic S3/local data loader for pipeline steps.

Handles S3 prefix-based discovery, local loading, GEOID normalization,
county-level filtering, Census sentinel replacement, and Parquet output.
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import boto3
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def get_s3_client() -> boto3.client:
    """Create a boto3 S3 client."""
    return boto3.client("s3")


def _extract_year_from_key(key: str) -> int | None:
    """Extract a 4-digit year (2000-2099) from an S3 key."""
    matches = re.findall(r"(20\d{2})", key)
    return max(int(m) for m in matches) if matches else None


def _pick_latest_data_key(contents: list[dict]) -> str | None:
    """Pick the latest CSV or Parquet file from S3 listing.

    Prefers the highest vintage year in the filename. If no year is found,
    falls back to the most recent LastModified timestamp.
    Prefers Parquet over CSV when both exist for the same year.
    """
    data_files = [
        obj for obj in contents
        if obj["Key"].endswith(".csv") or obj["Key"].endswith(".parquet")
    ]
    if not data_files:
        return None

    def sort_key(obj: dict) -> tuple:
        key = obj["Key"]
        year = _extract_year_from_key(key)
        is_parquet = 1 if key.endswith(".parquet") else 0
        last_modified = obj.get("LastModified", "")
        return (year or 0, is_parquet, last_modified)

    data_files.sort(key=sort_key, reverse=True)
    return data_files[0]["Key"]


def load_from_s3_prefix(
    bucket: str,
    prefix: str,
) -> pd.DataFrame:
    """Discover and load the latest CSV/Parquet file under an S3 prefix.

    Picks the file with the highest vintage year in its filename.
    Falls back to LastModified if no year is found in filenames.

    Args:
        bucket: S3 bucket name.
        prefix: S3 key prefix to search under.

    Returns:
        DataFrame loaded from the discovered file.

    Raises:
        FileNotFoundError: If no files found under prefix.
    """
    client = get_s3_client()
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])

    if not contents:
        raise FileNotFoundError(
            f"No files found under s3://{bucket}/{prefix}"
        )

    key = _pick_latest_data_key(contents)
    if key is None:
        raise FileNotFoundError(
            f"No CSV or Parquet files found under s3://{bucket}/{prefix}"
        )

    if key.endswith(".parquet"):
        return _load_parquet_from_s3(client, bucket, key)
    return _load_csv_from_s3(client, bucket, key)


def load_from_s3_key(bucket: str, key: str) -> pd.DataFrame:
    """Load a specific file from S3 by key."""
    client = get_s3_client()
    if key.endswith(".parquet"):
        return _load_parquet_from_s3(client, bucket, key)
    return _load_csv_from_s3(client, bucket, key)


def load_from_local(path: str) -> pd.DataFrame:
    """Load a CSV or Parquet file from local disk."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if p.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _load_csv_from_s3(
    client: boto3.client, bucket: str, key: str
) -> pd.DataFrame:
    """Load CSV from S3."""
    logger.info("Loading CSV from s3://%s/%s", bucket, key)
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return pd.read_csv(io.BytesIO(body))


def _load_parquet_from_s3(
    client: boto3.client, bucket: str, key: str
) -> pd.DataFrame:
    """Load Parquet from S3."""
    logger.info("Loading Parquet from s3://%s/%s", bucket, key)
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def normalize_geoid(df: pd.DataFrame, geoid_col: str = "GEOID") -> pd.DataFrame:
    """Normalize GEOID column to 11-character zero-filled strings.

    Strips Census-style prefixes (e.g. '1400000US', '860Z200US') before zero-filling.
    """
    if geoid_col in df.columns:
        col = df[geoid_col].astype(str)
        col = col.str.replace(r"^.*US", "", regex=True)
        df[geoid_col] = col.str.zfill(11)
    return df


def filter_by_county(
    df: pd.DataFrame,
    state_fips: str,
    county_fips: str,
    geoid_col: str = "GEOID",
    allowed_geoids: set[str] | None = None,
    county_fips_set: set[str] | None = None,
) -> pd.DataFrame:
    """Filter DataFrame to rows matching county or MSA.

    Uses three strategies in priority order:
    1. allowed_geoids — exact GEOID match (ZCTAs from geo file)
    2. county_fips_set — 5-char state+county prefix match (MSA multi-county)
    3. state_fips + county_fips — single prefix match (legacy single-county)
    """
    if allowed_geoids is not None:
        mask = df[geoid_col].astype(str).isin(allowed_geoids)
        filtered = df[mask].copy()
        logger.info(
            "Filtered %d -> %d rows using allowed GEOID set (%d IDs)",
            len(df), len(filtered), len(allowed_geoids),
        )
        return filtered

    if county_fips_set is not None:
        mask = df[geoid_col].astype(str).str[:5].isin(county_fips_set)
        filtered = df[mask].copy()
        logger.info(
            "Filtered %d -> %d rows for %d-county MSA",
            len(df), len(filtered), len(county_fips_set),
        )
        return filtered

    prefix = state_fips + county_fips
    mask = df[geoid_col].astype(str).str.startswith(prefix)
    filtered = df[mask].copy()
    logger.info(
        "Filtered %d -> %d rows for county FIPS %s",
        len(df), len(filtered), prefix,
    )
    return filtered


def load_geoid_set_from_geofile(geo_file: str) -> set[str]:
    """Load the set of GEOIDs from a GeoJSON file for filtering.

    Returns GEOIDs in multiple normalized forms to handle format mismatches
    (e.g. ZCTA '37135' vs zero-padded '00000037135').
    """
    path = Path(geo_file)
    if not path.exists():
        logger.warning("Geo file %s not found — skipping GEOID set filter", geo_file)
        return set()
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    geoids = set()
    for feature in data.get("features", []):
        gid = str(feature.get("properties", {}).get("GEOID", ""))
        if gid:
            geoids.add(gid)
            geoids.add(gid.zfill(11))
    logger.info("Loaded %d GEOIDs from %s", len(geoids) // 2, geo_file)
    return geoids


def replace_census_sentinels(df: pd.DataFrame) -> pd.DataFrame:
    """Replace Census sentinel values (negative numbers) with NaN."""
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df.loc[df[col] < 0, col] = np.nan
    return df


def apply_filters(
    df: pd.DataFrame, filters: list[dict[str, Any]]
) -> pd.DataFrame:
    """Apply row-level filters from config."""
    for f in filters:
        col = f["column"]
        val = f["value"]
        if col in df.columns:
            df = df[df[col] == val].copy()
            logger.info("Filtered on %s == %s: %d rows", col, val, len(df))
    return df


def pivot_long_to_wide(
    df: pd.DataFrame,
    var_columns: list[str],
    geoid_col: str = "GEOID",
    measure_col: str = "MeasureId",
    value_col: str = "Data_Value",
) -> pd.DataFrame:
    """Pivot long-format data (one row per measure) to wide format (one row per GEOID).

    CDC PLACES 2025+ uses long format with MeasureId/Data_Value columns.
    Earlier vintages use wide format with measure columns directly.
    """
    if measure_col not in df.columns or value_col not in df.columns:
        return df

    # Filter to only the measures we need
    df_filtered = df[df[measure_col].isin(var_columns)].copy()
    if df_filtered.empty:
        logger.warning("No matching measures found for pivot: %s", var_columns)
        return df

    pivoted = df_filtered.pivot_table(
        index=geoid_col,
        columns=measure_col,
        values=value_col,
        aggfunc="first",
    ).reset_index()

    pivoted.columns.name = None
    logger.info(
        "Pivoted long->wide: %d rows, columns: %s",
        len(pivoted), list(pivoted.columns),
    )
    return pivoted


def upload_file_to_s3(local_path: str, bucket: str, key: str) -> None:
    """Upload a local file to S3. No-op when bucket is empty."""
    if not bucket:
        return
    try:
        get_s3_client().upload_file(str(local_path), bucket, key)
        logger.info("Uploaded %s → s3://%s/%s", local_path, bucket, key)
    except Exception as exc:
        logger.warning("S3 upload failed for %s: %s", local_path, exc)


def save_parquet(df: pd.DataFrame, output_path: str) -> None:
    """Save DataFrame as Parquet file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info("Saved %d rows to %s", len(df), output_path)


def process_data_source(
    source_key: str,
    source_config: dict[str, Any],
    geography: dict[str, Any],
    granularity: str,
    granularity_config: dict[str, Any] | None = None,
) -> pd.DataFrame | None:
    """Process a single data source for a given granularity.

    Loads from S3, normalizes GEOIDs, filters by county, replaces sentinels,
    applies row-level filters, and saves as Parquet.

    Args:
        source_key: Config key (e.g., 'census_acs').
        source_config: Source configuration dict from project.yml.
        geography: Geography configuration dict.
        granularity: 'tract' or 'zip'.
        granularity_config: Granularity dict with geo_file path for ZCTA filtering.

    Returns:
        Processed DataFrame, or None if loading fails.
    """
    bucket = source_config["s3_bucket"]
    prefix = source_config["s3_prefix"].get(granularity)
    if not prefix:
        logger.warning("No prefix for %s/%s", source_key, granularity)
        return None

    geoid_col = source_config.get("geoid_column", "GEOID")
    output_prefix = source_config.get("output_prefix", source_key)

    try:
        df = load_from_s3_prefix(bucket, prefix)
    except FileNotFoundError:
        logger.error("No data found for %s/%s", source_key, granularity)
        return None

    # Rename geoid column to standard GEOID
    if geoid_col != "GEOID" and geoid_col in df.columns:
        df = df.rename(columns={geoid_col: "GEOID"})

    df = normalize_geoid(df)

    # For ZCTA/zip granularity, use geo file GEOIDs instead of FIPS prefix
    allowed_geoids = None
    if granularity == "zip" and granularity_config:
        geo_file = granularity_config.get("geo_file", "")
        if geo_file:
            allowed_geoids = load_geoid_set_from_geofile(geo_file)

    # Build multi-county FIPS set from msa_counties config (or fall back
    # to single county_fips for legacy configs).
    state_fips = geography["state_fips"]
    msa = geography.get("msa_counties")
    if msa:
        county_fips_set = {state_fips + c["fips"] for c in msa}
    else:
        county_fips_set = None

    df = filter_by_county(
        df, state_fips, geography.get("county_fips", ""),
        allowed_geoids=allowed_geoids,
        county_fips_set=county_fips_set,
    )
    df = replace_census_sentinels(df)

    # Apply any row-level filters
    filters = source_config.get("filters", [])
    if filters:
        df = apply_filters(df, filters)

    # Keep only GEOID + variable columns
    var_columns = [v["column"] for v in source_config.get("variables", [])]

    # Detect long-format data (e.g. CDC PLACES 2025+) and pivot to wide
    missing_vars = [c for c in var_columns if c not in df.columns]
    if missing_vars and "MeasureId" in df.columns and "Data_Value" in df.columns:
        logger.info("Detected long-format data — pivoting to wide for: %s", missing_vars)
        df = pivot_long_to_wide(df, var_columns)

    keep_cols = ["GEOID"] + [c for c in var_columns if c in df.columns]
    df = df[keep_cols].copy()

    output_path = f"data/{output_prefix}_{granularity}.parquet"
    save_parquet(df, output_path)

    return df
