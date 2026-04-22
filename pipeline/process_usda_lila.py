"""USDA LILA pipeline step — 2010→2020 tract crosswalk conversion.

Loads LILA data (2010 Census tract GEOIDs), applies the Census Bureau
2010-to-2020 tract relationship file to convert values to 2020 tract
GEOIDs, and outputs a Parquet file keyed by 2020 GEOID.

Conversion methods by variable type:
- Binary flags (LILATracts_*): max() — conservative, any contributing 2010
  tract flagged → 2020 tract flagged
- Population counts (lapop1, lalowi1): area-weighted sum, rounded to int
- Rate/percentage (PovertyRate, MedianFamilyIncome): area-weighted mean
"""
from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Any

import boto3
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Variable type classification for crosswalk conversion
FLAG_COLUMNS = {"LILATracts_1And10", "LILATracts_halfAnd10", "LILATracts_1And20"}
COUNT_COLUMNS = {"lapop1", "lalowi1"}
RATE_COLUMNS = {"PovertyRate", "MedianFamilyIncome"}

# Minimum area proportion to keep (filter tiny boundary slivers)
MIN_AREA_PROPORTION = 0.01


def _load_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Load a CSV file from S3."""
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    return pd.read_csv(io.BytesIO(body), low_memory=False)


def _build_county_fips_set(geography: dict) -> set[str]:
    """Build set of 5-char state+county FIPS from geography config."""
    state = geography["state_fips"]
    msa = geography.get("msa_counties")
    if msa:
        return {state + c["fips"] for c in msa}
    return {state + geography.get("county_fips", "")}


def process_usda_lila(
    source_config: dict[str, Any],
    geography: dict[str, Any],
) -> pd.DataFrame | None:
    """Process USDA LILA data with 2010→2020 tract crosswalk.

    Args:
        source_config: usda_lila config from project.yml.
        geography: Geography config with MSA counties.

    Returns:
        DataFrame keyed by 2020 GEOID with LILA variables, or None on error.
    """
    bucket = source_config["s3_bucket"]
    source_key = source_config["source_key"]
    crosswalk_key = source_config["crosswalk_key"]
    geoid_col = source_config.get("source_geoid_column", "CensusTract")
    var_configs = source_config.get("variables", [])
    var_columns = [v["column"] for v in var_configs]

    # 1. Load LILA CSV from S3
    logger.info("Loading LILA data from s3://%s/%s", bucket, source_key)
    try:
        lila_df = _load_csv_from_s3(bucket, source_key)
    except Exception as exc:
        logger.error("Failed to load LILA data: %s", exc)
        return None
    logger.info("Loaded %d LILA rows, %d columns", len(lila_df), len(lila_df.columns))

    # 2. Load crosswalk from S3
    logger.info("Loading crosswalk from s3://%s/%s", bucket, crosswalk_key)
    try:
        xwalk_df = _load_csv_from_s3(bucket, crosswalk_key)
    except Exception as exc:
        logger.error("Failed to load crosswalk: %s", exc)
        return None
    logger.info("Loaded %d crosswalk rows", len(xwalk_df))

    # 3. Normalize GEOIDs
    lila_df[geoid_col] = lila_df[geoid_col].astype(str).str.zfill(11)
    xwalk_df["GEOID_TRACT_10"] = xwalk_df["GEOID_TRACT_10"].astype(str).str.zfill(11)
    xwalk_df["GEOID_TRACT_20"] = xwalk_df["GEOID_TRACT_20"].astype(str).str.zfill(11)

    # 4. Filter crosswalk to MSA counties (using 2020 GEOID prefix)
    county_fips_set = _build_county_fips_set(geography)
    xwalk_df = xwalk_df[xwalk_df["GEOID_TRACT_20"].str[:5].isin(county_fips_set)].copy()
    logger.info("Crosswalk filtered to MSA: %d rows", len(xwalk_df))

    # Filter out tiny intersection slivers
    if "OPP_TRACT_10" in xwalk_df.columns:
        before = len(xwalk_df)
        xwalk_df = xwalk_df[xwalk_df["OPP_TRACT_10"] >= MIN_AREA_PROPORTION].copy()
        logger.info(
            "Filtered slivers (OPP_TRACT_10 < %.2f): %d -> %d rows",
            MIN_AREA_PROPORTION, before, len(xwalk_df),
        )
    else:
        # Compute OPP_TRACT_10 from area fields if not present
        if "AREALAND_PART" in xwalk_df.columns and "AREALAND_TRACT_10" in xwalk_df.columns:
            xwalk_df["OPP_TRACT_10"] = (
                xwalk_df["AREALAND_PART"] / xwalk_df["AREALAND_TRACT_10"].replace(0, np.nan)
            )
            before = len(xwalk_df)
            xwalk_df = xwalk_df[xwalk_df["OPP_TRACT_10"] >= MIN_AREA_PROPORTION].copy()
            logger.info(
                "Computed OPP_TRACT_10 from area fields, filtered slivers: %d -> %d",
                before, len(xwalk_df),
            )
        else:
            logger.warning("No area proportion columns found in crosswalk — skipping sliver filter")
            xwalk_df["OPP_TRACT_10"] = 1.0

    # 5. Join LILA data to crosswalk on 2010 GEOID
    merged = xwalk_df.merge(
        lila_df[[geoid_col] + [c for c in var_columns if c in lila_df.columns]],
        left_on="GEOID_TRACT_10",
        right_on=geoid_col,
        how="left",
    )
    logger.info("Merged LILA + crosswalk: %d rows", len(merged))

    # 6. Apply conversion by variable type
    result_parts = []
    geoid_20_col = "GEOID_TRACT_20"

    for col in var_columns:
        if col not in merged.columns:
            logger.warning("Variable %s not found in LILA data — skipping", col)
            continue

        if col in FLAG_COLUMNS:
            # Binary flags: max() — conservative approach
            part = merged.groupby(geoid_20_col)[col].max().reset_index()
            logger.info("  %s (flag): max() aggregation -> %d tracts", col, len(part))

        elif col in COUNT_COLUMNS:
            # Population counts: area-weighted sum, rounded to int
            merged[f"_{col}_weighted"] = merged[col] * merged["OPP_TRACT_10"]
            part = (
                merged.groupby(geoid_20_col)[f"_{col}_weighted"]
                .sum()
                .round(0)
                .astype("Int64")
                .reset_index()
                .rename(columns={f"_{col}_weighted": col})
            )
            logger.info("  %s (count): weighted sum -> %d tracts", col, len(part))

        elif col in RATE_COLUMNS:
            # Rates: area-weighted mean
            merged[f"_{col}_wt"] = merged[col] * merged["OPP_TRACT_10"]
            numer = merged.groupby(geoid_20_col)[f"_{col}_wt"].sum()
            denom = merged.groupby(geoid_20_col)["OPP_TRACT_10"].sum()
            part = (numer / denom).reset_index().rename(columns={0: col})
            part.columns = [geoid_20_col, col]
            logger.info("  %s (rate): weighted mean -> %d tracts", col, len(part))

        else:
            # Default: treat as count (weighted sum)
            merged[f"_{col}_weighted"] = merged[col] * merged["OPP_TRACT_10"]
            part = (
                merged.groupby(geoid_20_col)[f"_{col}_weighted"]
                .sum()
                .reset_index()
                .rename(columns={f"_{col}_weighted": col})
            )
            logger.info("  %s (default): weighted sum -> %d tracts", col, len(part))

        result_parts.append(part)

    if not result_parts:
        logger.error("No variables processed — aborting")
        return None

    # Combine all variable columns
    result = result_parts[0]
    for part in result_parts[1:]:
        result = result.merge(part, on=geoid_20_col, how="outer")

    result = result.rename(columns={geoid_20_col: "GEOID"})

    # Log 2020 tracts with no LILA data
    all_vars = [c for c in var_columns if c in result.columns]
    no_data = result[result[all_vars].isna().all(axis=1)]
    if len(no_data) > 0:
        logger.info(
            "%d 2020 tracts have no LILA data (NaN) — these had no 2010 match",
            len(no_data),
        )

    # 7. Save as Parquet
    output_prefix = source_config.get("output_prefix", "usda_lila")
    output_path = f"data/{output_prefix}_tract.parquet"
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False)
    logger.info("Saved %d rows to %s", len(result), output_path)

    return result
