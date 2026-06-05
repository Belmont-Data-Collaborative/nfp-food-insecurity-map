"""S3 reads and local output writes. No transformation logic — that lives in
`predictors` and `compute`. Constants come from `config`.

The public surface is three functions:
    load_acs_profile() -> tract-level ACS predictors (TN, configured YEAR)
    load_fa_county()   -> county-level Feeding America anchor (TN, configured YEAR)
    write_output(df)   -> writes the tract FI output as parquet + CSV locally

AWS credentials are picked up by boto3 from the default chain (typically
~/.aws/credentials populated by `aws configure`). No credentials in code or .env.
"""

from __future__ import annotations

import io as _io
from pathlib import Path

import boto3
import pandas as pd

from . import config

# Standard ACS missing-value markers — convert to NaN at parse time.
# Includes the Census numeric sentinel codes (-666666666 = estimate not available,
# etc.) that appear unnormalized in the legacy `bdaic-public-transform` profile
# CSV; without these, pandas would read them as the literal large negatives and
# they would poison the population-weighted county means in compute.py.
_ACS_NA_MARKERS = [
    "(X)",
    "N",
    "**",
    "***",
    "-",
    "-666666666",
    "-999999999",
    "-888888888",
    "-555555555",
    "-333333333",
    "-222222222",
    "-111111111",
]

# The summary-level prefix on raw GEO_ID values, e.g. "1400000US47037012701".
# Stripped here so downstream sees clean 11-char tract GEOIDs.
_GEOID_PREFIX = "1400000US"


def _s3_get_csv_bytes(key: str) -> bytes:
    """Fetch a CSV from s3://{config.S3_BUCKET}/{key} as raw bytes."""
    client = boto3.client("s3")
    obj = client.get_object(Bucket=config.S3_BUCKET, Key=key)
    return obj["Body"].read()


def load_acs_profile() -> pd.DataFrame:
    """Tract-level ACS predictors from the 2023 Data Profile, TN only.

    Projects to only the 8 DP columns we need (out of ~4,224 in the source file)
    + GEO_ID + state, filters to STATE_FIPS, and strips the GEO_ID prefix so
    every downstream caller sees a clean 11-char `tract_geoid`.

    Returns
    -------
    DataFrame with columns:
        tract_geoid        (str, 11 chars, e.g. "47037012701")
        <DP code columns>  (numeric — percents on 0-100, income in dollars)
    """
    predictor_codes = list(config.PREDICTOR_COLUMNS.values()) + [config.POPULATION_COLUMN]
    usecols = [config.ACS_GEOID_COL, "state", *predictor_codes]

    body = _s3_get_csv_bytes(config.ACS_PROFILE_KEY)
    df = pd.read_csv(
        _io.BytesIO(body),
        usecols=usecols,
        dtype={config.ACS_GEOID_COL: str, "state": str},
        na_values=_ACS_NA_MARKERS,
    )
    df = df[df["state"] == config.STATE_FIPS].copy()
    df["tract_geoid"] = df[config.ACS_GEOID_COL].str.removeprefix(_GEOID_PREFIX)
    df = df.drop(columns=[config.ACS_GEOID_COL, "state"])
    return df.reset_index(drop=True)


def load_fa_county() -> pd.DataFrame:
    """County-level Feeding America anchor, TN tracts only, configured YEAR.

    The FA file is national and covers 2019-2023; we filter to TN counties for
    `config.YEAR`. FIPS values are zero-padded to 5 chars so they always align
    with `tract_geoid[:5]` regardless of state (Alabama's "1001" → "01001").

    Returns
    -------
    DataFrame with columns:
        county_fips  (str, 5 chars, e.g. "47037")
        fi_county    (float, 0-1, the FA "Overall Food Insecurity Rate")
    """
    body = _s3_get_csv_bytes(config.FA_COUNTY_KEY)
    df = pd.read_csv(
        _io.BytesIO(body),
        usecols=[config.FA_FIPS_COL, config.FA_YEAR_COL, config.FA_FI_RATE_COL],
        dtype={config.FA_FIPS_COL: str},
    )
    df[config.FA_FIPS_COL] = df[config.FA_FIPS_COL].str.zfill(5)
    df = df[
        (df[config.FA_YEAR_COL] == config.YEAR)
        & (df[config.FA_FIPS_COL].str.startswith(config.STATE_FIPS))
    ].copy()
    df = df.rename(
        columns={
            config.FA_FIPS_COL: "county_fips",
            config.FA_FI_RATE_COL: "fi_county",
        }
    )
    return df[["county_fips", "fi_county"]].reset_index(drop=True)


def write_output(df: pd.DataFrame) -> tuple[Path, Path]:
    """Write the tract FI output as both Parquet and CSV under OUTPUT_DIR.

    Creates OUTPUT_DIR if it doesn't exist. Returns (parquet_path, csv_path).
    """
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = config.OUTPUT_DIR / f"{config.OUTPUT_BASENAME}.parquet"
    csv_path = config.OUTPUT_DIR / f"{config.OUTPUT_BASENAME}.csv"
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)
    return parquet_path, csv_path
