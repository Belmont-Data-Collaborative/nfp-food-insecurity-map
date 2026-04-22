"""Download Census Bureau 2010-to-2020 Tract Relationship File and upload to S3.

Standalone script — no imports from src/.

Per spec_updates_2.md Section 2.4:
    1. Downloads the 2010-to-2020 Census Tract Relationship File from the Census Bureau
    2. Parses the pipe-delimited format
    3. Filters to Tennessee (state FIPS 47) to reduce file size
    4. Uploads to S3 at nfp-mapping/usda/lila/crosswalk/tract_2010_to_2020_tn.csv
    5. Logs the source URL and download date for provenance

Usage:
    python scripts/download_tract_crosswalk.py

Environment variables (loaded from .env if present):
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION (optional, defaults to us-east-1)
"""
from __future__ import annotations

import io
import os
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests
import yaml
from botocore.exceptions import BotoCoreError, ClientError

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Census Bureau 2020-to-2010 Census Tract Relationship File (national, pipe-delimited).
CROSSWALK_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/"
    "tab20_tract20_tract10_natl.txt"
)

REQUIRED_COLUMNS = [
    "GEOID_TRACT_20",
    "GEOID_TRACT_10",
    "AREALAND_PART",
    "AREALAND_TRACT_20",
    "AREALAND_TRACT_10",
]


def load_config() -> dict:
    config_path = os.environ.get("PROJECT_CONFIG", "project.yml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def download_crosswalk(url: str) -> bytes:
    print(f"Downloading Census tract crosswalk from {url}...")
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content):,} bytes")
    return resp.content


def parse_and_filter(raw: bytes, state_fips: str) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(raw), sep="|", low_memory=False)
    print(f"  Loaded {len(df):,} rows (national)")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        print(f"  ERROR: missing expected columns: {missing}", file=sys.stderr)
        print(f"  Available columns: {sorted(df.columns.tolist())}", file=sys.stderr)
        sys.exit(1)

    df["GEOID_TRACT_20"] = df["GEOID_TRACT_20"].astype(str).str.zfill(11)
    df["GEOID_TRACT_10"] = df["GEOID_TRACT_10"].astype(str).str.zfill(11)

    tn_mask = df["GEOID_TRACT_20"].str[:2] == state_fips
    df_tn = df[tn_mask].copy()
    print(f"  Filtered to state {state_fips}: {len(df_tn):,} rows")

    if len(df_tn) == 0:
        print(
            f"ERROR: No rows for state FIPS {state_fips}. Check config.",
            file=sys.stderr,
        )
        sys.exit(1)

    return df_tn


def upload_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get(
        "AWS_SECRET_ACCESS_KEY"
    ):
        print(
            "ERROR: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not set in env.",
            file=sys.stderr,
        )
        sys.exit(1)

    region = os.environ.get("AWS_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name=region)

    csv_buf = io.BytesIO()
    df.to_csv(csv_buf, index=False)
    body = csv_buf.getvalue()

    print(f"Uploading to s3://{bucket}/{key} ({len(body):,} bytes)...")
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="text/csv",
        )
    except (BotoCoreError, ClientError) as exc:
        print(f"ERROR: S3 upload failed: {exc}", file=sys.stderr)
        sys.exit(1)
    print("  Upload complete.")


def main() -> None:
    config = load_config()
    lila_cfg = config["data_sources"]["usda_lila"]
    bucket = lila_cfg["s3_bucket"]
    s3_key = lila_cfg["crosswalk_key"]
    state_fips = config["geography"]["state_fips"]

    raw = download_crosswalk(CROSSWALK_URL)
    df_tn = parse_and_filter(raw, state_fips)
    upload_to_s3(df_tn, bucket, s3_key)

    now = datetime.now(timezone.utc).isoformat()
    print("\nProvenance:")
    print(f"  Source URL:         {CROSSWALK_URL}")
    print(f"  Download date:      {now}")
    print(f"  S3 location:        s3://{bucket}/{s3_key}")
    print(f"  Rows (TN only):     {len(df_tn):,}")
    print(f"  Unique 2020 tracts: {df_tn['GEOID_TRACT_20'].nunique():,}")
    print(f"  Unique 2010 tracts: {df_tn['GEOID_TRACT_10'].nunique():,}")


if __name__ == "__main__":
    main()
