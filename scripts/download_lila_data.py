"""Download USDA Food Access Research Atlas (LILA) data and upload to S3.

Standalone script — no imports from src/.

Per spec_updates_2.md Section 2.4:
    1. Downloads the Food Access Research Atlas CSV from the USDA ERS website
    2. Validates expected columns are present
    3. Uploads to S3 at nfp-mapping/usda/lila/source/food_access_research_atlas.csv
    4. Logs the source URL and download date for provenance

Usage:
    python scripts/download_lila_data.py

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

# USDA ERS Food Access Research Atlas download URL.
# Data is published as Excel (.xlsx); we convert to CSV before S3 upload.
LILA_URL = (
    "https://www.ers.usda.gov/media/5626/"
    "food-access-research-atlas-data-download-2019.xlsx"
)

EXPECTED_COLUMNS = [
    "CensusTract",
    "LILATracts_1And10",
    "LILATracts_halfAnd10",
    "LILATracts_1And20",
    "lapop1",
    "lalowi1",
    "PovertyRate",
    "MedianFamilyIncome",
]


def load_config() -> dict:
    config_path = os.environ.get("PROJECT_CONFIG", "project.yml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def download_lila_xlsx(url: str) -> bytes:
    print(f"Downloading LILA data from {url}...")
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content):,} bytes")
    return resp.content


def parse_lila_xlsx(xlsx_bytes: bytes) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    print(f"  Sheet names: {xls.sheet_names}")

    chosen = None
    for sheet in xls.sheet_names:
        head = pd.read_excel(xls, sheet_name=sheet, nrows=5)
        if "CensusTract" in head.columns:
            chosen = sheet
            break
    if chosen is None:
        chosen = xls.sheet_names[0]

    print(f"  Reading sheet: {chosen}")
    df = pd.read_excel(xls, sheet_name=chosen)
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


def validate_columns(df: pd.DataFrame) -> None:
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    found = [c for c in EXPECTED_COLUMNS if c in df.columns]
    print(f"  Found {len(found)}/{len(EXPECTED_COLUMNS)} expected columns")
    if missing:
        # Column names may drift between vintages — warn but do not abort.
        print(f"  WARNING: missing expected columns: {missing}", file=sys.stderr)


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
    s3_key = lila_cfg["source_key"]

    xlsx_bytes = download_lila_xlsx(LILA_URL)
    df = parse_lila_xlsx(xlsx_bytes)
    validate_columns(df)
    upload_to_s3(df, bucket, s3_key)

    now = datetime.now(timezone.utc).isoformat()
    print("\nProvenance:")
    print(f"  Source URL:    {LILA_URL}")
    print(f"  Download date: {now}")
    print(f"  S3 location:   s3://{bucket}/{s3_key}")
    print(f"  Rows:          {len(df):,}")
    print(f"  Columns:       {len(df.columns)}")


if __name__ == "__main__":
    main()
