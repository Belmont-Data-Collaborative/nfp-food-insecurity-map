"""Download and process Census TIGER/Line geographic boundaries.

Downloads tracts, ZCTAs (ZIP codes), and county/MSA boundaries.
Reads geography config from project.yml.

STANDALONE script — NO imports from src/ modules.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
import zipfile
from pathlib import Path

import yaml

try:
    import geopandas as gpd
    import requests
    from shapely.geometry import shape
    from shapely.validation import make_valid
except ImportError as e:
    print(f"Missing dependency: {e}. Install with: pip install geopandas requests shapely")
    sys.exit(1)


OUTPUT_DIR = "data"
RETAINED_COLUMNS = ["GEOID", "NAME", "NAMELSAD", "geometry"]


def _upload_geojson_to_s3(local_path: Path) -> None:
    """Upload a GeoJSON to S3 if S3_OUTPUT_BUCKET env var is set."""
    bucket = os.environ.get("S3_OUTPUT_BUCKET", "")
    prefix = os.environ.get("S3_OUTPUT_PREFIX", "").rstrip("/")
    if not bucket:
        return
    try:
        import boto3
        key = f"{prefix}/{local_path.name}" if prefix else local_path.name
        boto3.client("s3").upload_file(str(local_path), bucket, key)
        print(f"  Uploaded {local_path.name} → s3://{bucket}/{key}")
    except Exception as exc:
        print(f"  WARNING: S3 upload failed for {local_path.name}: {exc}")


def load_geography_config() -> dict:
    """Load geography config from project.yml."""
    config_path = os.environ.get("PROJECT_CONFIG", "project.yml")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["geography"]


def _get_county_fips_list(geo_cfg: dict) -> list[str]:
    """Extract county FIPS codes from geography config.

    Reads from msa_counties list if present (multi-county MSA mode).
    Falls back to legacy single county_fips key for backward compatibility.
    """
    if "msa_counties" in geo_cfg:
        return [c["fips"] for c in geo_cfg["msa_counties"]]
    # Legacy single-county config
    return [geo_cfg["county_fips"]]


def download_and_extract(url: str, tmpdir: str) -> Path:
    """Download a zip file and extract it, returning the directory."""
    print(f"  Downloading {url}...")
    try:
        response = requests.get(url, timeout=120, stream=True)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"ERROR: Failed to download: {exc}")
        sys.exit(1)

    zip_path = os.path.join(tmpdir, "data.zip")
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmpdir)
    except zipfile.BadZipFile:
        print("ERROR: Downloaded file is not a valid zip archive.")
        sys.exit(1)

    return Path(tmpdir)


def find_shapefile(directory: Path) -> Path:
    """Find the .shp file in a directory."""
    shp_files = list(directory.glob("**/*.shp"))
    if not shp_files:
        print("ERROR: No .shp file found in archive.")
        sys.exit(1)
    return shp_files[0]


def fix_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Fix invalid geometries using shapely make_valid."""
    gdf = gdf.copy()
    gdf["geometry"] = gdf["geometry"].apply(
        lambda g: make_valid(g) if g is not None and not g.is_valid else g
    )
    return gdf


def process_tracts(geo_cfg: dict, output_dir: Path) -> gpd.GeoDataFrame:
    """Download and process Census tract boundaries for all MSA counties."""
    state_fips = geo_cfg["state_fips"]
    county_fips_list = _get_county_fips_list(geo_cfg)
    county_fips_set = set(county_fips_list)
    tiger_year = geo_cfg.get("tiger_year", 2023)

    url = (
        f"https://www2.census.gov/geo/tiger/TIGER{tiger_year}"
        f"/TRACT/tl_{tiger_year}_{state_fips}_tract.zip"
    )

    print(f"Processing Census tracts for state {state_fips}, "
          f"{len(county_fips_list)} counties...")

    with tempfile.TemporaryDirectory() as tmpdir:
        extracted = download_and_extract(url, tmpdir)
        shp = find_shapefile(extracted)
        gdf = gpd.read_file(shp)

    # Filter to MSA counties
    gdf = gdf[gdf["COUNTYFP"].isin(county_fips_set)].copy()
    if len(gdf) == 0:
        print(f"ERROR: No tracts found for county FIPS codes {county_fips_list}")
        sys.exit(1)

    tract_count = len(gdf)
    if not (100 <= tract_count <= 700):
        print(f"ERROR: Expected 100-700 tracts, got {tract_count}")
        sys.exit(1)

    print(f"  Found {tract_count} census tracts across {len(county_fips_list)} counties")

    # Reproject to EPSG:4326
    gdf = gdf.to_crs(epsg=4326)

    # Retain columns, zero-pad GEOID
    keep_cols = [c for c in RETAINED_COLUMNS if c in gdf.columns]
    gdf = gdf[keep_cols].copy()
    gdf["GEOID"] = gdf["GEOID"].astype(str).str.zfill(11)

    gdf = fix_geometries(gdf)

    output_path = output_dir / "tracts.geojson"
    gdf.to_file(str(output_path), driver="GeoJSON")
    print(f"  Saved {tract_count} tracts to {output_path}")
    _upload_geojson_to_s3(output_path)

    return gdf


def process_zctas(geo_cfg: dict, output_dir: Path, county_gdf: gpd.GeoDataFrame | None = None) -> None:
    """Download and process ZCTA (ZIP code) boundaries, clipped to county."""
    tiger_year = geo_cfg.get("tiger_year", 2023)

    url = (
        f"https://www2.census.gov/geo/tiger/TIGER{tiger_year}"
        f"/ZCTA520/tl_{tiger_year}_us_zcta520.zip"
    )

    print("Processing ZCTA (ZIP code) boundaries...")

    with tempfile.TemporaryDirectory() as tmpdir:
        extracted = download_and_extract(url, tmpdir)
        shp = find_shapefile(extracted)
        gdf = gpd.read_file(shp)

    gdf = gdf.to_crs(epsg=4326)

    # Clip to county boundary if available
    if county_gdf is not None and not county_gdf.empty:
        county_boundary = county_gdf.geometry.unary_union
        gdf = gpd.clip(gdf, county_boundary)
        gdf = fix_geometries(gdf)

    # Standardize columns
    if "ZCTA5CE20" in gdf.columns:
        gdf = gdf.rename(columns={"ZCTA5CE20": "GEOID"})
    elif "GEOID20" in gdf.columns:
        gdf = gdf.rename(columns={"GEOID20": "GEOID"})

    gdf["GEOID"] = gdf["GEOID"].astype(str)

    # Add NAME and NAMELSAD if missing
    if "NAME" not in gdf.columns:
        gdf["NAME"] = gdf["GEOID"]
    if "NAMELSAD" not in gdf.columns:
        gdf["NAMELSAD"] = "ZCTA " + gdf["GEOID"]

    keep_cols = [c for c in RETAINED_COLUMNS if c in gdf.columns]
    gdf = gdf[keep_cols].copy()

    zcta_count = len(gdf)
    print(f"  Found {zcta_count} ZCTAs in county area")

    output_path = output_dir / "zipcodes.geojson"
    gdf.to_file(str(output_path), driver="GeoJSON")
    print(f"  Saved {zcta_count} ZCTAs to {output_path}")
    _upload_geojson_to_s3(output_path)


def process_county_boundaries(geo_cfg: dict, output_dir: Path) -> gpd.GeoDataFrame:
    """Download and extract county boundary polygons for all MSA counties.

    Saves two files:
      - county_boundaries.geojson: all MSA counties as separate features
      - msa_boundary.geojson: dissolved union of all counties
    Returns the combined GeoDataFrame for ZCTA clipping.
    """
    state_fips = geo_cfg["state_fips"]
    county_fips_list = _get_county_fips_list(geo_cfg)
    county_fips_set = set(county_fips_list)
    tiger_year = geo_cfg.get("tiger_year", 2023)
    msa_name = geo_cfg.get("msa_name", "MSA")

    url = (
        f"https://www2.census.gov/geo/tiger/TIGER{tiger_year}"
        f"/COUNTY/tl_{tiger_year}_us_county.zip"
    )

    print(f"Processing county boundaries for {msa_name} "
          f"({len(county_fips_list)} counties)...")

    with tempfile.TemporaryDirectory() as tmpdir:
        extracted = download_and_extract(url, tmpdir)
        shp = find_shapefile(extracted)
        gdf = gpd.read_file(shp)

    # Filter to MSA counties
    gdf = gdf[
        (gdf["STATEFP"] == state_fips) & (gdf["COUNTYFP"].isin(county_fips_set))
    ].copy()

    if len(gdf) == 0:
        print(f"ERROR: No counties found for state {state_fips}, "
              f"FIPS codes {county_fips_list}")
        sys.exit(1)

    found_count = len(gdf)
    if found_count != len(county_fips_list):
        missing = county_fips_set - set(gdf["COUNTYFP"].unique())
        print(f"WARNING: Expected {len(county_fips_list)} counties, "
              f"found {found_count}. Missing FIPS: {sorted(missing)}")

    gdf = gdf.to_crs(epsg=4326)
    gdf = fix_geometries(gdf)

    # Save individual county boundaries
    counties_path = output_dir / "counties.geojson"
    gdf.to_file(str(counties_path), driver="GeoJSON")
    print(f"  Saved {found_count} county boundaries to {counties_path}")
    _upload_geojson_to_s3(counties_path)

    # Dissolve to single MSA boundary using unary_union (NOT union_all — see G002)
    msa_boundary = gdf.geometry.unary_union
    msa_gdf = gpd.GeoDataFrame(
        {"NAME": [msa_name], "geometry": [msa_boundary]},
        crs=gdf.crs,
    )
    msa_path = output_dir / "msa.geojson"
    msa_gdf.to_file(str(msa_path), driver="GeoJSON")
    print(f"  Saved MSA boundary to {msa_path}")
    _upload_geojson_to_s3(msa_path)

    return gdf


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download and process geographic boundaries for Nashville MSA."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=OUTPUT_DIR,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--skip-zcta",
        action="store_true",
        help="Skip ZCTA (ZIP code) download (large file)",
    )
    args = parser.parse_args()

    geo_cfg = load_geography_config()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Display area name — MSA or legacy single county
    area_name = geo_cfg.get("msa_name")
    if area_name:
        county_fips_list = _get_county_fips_list(geo_cfg)
        print(f"Processing geographic data for {area_name} MSA "
              f"({len(county_fips_list)} counties), {geo_cfg['state_name']}")
    else:
        print(f"Processing geographic data for {geo_cfg['county_name']}, "
              f"{geo_cfg['state_name']}")
    print(f"Output directory: {output_dir}")
    print()

    # 1. County boundaries (individual + dissolved MSA boundary)
    county_gdf = process_county_boundaries(geo_cfg, output_dir)

    # 2. Census tracts
    process_tracts(geo_cfg, output_dir)

    # 3. ZCTAs (optional — large download)
    if not args.skip_zcta:
        process_zctas(geo_cfg, output_dir, county_gdf)
    else:
        print("Skipping ZCTA download (--skip-zcta)")

    print()
    print("SUCCESS: All geographic data processed")


if __name__ == "__main__":
    main()
