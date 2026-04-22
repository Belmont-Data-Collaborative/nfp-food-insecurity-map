"""Download and process Census TIGER/Line Davidson County tract shapefile.

STANDALONE script — NO imports from src/ modules.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
import zipfile
from pathlib import Path

try:
    import geopandas as gpd
    import requests
except ImportError as e:
    print(f"Missing dependency: {e}. Install with: pip install geopandas requests")
    sys.exit(1)

TIGER_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_47_tract.zip"
)
DAVIDSON_COUNTY_FIPS = "037"
DEFAULT_OUTPUT = "data/shapefiles/davidson_county_tracts.geojson"
RETAINED_COLUMNS = ["GEOID", "NAME", "NAMELSAD", "geometry"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download and filter Davidson County census tracts."
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help=f"Output GeoJSON file path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    output_path = Path(args.output)

    # Download shapefile
    print(f"Downloading shapefile from {TIGER_URL}...")
    try:
        response = requests.get(TIGER_URL, timeout=120, stream=True)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"ERROR: Failed to download shapefile: {exc}")
        sys.exit(1)

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "tracts.zip")

        # Write downloaded content
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Download complete. Extracting...")

        # Extract
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(tmpdir)
        except zipfile.BadZipFile:
            print("ERROR: Downloaded file is not a valid zip archive.")
            sys.exit(1)

        # Find the .shp file
        shp_files = list(Path(tmpdir).glob("*.shp"))
        if not shp_files:
            print("ERROR: No .shp file found in archive.")
            sys.exit(1)

        print(f"Reading shapefile: {shp_files[0].name}")
        gdf = gpd.read_file(shp_files[0])

    # Filter to Davidson County
    print(f"Filtering to Davidson County (COUNTYFP={DAVIDSON_COUNTY_FIPS})...")
    gdf = gdf[gdf["COUNTYFP"] == DAVIDSON_COUNTY_FIPS].copy()

    if len(gdf) == 0:
        print("ERROR: No tracts found after filtering to Davidson County.")
        sys.exit(1)

    # Validate tract count with flexible range
    tract_count = len(gdf)
    if not (100 <= tract_count <= 400):
        print(
            f"ERROR: Expected 100-400 tracts, got {tract_count}. "
            "Data may be corrupted."
        )
        sys.exit(1)

    print(f"Found {tract_count} census tracts.")

    # Reproject to EPSG:4326
    print("Reprojecting to EPSG:4326...")
    gdf = gdf.to_crs(epsg=4326)

    # Retain only required columns
    gdf = gdf[RETAINED_COLUMNS].copy()

    # Zero-pad GEOID to 11 characters
    gdf["GEOID"] = gdf["GEOID"].astype(str).str.zfill(11)

    # Create output directory
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"ERROR: Failed to create output directory: {exc}")
        sys.exit(1)

    # Write GeoJSON
    print(f"Writing GeoJSON to {output_path}...")
    gdf.to_file(str(output_path), driver="GeoJSON")

    print(f"SUCCESS: Wrote {tract_count} Davidson County census tracts to {output_path}")


if __name__ == "__main__":
    main()
