#!/usr/bin/env python3
"""Download TIGER/Line tract shapefile, filter to Davidson County, and save as GeoJSON.

STANDALONE script — NO imports from src/ modules.
Dependencies: geopandas, requests, pyogrio
"""
from __future__ import annotations

import os
import sys
import tempfile
import zipfile

import geopandas as gpd
import requests

# Constants defined locally (NOT imported from src/)
TIGER_URL = "https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_47_tract.zip"
DAVIDSON_COUNTY_FP = "037"
OUTPUT_DIR = os.path.join("data", "shapefiles")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "davidson_county_tracts.geojson")
MIN_TRACTS = 150
MAX_TRACTS = 300


def main() -> None:
    """Download, filter, reproject, and save Davidson County tract boundaries."""
    print(f"Downloading TIGER/Line shapefile from {TIGER_URL}...")

    # Download the zip file
    response = requests.get(TIGER_URL, timeout=120)
    response.raise_for_status()

    # Save to temp file and extract
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "tracts.zip")
        with open(zip_path, "wb") as f:
            f.write(response.content)

        print("Extracting shapefile...")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmpdir)

        # Find the .shp file
        shp_files = [f for f in os.listdir(tmpdir) if f.endswith(".shp")]
        if not shp_files:
            print("ERROR: No .shp file found in downloaded archive.", file=sys.stderr)
            sys.exit(1)

        shp_path = os.path.join(tmpdir, shp_files[0])
        print(f"Reading shapefile: {shp_files[0]}")

        # Read with geopandas
        gdf = gpd.read_file(shp_path, engine="pyogrio")

    # Filter to Davidson County (COUNTYFP == "037")
    print(f"Filtering to Davidson County (COUNTYFP={DAVIDSON_COUNTY_FP})...")
    davidson = gdf[gdf["COUNTYFP"] == DAVIDSON_COUNTY_FP].copy()

    tract_count = len(davidson)
    print(f"Found {tract_count} census tracts in Davidson County.")

    assert MIN_TRACTS <= tract_count <= MAX_TRACTS, (
        f"Expected {MIN_TRACTS}-{MAX_TRACTS} tracts, found {tract_count}"
    )

    # Reproject to EPSG:4326
    print("Reprojecting to EPSG:4326...")
    davidson = davidson.to_crs(epsg=4326)

    # Retain only needed columns and zero-pad GEOID to 11 chars
    davidson = davidson[["GEOID", "NAME", "NAMELSAD", "geometry"]].copy()
    davidson["GEOID"] = davidson["GEOID"].astype(str).str.zfill(11)

    # Output
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Writing to {OUTPUT_FILE}...")
    davidson.to_file(OUTPUT_FILE, driver="GeoJSON")

    print(f"Done! {tract_count} tracts written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
