#!/usr/bin/env python3
"""Generate mock data files for local development.

STANDALONE script — NO imports from src/ modules.
Dependencies: faker, numpy, json, csv, argparse
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys

import numpy as np
from faker import Faker

# Constants defined locally (NOT imported from src/)
GEOJSON_PATH = os.path.join("data", "shapefiles", "davidson_county_tracts.geojson")
OUTPUT_DIR = os.path.join("data", "mock")

PARTNER_COUNT = 30
BLANK_ADDRESS_COUNT = 2

NEIGHBORHOODS = [
    "Antioch",
    "Bordeaux",
    "Donelson",
    "East Nashville",
    "Germantown",
    "Madison",
    "Napier",
    "North Nashville",
    "Rivergate",
    "Sylvan Park",
]

ORG_TYPES = [
    "Center",
    "Alliance",
    "Foundation",
    "Network",
    "Hub",
    "Collective",
    "Initiative",
    "Ministry",
]

PARTNER_TYPES = [
    "school_summer",
    "medical_health",
    "transitional_housing",
    "senior_services",
    "community_development",
    "homeless_outreach",
    "workforce_development",
    "after_school",
]

# Weighted distribution: school_summer and community_development at 20% each
PARTNER_TYPE_WEIGHTS = [0.20, 0.10, 0.10, 0.10, 0.20, 0.10, 0.10, 0.10]

DAVIDSON_COUNTY_ZIP_CODES = [
    "37201", "37203", "37204", "37205", "37206", "37207", "37208",
    "37209", "37210", "37211", "37212", "37213", "37214", "37215",
    "37216", "37217", "37218", "37219", "37220", "37221", "37228",
]

NASHVILLE_STREETS = [
    "Broadway", "West End Ave", "Charlotte Ave", "Gallatin Pike",
    "Nolensville Pike", "Murfreesboro Pike", "Dickerson Pike",
    "Shelby Ave", "Main St", "Church St", "Woodland St",
    "Rosa L Parks Blvd", "Jefferson St", "Clarksville Pike",
    "Briley Pkwy", "Harding Pike", "Lebanon Pike", "Elm Hill Pike",
    "Trinity Ln", "McGavock Pike",
]


def load_geoids() -> list[str]:
    """Load GEOID values from the GeoJSON file."""
    if not os.path.exists(GEOJSON_PATH):
        print(
            f"ERROR: GeoJSON file not found at {GEOJSON_PATH}. "
            f"Run scripts/import_shapefiles.py first.",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        geojson = json.load(f)

    geoids = []
    for feature in geojson.get("features", []):
        geoid = feature.get("properties", {}).get("GEOID")
        if geoid:
            geoids.append(str(geoid).zfill(11))

    if not geoids:
        print("ERROR: No GEOIDs found in GeoJSON.", file=sys.stderr)
        sys.exit(1)

    return geoids


def generate_partners(rng: np.random.Generator, fake: Faker) -> list[dict]:
    """Generate mock partner data."""
    partners = []
    for i in range(PARTNER_COUNT):
        neighborhood = rng.choice(NEIGHBORHOODS)
        org_type = rng.choice(ORG_TYPES)
        partner_type = rng.choice(PARTNER_TYPES, p=PARTNER_TYPE_WEIGHTS)
        org_name = f"{neighborhood} {org_type}"

        # Generate address — leave 2 blank
        if i < BLANK_ADDRESS_COUNT:
            address = ""
        else:
            street_num = rng.integers(100, 9999)
            street = rng.choice(NASHVILLE_STREETS)
            zipcode = rng.choice(DAVIDSON_COUNTY_ZIP_CODES)
            address = f"{street_num} {street}, Nashville, TN {zipcode}"

        partners.append({
            "organization_name": org_name,
            "address": address,
            "partner_type": partner_type,
        })

    return partners


def generate_census(rng: np.random.Generator, geoids: list[str]) -> list[dict]:
    """Generate mock census tract data.

    poverty_rate: 3-45% right-skewed via lognormal
    median_household_income: 22000-120000 negatively correlated with poverty
    3 rows with empty poverty/income values.
    """
    rows = []
    blank_indices = set(rng.choice(len(geoids), size=3, replace=False))

    for i, geoid in enumerate(geoids):
        if i in blank_indices:
            rows.append({
                "GEOID": geoid,
                "poverty_rate": "",
                "median_household_income": "",
            })
        else:
            # Right-skewed poverty rate using lognormal
            raw = rng.lognormal(mean=2.3, sigma=0.6)
            poverty_rate = max(3.0, min(45.0, raw))

            # Income negatively correlated with poverty
            # Higher poverty -> lower income
            poverty_frac = (poverty_rate - 3.0) / 42.0  # 0 to 1
            base_income = 120000 - poverty_frac * 80000
            noise = rng.normal(0, 5000)
            income = max(22000, min(120000, base_income + noise))

            rows.append({
                "GEOID": geoid,
                "poverty_rate": round(poverty_rate, 1),
                "median_household_income": round(income),
            })

    return rows


def generate_cdc_places(rng: np.random.Generator, geoids: list[str], census_rows: list[dict]) -> list[dict]:
    """Generate mock CDC PLACES data.

    DIABETES_CrudePrev: 5-22% with mild positive correlation to poverty.
    """
    # Build a lookup of poverty rates for correlation
    poverty_lookup: dict[str, float] = {}
    for row in census_rows:
        if row["poverty_rate"] != "":
            poverty_lookup[row["GEOID"]] = float(row["poverty_rate"])

    rows = []
    for geoid in geoids:
        poverty = poverty_lookup.get(geoid, 15.0)  # default to middle
        # Mild positive correlation with poverty
        poverty_frac = (poverty - 3.0) / 42.0
        base_diabetes = 5.0 + poverty_frac * 12.0
        noise = rng.normal(0, 2.0)
        diabetes = max(5.0, min(22.0, base_diabetes + noise))

        rows.append({
            "GEOID": geoid,
            "DIABETES_CrudePrev": round(diabetes, 1),
        })

    return rows


def generate_geocode_cache(partners: list[dict], rng: np.random.Generator) -> list[dict]:
    """Generate a mock geocode cache with lat/lon for partners with addresses."""
    cache = []
    for partner in partners:
        if not partner["address"]:
            continue
        # Generate plausible Nashville-area coordinates
        lat = 36.1627 + rng.uniform(-0.08, 0.08)
        lon = -86.7816 + rng.uniform(-0.12, 0.12)
        cache.append({
            "organization_name": partner["organization_name"],
            "address": partner["address"],
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
        })
    return cache


def write_csv(path: str, rows: list[dict], fieldnames: list[str]) -> None:
    """Write a list of dicts to a CSV file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows)} rows to {path}")


def main() -> None:
    """Generate all mock data files."""
    parser = argparse.ArgumentParser(description="Generate mock data for NFP mapping tool")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    fake = Faker()
    Faker.seed(args.seed)

    print("Loading GEOIDs from GeoJSON...")
    geoids = load_geoids()
    print(f"  Found {len(geoids)} GEOIDs")

    print("Generating mock partner data...")
    partners = generate_partners(rng, fake)
    write_csv(
        os.path.join(OUTPUT_DIR, "mock_nfp_partners.csv"),
        partners,
        ["organization_name", "address", "partner_type"],
    )

    print("Generating mock census tract data...")
    census_rows = generate_census(rng, geoids)
    write_csv(
        os.path.join(OUTPUT_DIR, "mock_census_tract_data.csv"),
        census_rows,
        ["GEOID", "poverty_rate", "median_household_income"],
    )

    print("Generating mock CDC PLACES data...")
    cdc_rows = generate_cdc_places(rng, geoids, census_rows)
    write_csv(
        os.path.join(OUTPUT_DIR, "mock_cdc_places_data.csv"),
        cdc_rows,
        ["GEOID", "DIABETES_CrudePrev"],
    )

    print("Generating mock geocode cache...")
    cache_rows = generate_geocode_cache(partners, rng)
    write_csv(
        os.path.join(OUTPUT_DIR, "mock_geocode_cache.csv"),
        cache_rows,
        ["organization_name", "address", "latitude", "longitude"],
    )

    print("Done! All mock data files generated.")


if __name__ == "__main__":
    main()
