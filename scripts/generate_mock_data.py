"""Generate mock CSV data for local development.

STANDALONE script — NO imports from src/ modules.
Must be run AFTER import_shapefiles.py so that the GeoJSON file exists.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Use MSA-wide tracts produced by scripts/process_geographic_data.py
# (post spec_updates_2 Phase 1). Falls back to legacy Davidson-only file
# if the MSA file isn't present.
GEOJSON_PATH = "data/tracts.geojson"
LEGACY_GEOJSON_PATH = "data/shapefiles/davidson_county_tracts.geojson"
DEFAULT_OUTPUT_DIR = "data/mock/"
DEFAULT_SEED = 42

# Real Davidson County neighborhood prefixes
NEIGHBORHOOD_PREFIXES = [
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

# Real Nashville street names
STREET_NAMES = [
    "Broadway",
    "West End Ave",
    "Charlotte Ave",
    "Gallatin Pike",
    "Nolensville Pike",
    "Murfreesboro Pike",
    "Dickerson Pike",
    "Shelby Ave",
    "Main St",
    "Church St",
    "Commerce St",
    "Demonbreun St",
    "Rosa L Parks Blvd",
    "Jefferson St",
    "Buchanan St",
    "Trinity Ln",
    "Clarksville Pike",
    "Briley Pkwy",
    "Ellington Pkwy",
    "Lebanon Pike",
]

# Davidson County zip codes
ZIP_CODES = [
    "37201", "37203", "37204", "37205", "37206", "37207", "37208",
    "37209", "37210", "37211", "37212", "37213", "37214", "37215",
    "37216", "37217", "37218", "37219", "37220", "37221", "37228",
]

# Partner types with exact suffix names
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

# Organization name suffixes by type
ORG_SUFFIXES = {
    "school_summer": [
        "Elementary Summer Program",
        "Youth Summer Camp",
        "Summer Meals Initiative",
    ],
    "medical_health": [
        "Health Clinic",
        "Community Health Center",
        "Medical Outreach",
    ],
    "transitional_housing": [
        "Transitional Housing",
        "Family Shelter",
        "Housing Support",
    ],
    "senior_services": [
        "Senior Center",
        "Elder Care Services",
        "Senior Nutrition Program",
    ],
    "community_development": [
        "Community Center",
        "Neighborhood Alliance",
        "Development Corp",
    ],
    "homeless_outreach": [
        "Outreach Center",
        "Homeless Services",
        "Street Outreach",
    ],
    "workforce_development": [
        "Job Training Center",
        "Workforce Solutions",
        "Career Services",
    ],
    "after_school": [
        "After School Program",
        "Youth Development",
        "Kids Club",
    ],
}


def load_geoids(geojson_path: str) -> list[str]:
    """Load GEOIDs from the GeoJSON file."""
    path = Path(geojson_path)
    if not path.exists():
        print(f"ERROR: GeoJSON file not found: {geojson_path}")
        print("Run import_shapefiles.py first.")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    geoids = []
    for feature in data.get("features", []):
        geoid = feature.get("properties", {}).get("GEOID", "")
        if geoid:
            geoids.append(str(geoid).zfill(11))
    return geoids


def generate_partners(rng: np.random.Generator, n: int = 30) -> pd.DataFrame:
    """Generate mock_nfp_partners.csv with 30 rows, 2 blank addresses."""
    # Weighted partner types:
    # school_summer and community_development at 20% each (6 each)
    # remaining 6 types split evenly: 18/6 = 3 each
    types_list: list[str] = []
    types_list.extend(["school_summer"] * 6)
    types_list.extend(["community_development"] * 6)
    for pt in PARTNER_TYPES:
        if pt not in ("school_summer", "community_development"):
            types_list.extend([pt] * 3)
    # Shuffle with our rng
    rng.shuffle(types_list)

    rows: list[dict] = []
    for i in range(n):
        ptype = types_list[i]
        prefix = NEIGHBORHOOD_PREFIXES[i % len(NEIGHBORHOOD_PREFIXES)]
        suffix_options = ORG_SUFFIXES[ptype]
        suffix = suffix_options[i % len(suffix_options)]
        name = f"{prefix} {suffix}"

        street_num = rng.integers(100, 9999)
        street = STREET_NAMES[i % len(STREET_NAMES)]
        zipcode = ZIP_CODES[i % len(ZIP_CODES)]
        address = f"{street_num} {street}, Nashville, TN {zipcode}"

        rows.append(
            {
                "partner_name": name,
                "address": address,
                "partner_type": ptype,
            }
        )

    # Make exactly 2 addresses blank (indices 5 and 17)
    rows[5]["address"] = ""
    rows[17]["address"] = ""

    return pd.DataFrame(rows)


def generate_census(
    rng: np.random.Generator, geoids: list[str]
) -> pd.DataFrame:
    """Generate mock_census_tract_data.csv with one row per GEOID."""
    n = len(geoids)

    # poverty_rate: 3.0-45.0, right-skewed via lognormal
    raw_poverty = rng.lognormal(mean=2.5, sigma=0.6, size=n)
    # Scale to 3.0-45.0 range
    poverty_rate = 3.0 + (raw_poverty - raw_poverty.min()) / (
        raw_poverty.max() - raw_poverty.min()
    ) * 42.0

    # median_household_income: 22000-120000, negatively correlated with poverty
    # Higher poverty -> lower income
    noise = rng.normal(0, 5000, size=n)
    income_base = 120000 - (poverty_rate - 3.0) / 42.0 * 98000
    median_income = np.clip(income_base + noise, 22000, 120000)

    df = pd.DataFrame(
        {
            "GEOID": geoids,
            "poverty_rate": np.round(poverty_rate, 1),
            "median_household_income": np.round(median_income, 0).astype(int),
            "data_vintage": "ACS 2022 5-Year Estimates",
        }
    )

    # Make exactly 3 rows have blank values (poverty_rate and median_income)
    blank_indices = [2, 10, 20] if len(geoids) > 20 else list(range(min(3, n)))
    for idx in blank_indices:
        if idx < len(df):
            df.loc[idx, "poverty_rate"] = np.nan
            df.loc[idx, "median_household_income"] = np.nan

    return df


def generate_cdc_places(
    rng: np.random.Generator,
    geoids: list[str],
    census_df: pd.DataFrame,
) -> pd.DataFrame:
    """Generate mock_cdc_places_data.csv, mildly correlated with poverty."""
    n = len(geoids)

    # Base diabetes prevalence: 5.0-22.0, mildly correlated with poverty
    poverty_values = census_df["poverty_rate"].values.copy()
    # Fill NaN with median for correlation
    median_pov = np.nanmedian(poverty_values)
    poverty_filled = np.where(np.isnan(poverty_values), median_pov, poverty_values)

    # Mild correlation: base from poverty + noise
    base = 5.0 + (poverty_filled - 3.0) / 42.0 * 10.0
    noise = rng.normal(0, 3.0, size=n)
    diabetes = np.clip(base + noise, 5.0, 22.0)

    return pd.DataFrame(
        {
            "GEOID": geoids,
            "DIABETES": np.round(diabetes, 1),
            "data_vintage": "CDC PLACES 2022",
        }
    )


def generate_usda_lila(
    rng: np.random.Generator,
    geoids: list[str],
    census_df: pd.DataFrame,
) -> pd.DataFrame:
    """Generate mock USDA LILA data keyed by 2020 GEOID.

    Per spec_updates_2.md §5.2: binary LILA flag plus area-weighted population
    counts. Mildly correlated with poverty so the mock map looks realistic.
    """
    n = len(geoids)
    poverty = census_df["poverty_rate"].values.copy()
    median_pov = float(np.nanmedian(poverty))
    poverty_filled = np.where(np.isnan(poverty), median_pov, poverty)

    # Tracts above ~70th-percentile poverty are flagged LILA, with noise.
    threshold = float(np.percentile(poverty_filled, 70))
    flag_prob = 1.0 / (1.0 + np.exp(-(poverty_filled - threshold) / 2.0))
    lila_flag = (rng.random(n) < flag_prob).astype(int)

    # Population beyond 1 mile from a supermarket: scales with poverty.
    lapop1 = np.clip(
        rng.normal(loc=600 + poverty_filled * 30, scale=200, size=n),
        0,
        None,
    ).round().astype(int)
    # Low-income subset of low-access pop.
    lalowi1 = (lapop1 * rng.uniform(0.25, 0.55, size=n)).round().astype(int)

    return pd.DataFrame(
        {
            "GEOID": geoids,
            "LILATracts_1And10": lila_flag,
            "lapop1": lapop1,
            "lalowi1": lalowi1,
            "data_vintage": "USDA Food Access Research Atlas (mock, 2010 boundaries)",
        }
    )


# Counties + city seeds for the Nashville MSA (rough centroids).
_MSA_CITIES = [
    ("Nashville", 36.1627, -86.7816, "37203"),
    ("Franklin", 35.9251, -86.8689, "37064"),
    ("Murfreesboro", 35.8456, -86.3903, "37130"),
    ("Hendersonville", 36.3047, -86.6200, "37075"),
    ("Gallatin", 36.3884, -86.4467, "37066"),
    ("Mt. Juliet", 36.2009, -86.5186, "37122"),
    ("Brentwood", 35.9889, -86.7833, "37027"),
    ("Springfield", 36.5089, -86.8864, "37172"),
    ("Columbia", 35.6151, -87.0353, "38401"),
    ("Lebanon", 36.2081, -86.2911, "37087"),
    ("Dickson", 36.0770, -87.3878, "37055"),
    ("Smyrna", 35.9828, -86.5186, "37167"),
]


def generate_giving_matters(rng: np.random.Generator, n: int = 12) -> pd.DataFrame:
    """Generate a small mock Giving Matters CSV (10-15 fake nonprofits).

    Schema is illustrative — the real CFMT schema is TBD per spec §3.1.
    """
    categories = [
        "Education",
        "Health & Human Services",
        "Hunger Relief",
        "Workforce Development",
        "Housing",
        "Youth Services",
    ]
    name_prefixes = [
        "Greater Nashville", "Middle Tennessee", "Hope", "Hands of",
        "Community", "Riverbend", "Cumberland", "Heartland", "Foundation for",
        "United", "Bridge", "Open Door",
    ]
    name_suffixes = [
        "Cares", "Coalition", "Alliance", "Project", "Initiative",
        "Foundation", "Partners", "Network", "Center", "Outreach",
    ]

    rows: list[dict] = []
    for i in range(n):
        city = _MSA_CITIES[i % len(_MSA_CITIES)]
        name = f"{rng.choice(name_prefixes)} {rng.choice(name_suffixes)}"
        street_num = int(rng.integers(100, 5000))
        street = rng.choice(STREET_NAMES)
        rows.append(
            {
                "Org Name": name,
                "Address": f"{street_num} {street}, {city[0]}, TN {city[3]}",
                "Category": rng.choice(categories),
                "Latitude": round(city[1] + float(rng.uniform(-0.04, 0.04)), 6),
                "Longitude": round(city[2] + float(rng.uniform(-0.05, 0.05)), 6),
            }
        )
    return pd.DataFrame(rows)


def generate_geocode_cache(partners_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Generate mock geocode cache for partners with addresses."""
    rows: list[dict] = []
    for _, partner in partners_df.iterrows():
        addr = partner["address"]
        if not addr or str(addr).strip() == "":
            continue
        # Generate plausible Nashville coordinates
        lat = 36.1627 + rng.uniform(-0.08, 0.08)
        lon = -86.7816 + rng.uniform(-0.12, 0.12)
        rows.append(
            {
                "address": addr,
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate mock CSV data for NFP Food Insecurity Map."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for reproducibility (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    output_dir = Path(args.output_dir)

    # Load GEOIDs (prefer MSA-wide tracts.geojson, fall back to legacy file)
    geojson_path = GEOJSON_PATH if Path(GEOJSON_PATH).exists() else LEGACY_GEOJSON_PATH
    print(f"Loading GEOIDs from {geojson_path}...")
    geoids = load_geoids(geojson_path)
    print(f"Loaded {len(geoids)} GEOIDs.")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate partners CSV
    print("Generating mock partners data (30 rows)...")
    partners_df = generate_partners(rng, n=30)
    partners_path = output_dir / "mock_nfp_partners.csv"
    partners_df.to_csv(partners_path, index=False)
    print(f"  Wrote {len(partners_df)} rows to {partners_path}")

    # Generate census CSV
    print(f"Generating mock census data ({len(geoids)} rows)...")
    census_df = generate_census(rng, geoids)
    census_path = output_dir / "mock_census_tract_data.csv"
    census_df.to_csv(census_path, index=False)
    print(f"  Wrote {len(census_df)} rows to {census_path}")

    # Generate CDC PLACES CSV
    print(f"Generating mock CDC PLACES data ({len(geoids)} rows)...")
    cdc_df = generate_cdc_places(rng, geoids, census_df)
    cdc_path = output_dir / "mock_cdc_places_data.csv"
    cdc_df.to_csv(cdc_path, index=False)
    print(f"  Wrote {len(cdc_df)} rows to {cdc_path}")

    # Generate USDA LILA mock CSV (tract level only)
    print(f"Generating mock USDA LILA data ({len(geoids)} rows)...")
    lila_df = generate_usda_lila(rng, geoids, census_df)
    lila_path = output_dir / "mock_usda_lila_data.csv"
    lila_df.to_csv(lila_path, index=False)
    print(f"  Wrote {len(lila_df)} rows to {lila_path}")

    # Generate Giving Matters mock CSV (small, illustrative)
    print("Generating mock Giving Matters data (12 rows)...")
    gm_df = generate_giving_matters(rng, n=12)
    gm_path = output_dir / "mock_giving_matters.csv"
    gm_df.to_csv(gm_path, index=False)
    print(f"  Wrote {len(gm_df)} rows to {gm_path}")

    # Generate geocode cache
    print("Generating mock geocode cache...")
    cache_df = generate_geocode_cache(partners_df, rng)
    cache_path = output_dir / "mock_geocode_cache.csv"
    cache_df.to_csv(cache_path, index=False)
    print(f"  Wrote {len(cache_df)} rows to {cache_path}")

    print(f"\nSUCCESS: All mock data written to {output_dir}")


if __name__ == "__main__":
    main()
