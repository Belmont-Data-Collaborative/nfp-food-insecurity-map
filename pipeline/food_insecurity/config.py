"""Project-wide constants for the TN tract food-insecurity index.

Every external value is documented here so a reader of just this file knows
where each number came from and whether the data behind it lives in S3.
The other modules (`io`, `predictors`, `compute`, `validate`, `build_index`)
import constants from here — they do not redefine them.

PROVENANCE
----------
Coefficients (MMG_COEFFICIENTS)
    Source: Feeding America, "Map the Meal Gap 2025 Technical Brief",
    Appendix Table 1, full-population column. Link in docs/references.md.
    Not in S3 (constants, not data).

    Why these are hardcoded constants, not a pipeline:
    The brief publishes 7 values in a PDF appendix table -- no API, no
    structured download -- and the update cadence is at most once per FA
    release (annual). Building a lake pipeline to extract 7 PDF numbers
    would cost more than the manual refresh it would replace AND would
    add a brittle PDF-parsing dependency. The values are regression
    parameters, closer to code constants than to observational data, so
    they live here next to the equation that consumes them. Refresh
    procedure on each new MMG release: docs/backlog.md ->
    "Future MMG releases" (numbered checklist).

Year (YEAR = 2023)
    Latest joint vintage where ACS profile + Feeding America county both have
    data. Derived in docs/plan.md.

State (STATE_FIPS = "47")
    Tennessee — U.S. Census standard state FIPS. The compute runs over every
    TN tract (~1,497 across all 95 counties).

ACS predictor columns (PREDICTOR_COLUMNS, POPULATION_COLUMN)
    DP codes verified live against api.census.gov DP02/DP03/DP04/DP05 2023
    schema and sample-row sanity-checked on Davidson tract 47037012701.
    The data lives in S3 (ACS_PROFILE_KEY below).

    v1 LIMITATION — B14006 substitution:
    MMG's published predictor is B14006 (non-student poverty), which excludes
    students living in dorms whose nominal income looks poor. B14006 is NOT
    in the lake. For v1 we substitute DP03_0128PE (all-people poverty), which
    over-states FI in student-heavy tracts (Belmont, Vandy, UT Knoxville,
    MTSU). Tracts with the substitution carry `b14006_placeholder=True` in
    the output so downstream consumers see the caveat. Upgrade path is a
    Census API pull using CENSUS_API_KEY (NOT a new lake pipeline).

S3 inputs (ACS_PROFILE_KEY, FA_COUNTY_KEY)
    Verified by direct S3 listing in bdaic-public-transform.
    NB: the "2025" in FA_COUNTY_KEY is the FA *release* year; the file's
    data_year column covers 2019-2023 — we filter to YEAR at read time.

LOW_CONFIDENCE_POP_THRESHOLD (500)
    Judgment-call cutoff documented in docs/plan.md (ACS tract margins of
    error are large at small populations). Not external.

NOT IN S3 (no action needed for v1; documented for completeness)
    * B14006 non-student poverty — Census API pull, not a new pipeline.
    * TIGER/Line tract shapefiles — needed only for the map, downloaded at
      build time the same way nfp-food-insecurity-map does it.
    * MMG state fixed effect — unpublished; replaced by the county anchor.
    * NielsenIQ cost-of-food / meal-cost / budget-shortfall — proprietary,
      out of scope (plan.md).
"""

from pathlib import Path

# ---- Vintage and geographic scope ----
YEAR = 2023
STATE_FIPS = "47"  # Tennessee

# ---- MMG regression coefficients (Appendix Table 1, full-pop column) ----
MMG_COEFFICIENTS = {
    "poverty": 0.332,
    "unemployment": 0.460,
    "median_income": -0.001,
    "pct_hispanic": 0.002,
    "pct_black": -0.043,
    "homeownership": -0.071,
    "disability": 0.198,
}

# ---- ACS predictor -> DP code mapping (DP percents are on a 0-100 scale) ----
PREDICTOR_COLUMNS = {
    "poverty": "DP03_0128PE",  # v1 placeholder for B14006 (see PROVENANCE)
    "unemployment": "DP03_0009PE",
    "median_income": "DP03_0062E",  # estimate (dollars), not percent
    "pct_hispanic": "DP05_0076PE",
    "pct_black": "DP05_0038PE",
    "homeownership": "DP04_0046PE",
    "disability": "DP02_0072PE",
}
POPULATION_COLUMN = "DP05_0001E"  # weighting denominator only; not in the equation

# ---- S3 inputs (read only; no v1 S3 writes) ----
S3_BUCKET = "bdaic-public-transform"
ACS_PROFILE_KEY = f"census_acs5_profile/geo_tract/census_acs5_profile_transform_tract_{YEAR}.csv"
FA_COUNTY_KEY = "feeding_america/2025/feeding_america_county_2025.csv"

# ---- Join / lookup columns ----
# GEO_ID values look like "1400000US47037012701"; io.py strips the
# "1400000US" summary-level prefix to get the 11-char tract GEOID.
ACS_GEOID_COL = "GEO_ID"
FA_FIPS_COL = "FIPS"  # 5-digit county FIPS, e.g. "47037"
FA_YEAR_COL = "data_year"
FA_FI_RATE_COL = "Overall Food Insecurity Rate"  # 0-1 scale

# ---- Output (local; no S3 write in v1) ----
OUTPUT_DIR = Path("./output")
OUTPUT_BASENAME = f"tn_tract_fi_{YEAR}"  # writes {basename}.parquet and {basename}.csv

# ---- Post-processing ----
LOW_CONFIDENCE_POP_THRESHOLD = 500  # flag tracts under this population
