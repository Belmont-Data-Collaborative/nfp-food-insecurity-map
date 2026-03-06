from __future__ import annotations

import os

import streamlit as st


def _get_secret(key: str, default: str | None = None) -> str | None:
    """Check os.environ.get(key) FIRST, then fall back to st.secrets.

    MUST check os.environ before st.secrets to avoid StreamlitAPIException
    at import time.
    """
    val = os.environ.get(key)
    if val is not None:
        return val
    try:
        return st.secrets[key]
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Secrets / Environment
# ---------------------------------------------------------------------------

BDAIC_BUCKET: str = _get_secret("BDAIC_BUCKET", "") or "bdaic-nfp-bucket"

USE_MOCK_DATA: bool = (_get_secret("USE_MOCK_DATA", "true") or "true").lower() == "true"

MOCK_DATA_DIR: str = _get_secret("MOCK_DATA_DIR", "data/mock") or "data/mock"

# ---------------------------------------------------------------------------
# S3 Paths
# ---------------------------------------------------------------------------

S3_PREFIX: str = "nfp-mapping/"
S3_PARTNERS_KEY: str = S3_PREFIX + "nfp_partners.csv"
S3_CENSUS_KEY: str = S3_PREFIX + "census_tract_data.csv"
S3_CDC_KEY: str = S3_PREFIX + "cdc_places_data.csv"
S3_GEOCODE_CACHE_KEY: str = S3_PREFIX + "nfp_partners_geocoded_cache.csv"

# ---------------------------------------------------------------------------
# CSV Column Names
# ---------------------------------------------------------------------------

PARTNERS_REQUIRED_COLUMNS: list[str] = ["organization_name", "address", "partner_type"]
CENSUS_REQUIRED_COLUMNS: list[str] = ["GEOID", "poverty_rate", "median_household_income"]
CDC_REQUIRED_COLUMNS: list[str] = ["GEOID", "DIABETES_CrudePrev"]

# ---------------------------------------------------------------------------
# Partner Types
# ---------------------------------------------------------------------------

PARTNER_TYPE_COLORS: dict[str, str] = {
    "school_summer": "#E41A1C",
    "medical_health": "#377EB8",
    "transitional_housing": "#4DAF4A",
    "senior_services": "#984EA3",
    "community_development": "#FF7F00",
    "homeless_outreach": "#A65628",
    "workforce_development": "#F781BF",
    "after_school": "#999999",
}

PARTNER_TYPE_LABELS: dict[str, str] = {
    "school_summer": "School / Summer Programs",
    "medical_health": "Medical & Health Services",
    "transitional_housing": "Transitional Housing",
    "senior_services": "Senior Services",
    "community_development": "Community Development",
    "homeless_outreach": "Homeless Outreach",
    "workforce_development": "Workforce Development",
    "after_school": "After-School Programs",
}

# ---------------------------------------------------------------------------
# Choropleth Layers
# ---------------------------------------------------------------------------

CHOROPLETH_LAYERS: list[dict] = [
    {
        "id": "poverty_rate",
        "display_name": "Poverty Rate (Census)",
        "csv_column": "poverty_rate",
        "unit": "%",
        "format_string": "{:.1f}%",
        "data_vintage": "American Community Survey 2022",
    },
    {
        "id": "median_income",
        "display_name": "Median Household Income (Census)",
        "csv_column": "median_household_income",
        "unit": "$",
        "format_string": "${:,.0f}",
        "data_vintage": "American Community Survey 2022",
    },
    {
        "id": "diabetes",
        "display_name": "Diabetes Prevalence (CDC)",
        "csv_column": "DIABETES_CrudePrev",
        "unit": "%",
        "format_string": "{:.1f}%",
        "data_vintage": "CDC PLACES 2023",
    },
]

# ---------------------------------------------------------------------------
# Map Defaults
# ---------------------------------------------------------------------------

DAVIDSON_COUNTY_CENTER: tuple[float, float] = (36.1627, -86.7816)
DEFAULT_ZOOM_LEVEL: int = 11
DEFAULT_CHOROPLETH_LAYER: str = "median_income"
CDC_PLACES_INDICATOR_COLUMN: str = "DIABETES_CrudePrev"
NOMINATIM_USER_AGENT: str = "BDAIC-NFP-Mapping-Tool/1.0 (belmont.edu)"
GEOJSON_PATH: str = "data/shapefiles/davidson_county_tracts.geojson"

# ---------------------------------------------------------------------------
# User-Facing Messages
# ---------------------------------------------------------------------------

ERROR_DATA_LOAD: str = (
    "Map data could not be loaded. Please refresh the page or contact BDAIC support."
)
WARNING_GEOCODE_FAILURES: str = (
    "{count} partner location(s) could not be mapped due to address lookup errors."
)
WARNING_NO_PARTNERS: str = "No partner locations found in the data file."
WARNING_NO_CENSUS: str = "No census data available for Davidson County."
WARNING_GEOID_MISMATCH: str = "No data could be matched to map boundaries."
ERROR_MISSING_COLUMN: str = (
    "Partner data is missing required column: {col}. Please check the data file."
)
