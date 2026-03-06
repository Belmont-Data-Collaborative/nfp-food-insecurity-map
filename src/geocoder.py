from __future__ import annotations

import logging
import time

import pandas as pd
import streamlit as st
from geopy.geocoders import Nominatim

from src import config

logger = logging.getLogger(__name__)


@st.cache_resource
def get_geolocator() -> Nominatim:
    """Return a cached Nominatim geolocator with config.NOMINATIM_USER_AGENT."""
    return Nominatim(user_agent=config.NOMINATIM_USER_AGENT)


def geocode_partners(
    partners_df: pd.DataFrame,
    cache_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Geocode partner addresses, using cache where available.

    Returns:
        tuple of (geocoded_partners_df, updated_cache_df)
        - geocoded_partners_df: partners_df with 'latitude' and 'longitude' columns.
          Failed/blank addresses have NaN for lat/lon.
        - updated_cache_df: cache_df with any new geocode results appended.

    IMPORTANT: This function NEVER raises for individual address failures.
    All per-address exceptions are caught silently; NaN is set for failed lookups.
    """
    geolocator = get_geolocator()

    result_df = partners_df.copy()
    result_df["latitude"] = float("nan")
    result_df["longitude"] = float("nan")

    # Build a lookup from (address) -> (lat, lon) from the cache
    cache_lookup: dict[str, tuple[float, float]] = {}
    if not cache_df.empty and "address" in cache_df.columns:
        for _, row in cache_df.iterrows():
            addr = str(row.get("address", "")).strip()
            lat = row.get("latitude")
            lon = row.get("longitude")
            if addr and pd.notna(lat) and pd.notna(lon):
                cache_lookup[addr] = (float(lat), float(lon))

    new_cache_rows: list[dict] = []

    for idx, row in result_df.iterrows():
        address = str(row.get("address", "")).strip()

        # Skip blank addresses
        if not address or address.lower() == "nan":
            continue

        # Check cache first
        if address in cache_lookup:
            result_df.at[idx, "latitude"] = cache_lookup[address][0]
            result_df.at[idx, "longitude"] = cache_lookup[address][1]
            continue

        # Geocode with Nominatim - NEVER raise for individual failures
        try:
            query = f"{address}, Davidson County, TN, USA"
            location = geolocator.geocode(query, timeout=10)
            if location is not None:
                result_df.at[idx, "latitude"] = location.latitude
                result_df.at[idx, "longitude"] = location.longitude
                cache_lookup[address] = (location.latitude, location.longitude)
                new_cache_rows.append({
                    "organization_name": row.get("organization_name", ""),
                    "address": address,
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                })
            # Rate limit: 1 request per second (mandatory for Nominatim)
            time.sleep(1)
        except Exception:
            # Silently continue - NaN stays for this address
            logger.debug("Geocoding failed for address: %s", address)
            continue

    # Build updated cache
    updated_cache = cache_df.copy()
    if new_cache_rows:
        new_df = pd.DataFrame(new_cache_rows)
        updated_cache = pd.concat([updated_cache, new_df], ignore_index=True)

    return result_df, updated_cache
