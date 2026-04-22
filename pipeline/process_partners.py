"""Partner data pipeline: S3 source -> geocoding -> GeoJSON output.

Handles loading partner CSV, geocoding with cache, coordinate validation,
and GeoJSON export.
"""
from __future__ import annotations

import io
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
from botocore.exceptions import ClientError
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
from geopy.geocoders import Nominatim

logger = logging.getLogger(__name__)

# Davidson County bounding box for coordinate validation
DAVIDSON_BBOX = {
    "min_lat": 35.97,
    "max_lat": 36.41,
    "min_lon": -87.05,
    "max_lon": -86.52,
}


class GeocodingError(Exception):
    """Fatal pipeline errors only."""


def load_partner_csv(
    partner_config: dict[str, Any],
    use_mock: bool = False,
    mock_dir: str = "data/mock/",
) -> pd.DataFrame:
    """Load partner CSV from S3 or local mock."""
    if use_mock:
        path = os.path.join(mock_dir, "mock_nfp_partners.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Mock partner file not found: {path}")
        return pd.read_csv(path)

    from pipeline.load_source import get_s3_client

    bucket = partner_config["s3_bucket"]
    key = partner_config["s3_key"]
    client = get_s3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return pd.read_csv(io.BytesIO(body))


def load_geocode_cache(
    partner_config: dict[str, Any],
    use_mock: bool = False,
    mock_dir: str = "data/mock/",
) -> pd.DataFrame:
    """Load geocode cache from S3 or local mock."""
    if use_mock:
        cache_path = os.path.join(mock_dir, "mock_geocode_cache.csv")
        if os.path.exists(cache_path):
            return pd.read_csv(cache_path)
        return pd.DataFrame(columns=["address", "latitude", "longitude"])

    from pipeline.load_source import get_s3_client

    bucket = partner_config["s3_bucket"]
    key = partner_config["geocode_cache_key"]
    try:
        client = get_s3_client()
        response = client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        return pd.read_csv(io.BytesIO(body))
    except (ClientError, FileNotFoundError, pd.errors.ParserError) as exc:
        logger.info("No existing geocode cache found on S3: %s", exc)
        return pd.DataFrame(columns=["address", "latitude", "longitude"])


def save_geocode_cache(
    cache_df: pd.DataFrame,
    partner_config: dict[str, Any],
    use_mock: bool = False,
) -> None:
    """Write updated geocode cache to S3. Skip in mock mode."""
    if use_mock:
        return

    try:
        from pipeline.load_source import get_s3_client

        client = get_s3_client()
        csv_buffer = io.StringIO()
        cache_df.to_csv(csv_buffer, index=False)
        client.put_object(
            Bucket=partner_config["s3_bucket"],
            Key=partner_config["geocode_cache_key"],
            Body=csv_buffer.getvalue().encode("utf-8"),
        )
        logger.info("Geocode cache saved to S3")
    except (ClientError, OSError) as exc:
        logger.warning("Failed to write geocode cache to S3: %s", exc)


def validate_coordinates(lat: float, lon: float) -> bool:
    """Check if coordinates fall within Davidson County bounding box."""
    return (
        DAVIDSON_BBOX["min_lat"] <= lat <= DAVIDSON_BBOX["max_lat"]
        and DAVIDSON_BBOX["min_lon"] <= lon <= DAVIDSON_BBOX["max_lon"]
    )


def geocode_partners(
    partners_df: pd.DataFrame,
    partner_config: dict[str, Any],
    use_mock: bool = False,
    mock_dir: str = "data/mock/",
) -> pd.DataFrame:
    """Geocode partner addresses with cache support.

    Returns DataFrame with columns:
        partner_name, address, partner_type, latitude, longitude, geocode_status.
    """
    geolocator = Nominatim(user_agent="nfp_food_insecurity_map")
    cache_df = load_geocode_cache(partner_config, use_mock, mock_dir)

    # Build cache lookup
    cache_lookup: dict[str, tuple[float, float]] = {}
    for _, row in cache_df.iterrows():
        addr = str(row.get("address", ""))
        lat = row.get("latitude")
        lon = row.get("longitude")
        if addr and pd.notna(lat) and pd.notna(lon):
            cache_lookup[addr] = (float(lat), float(lon))

    results: list[dict] = []
    new_cache_entries: list[dict] = []
    success_count = 0
    fail_count = 0

    for _, partner in partners_df.iterrows():
        name = partner.get("partner_name", "")
        address = partner.get("address", "")
        ptype = partner.get("partner_type", "")

        if pd.isna(address) or str(address).strip() == "":
            results.append({
                "partner_name": name,
                "address": "",
                "partner_type": ptype,
                "latitude": float("nan"),
                "longitude": float("nan"),
                "geocode_status": "failed",
            })
            fail_count += 1
            continue

        address_str = str(address).strip()

        # Check cache
        if address_str in cache_lookup:
            lat, lon = cache_lookup[address_str]
            results.append({
                "partner_name": name,
                "address": address_str,
                "partner_type": ptype,
                "latitude": lat,
                "longitude": lon,
                "geocode_status": "success",
            })
            success_count += 1
            continue

        # Nominatim geocode
        try:
            query = f"{address_str}, Davidson County, TN, USA"
            time.sleep(1)
            location = geolocator.geocode(query, timeout=10)
            if location is not None:
                lat = location.latitude
                lon = location.longitude
                if validate_coordinates(lat, lon):
                    results.append({
                        "partner_name": name,
                        "address": address_str,
                        "partner_type": ptype,
                        "latitude": lat,
                        "longitude": lon,
                        "geocode_status": "success",
                    })
                    cache_lookup[address_str] = (lat, lon)
                    new_cache_entries.append({
                        "address": address_str,
                        "latitude": lat,
                        "longitude": lon,
                    })
                    success_count += 1
                else:
                    logger.warning(
                        "Coordinates for '%s' outside Davidson County: (%s, %s)",
                        name, lat, lon,
                    )
                    results.append({
                        "partner_name": name,
                        "address": address_str,
                        "partner_type": ptype,
                        "latitude": float("nan"),
                        "longitude": float("nan"),
                        "geocode_status": "failed",
                    })
                    fail_count += 1
            else:
                results.append({
                    "partner_name": name,
                    "address": address_str,
                    "partner_type": ptype,
                    "latitude": float("nan"),
                    "longitude": float("nan"),
                    "geocode_status": "failed",
                })
                fail_count += 1
        except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError, ValueError) as exc:
            logger.warning("Geocoding failed for '%s': %s", address_str, exc)
            results.append({
                "partner_name": name,
                "address": address_str,
                "partner_type": ptype,
                "latitude": float("nan"),
                "longitude": float("nan"),
                "geocode_status": "failed",
            })
            fail_count += 1

    # Update cache
    if new_cache_entries:
        new_df = pd.DataFrame(new_cache_entries)
        updated_cache = pd.concat([cache_df, new_df], ignore_index=True)
        save_geocode_cache(updated_cache, partner_config, use_mock)

    total = success_count + fail_count
    logger.info("Geocoded %d/%d partners (%d failed)", success_count, total, fail_count)

    return pd.DataFrame(results)


def partners_to_geojson(df: pd.DataFrame, output_path: str) -> None:
    """Convert geocoded partner DataFrame to GeoJSON and save."""
    features = []
    for _, row in df.iterrows():
        if pd.isna(row["latitude"]) or pd.isna(row["longitude"]):
            continue
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]],
            },
            "properties": {
                "partner_name": row["partner_name"],
                "address": row.get("address", ""),
                "partner_type": row.get("partner_type", ""),
                "geocode_status": row.get("geocode_status", ""),
            },
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2)

    logger.info("Saved %d partners to %s", len(features), output_path)


def run(
    partner_config: dict[str, Any],
    use_mock: bool = False,
    mock_dir: str = "data/mock/",
) -> None:
    """Run the full partner processing pipeline."""
    logger.info("Loading partner data...")
    partners_df = load_partner_csv(partner_config, use_mock, mock_dir)

    required_cols = ["partner_name", "address", "partner_type"]
    missing = [c for c in required_cols if c not in partners_df.columns]
    if missing:
        raise GeocodingError(f"Partner CSV missing columns: {missing}")

    logger.info("Geocoding %d partners...", len(partners_df))
    geocoded_df = geocode_partners(
        partners_df, partner_config, use_mock, mock_dir
    )

    output_path = "data/partners.geojson"
    partners_to_geojson(geocoded_df, output_path)

    from src import config as app_config
    from pipeline.load_source import upload_file_to_s3
    if app_config.S3_OUTPUT_BUCKET:
        key = f"{app_config.S3_OUTPUT_PREFIX.rstrip('/')}/partners.geojson"
        upload_file_to_s3(output_path, app_config.S3_OUTPUT_BUCKET, key)

    logger.info("Partner pipeline complete")
