"""Environment-driven configuration.

All layer definitions, partner types, colors, and S3 paths live in
project.yml and are accessed via src.config_loader.

This module retains only environment-based settings and user-facing
message constants used by the pipeline.
"""
from __future__ import annotations

import logging
import os


def _get_env(key: str, default: str | None = None) -> str | None:
    """Return os.environ[key] when set, else default."""
    return os.environ.get(key, default)


APP_ENV: str = os.environ.get("APP_ENV", "development")
IS_PRODUCTION: bool = APP_ENV == "production"
LOG_LEVEL: int = logging.WARNING if IS_PRODUCTION else logging.INFO

AWS_BUCKET_NAME: str = _get_env("AWS_BUCKET_NAME", "") or ""

# Output bucket/prefix: pipeline writes GeoJSONs, CSVs, config.json here.
S3_OUTPUT_BUCKET: str = _get_env("S3_OUTPUT_BUCKET", "") or ""
S3_OUTPUT_PREFIX: str = _get_env("S3_OUTPUT_PREFIX", "") or ""

MOCK_DATA_DIR: str = os.environ.get("MOCK_DATA_DIR", "data/mock/")
USE_MOCK_DATA: bool = os.environ.get("USE_MOCK_DATA", "false").lower() == "true"

ERROR_DATA_LOAD: str = (
    "Map data could not be loaded. "
    "Please refresh the page or contact BDAIC support."
)
ERROR_MISSING_COLUMN: str = (
    "Partner data is missing required column: {column_name}. "
    "Please check the data file."
)
WARNING_GEOCODE_FAILURES: str = (
    "{count} partner location(s) could not be mapped "
    "due to address lookup errors."
)
WARNING_NO_GEOID_MATCH: str = (
    "No data could be matched to map boundaries. "
    "Please contact BDAIC support."
)
