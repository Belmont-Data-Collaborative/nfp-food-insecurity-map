"""YAML configuration loader for project.yml.

Provides accessor functions for all project configuration.
Caches the parsed YAML so the file is read exactly once.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


_CONFIG_CACHE: dict[str, Any] | None = None


def _load_config() -> dict[str, Any]:
    """Load and cache project.yml. Supports PROJECT_CONFIG env override."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    config_path = os.environ.get("PROJECT_CONFIG", "project.yml")
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        _CONFIG_CACHE = yaml.safe_load(f)

    return _CONFIG_CACHE


def reload_config() -> dict[str, Any]:
    """Force reload of project.yml (useful for testing)."""
    global _CONFIG_CACHE
    _CONFIG_CACHE = None
    return _load_config()


def get_project() -> dict[str, Any]:
    """Return project metadata dict (name, slug, orgs)."""
    return _load_config()["project"]


def get_geography() -> dict[str, Any]:
    """Return geography dict (FIPS codes, center, zoom, tiger_year)."""
    return _load_config()["geography"]


def get_data_sources() -> dict[str, Any]:
    """Return dict of all data source configs (census_acs, health_lila)."""
    return _load_config()["data_sources"]


def get_all_layer_configs() -> list[dict[str, Any]]:
    """Return flattened list of all choropleth variable configs across sources."""
    layers: list[dict[str, Any]] = []
    for source_key, source_cfg in get_data_sources().items():
        for var in source_cfg.get("variables", []):
            layer = dict(var)
            layer["source_key"] = source_key
            layer["geoid_column"] = source_cfg.get("geoid_column", "GEOID")
            layer["output_prefix"] = source_cfg.get("output_prefix", source_key)
            layers.append(layer)
    return layers


def get_partner_config() -> dict[str, Any]:
    """Return partner types, colors, icons, S3 paths."""
    return _load_config()["partners"]


def get_map_display() -> dict[str, Any]:
    """Return map display settings (tiles, attribution, zoom limits)."""
    return _load_config()["map_display"]


def get_granularities() -> list[dict[str, Any]]:
    """Return list of granularity dicts (id, label, geo_file)."""
    return get_map_display()["granularities"]


def is_layer_available_for_granularity(
    layer_config: dict[str, Any], granularity: str
) -> bool:
    """Return True if the layer's source publishes data at the given granularity.

    Looks up the layer's ``source_key`` in ``data_sources`` and checks the
    source's ``s3_prefix`` mapping. ZIP granularity matches either the
    ``zip`` or ``zcta`` key (the two are used interchangeably across sources).
    """
    source_key = layer_config.get("source_key")
    if source_key is None:
        return True
    sources = get_data_sources()
    src = sources.get(source_key, {})
    prefixes = src.get("s3_prefix", {})
    if not prefixes:
        # No granularity restrictions declared — assume available everywhere.
        return True
    if granularity == "zip":
        return "zip" in prefixes or "zcta" in prefixes
    return granularity in prefixes


def get_layer_type(layer_config: dict[str, Any]) -> str:
    """Return the layer rendering type: ``"categorical"`` or ``"continuous"``.

    Defaults to ``"continuous"`` when ``layer_type`` is not present.
    """
    return layer_config.get("layer_type", "continuous")


def get_layer_categories(layer_config: dict[str, Any]) -> dict[Any, str]:
    """Return the ``categories`` mapping for a categorical layer.

    For continuous layers (or layers with no categories defined) returns ``{}``.
    """
    if get_layer_type(layer_config) != "categorical":
        return {}
    return dict(layer_config.get("categories", {}))


def get_county_fips_set() -> set[str]:
    """Return set of 5-char state+county FIPS strings for all MSA counties.

    Reads from ``msa_counties`` in project.yml. Falls back to the legacy
    single ``county_fips`` field for backward compatibility.

    Example return: ``{"47015", "47021", "47037", ...}``
    """
    geo = get_geography()
    state = geo["state_fips"]
    msa = geo.get("msa_counties")
    if msa:
        return {state + c["fips"] for c in msa}
    # Legacy single-county fallback
    return {state + geo["county_fips"]}
