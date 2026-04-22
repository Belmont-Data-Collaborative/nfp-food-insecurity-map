"""Export project.yml → data/config.json for the frontend.

The browser can't parse YAML natively and shouldn't need to know about
pipeline-only fields (S3 keys, filters, crosswalks). This emits a
frontend-shaped JSON derivative of project.yml that map.js consumes
at startup.

Shape mirrors what the original design mockup hardcoded in map.js:
{
  "project":       {"name", "slug"},
  "geography":     {"map_center", "default_zoom", "msa_counties_count"},
  "indicators":    [{id, label, col, src, granularities, palette, fmt, caption, categorical?, categories?}],
  "partner_types": {<type_id>: {label, color, icon}},
  "palettes":      {<palette_id>: [hex, hex, ...]},
  "palette_meta":  [{id, label}]
}
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from src import config as app_config
from src.config_loader import (
    get_data_sources,
    get_geography,
    get_map_display,
    get_partner_config,
    get_project,
    reload_config,
)
from pipeline.load_source import upload_file_to_s3

logger = logging.getLogger(__name__)

OUTPUT_PATH = Path("data/config.json")


def _infer_granularities(source_config: dict) -> list[str]:
    """Return the list of granularities this source supports.

    Sources mark themselves tract-only with ``tract_only: true``; everything
    else is assumed to support both tract and zip if s3_prefix has a zip key.
    """
    if source_config.get("tract_only"):
        return ["tract"]
    prefixes = source_config.get("s3_prefix") or {}
    grans = [g for g in ("tract", "zip") if g in prefixes]
    return grans or ["tract"]


def _build_indicators() -> list[dict]:
    """Flatten data_sources[*].variables[*] into a single frontend list."""
    indicators: list[dict] = []
    for source_key, source_cfg in get_data_sources().items():
        src = source_cfg.get("frontend_src", source_key)
        grans = _infer_granularities(source_cfg)
        for var in source_cfg.get("variables", []) or []:
            indicator = {
                "id": var.get("id") or var["column"].lower(),
                "label": var["display_name"],
                "col": var["column"],
                "src": src,
                "granularities": grans,
                "palette": var.get("palette", "greens"),
                "fmt": var.get("format_str", "{}"),
                "caption": var.get("caption", ""),
                "unit_label": var.get("legend_name", ""),
                "default_visible": bool(var.get("default_visible", False)),
            }
            if var.get("layer_type") == "categorical":
                indicator["categorical"] = True
                indicator["categories"] = var.get("categories", {})
            indicators.append(indicator)
    return indicators


def _build_partner_types() -> dict:
    """Collapse partner types YAML into {type_id: {label, color, icon}}."""
    partner_cfg = get_partner_config()
    out: dict[str, dict] = {}
    for type_id, spec in (partner_cfg.get("types") or {}).items():
        out[type_id] = {
            "label": spec.get("label", type_id),
            "color": spec.get("color", "#607D8B"),
            "icon": spec.get("icon", "circle"),
        }
    return out


def export_config() -> Path:
    """Write data/config.json from project.yml. Returns the output path."""
    project = get_project()
    geo = get_geography()
    display = get_map_display()
    raw = reload_config()

    cfg = {
        "project": {
            "name": project.get("name", ""),
            "slug": project.get("slug", ""),
            "primary_org": project.get("primary_org", ""),
            "secondary_org": project.get("secondary_org", ""),
        },
        "geography": {
            "map_center": geo.get("map_center", [36.05, -86.60]),
            "default_zoom": geo.get("default_zoom", 9),
            "msa_counties_count": len(geo.get("msa_counties", [])),
            "msa_name": geo.get("msa_name", ""),
        },
        "map_display": {
            "tiles": display.get("tiles", "cartodbpositron"),
            "tile_attribution": display.get("tile_attribution", ""),
            "min_zoom": display.get("min_zoom", 8),
            "max_zoom": display.get("max_zoom", 16),
            "granularities": display.get("granularities", []),
        },
        "indicators": _build_indicators(),
        "partner_types": _build_partner_types(),
        "palettes": raw.get("palettes", {}),
        "palette_meta": raw.get("palette_meta", []),
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(cfg, indent=2))
    logger.info(
        "Wrote %s (%d indicators, %d partner types, %d palettes)",
        OUTPUT_PATH, len(cfg["indicators"]),
        len(cfg["partner_types"]), len(cfg["palettes"]),
    )
    if app_config.S3_OUTPUT_BUCKET:
        key = f"{app_config.S3_OUTPUT_PREFIX.rstrip('/')}/{OUTPUT_PATH.name}"
        upload_file_to_s3(str(OUTPUT_PATH), app_config.S3_OUTPUT_BUCKET, key)
    return OUTPUT_PATH


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    export_config()
