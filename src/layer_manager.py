from __future__ import annotations

import logging

import branca.colormap
import folium
import pandas as pd

from src import config

logger = logging.getLogger(__name__)


def build_choropleth_layer(
    geojson: dict,
    data_df: pd.DataFrame,
    layer_config: dict,
) -> tuple[folium.GeoJson, branca.colormap.LinearColormap]:
    """Build a choropleth GeoJson layer joined on GEOID and its legend colormap.

    Args:
        geojson: GeoJSON dict with GEOID in feature properties.
        data_df: DataFrame with 'GEOID' column and the column specified in layer_config['csv_column'].
        layer_config: One of config.CHOROPLETH_LAYERS dicts.

    Returns:
        tuple of (geojson_layer, colormap)
        - geojson_layer: folium.GeoJson with style_function and popup.
        - colormap: branca.colormap.LinearColormap (YlOrRd) positioned bottom-right.
    """
    col = layer_config["csv_column"]
    fmt = layer_config["format_string"]
    vintage = layer_config["data_vintage"]
    display_name = layer_config["display_name"]

    # Build a lookup from GEOID -> value
    data_lookup: dict[str, float] = {}
    for _, row in data_df.iterrows():
        geoid = str(row["GEOID"]).zfill(11)
        val = row.get(col)
        if pd.notna(val):
            data_lookup[geoid] = float(val)

    # Determine min/max for the colormap
    values = list(data_lookup.values())
    if values:
        vmin = min(values)
        vmax = max(values)
    else:
        vmin = 0.0
        vmax = 1.0

    # Create YlOrRd colormap
    colormap = branca.colormap.LinearColormap(
        colors=["#FFFFB2", "#FED976", "#FEB24C", "#FD8D3C", "#FC4E2A", "#E31A1C", "#B10026"],
        vmin=vmin,
        vmax=vmax,
        caption=f"{display_name}",
    )

    def style_function(feature: dict) -> dict:
        geoid = str(feature["properties"].get("GEOID", "")).zfill(11)
        val = data_lookup.get(geoid)
        if val is not None:
            return {
                "fillColor": colormap(val),
                "color": "#333333",
                "weight": 0.5,
                "fillOpacity": 0.7,
            }
        return {
            "fillColor": "#EEEEEE",
            "color": "#999999",
            "weight": 0.5,
            "fillOpacity": 0.5,
        }

    def popup_function(feature: dict) -> folium.Popup:
        props = feature.get("properties", {})
        geoid = str(props.get("GEOID", "")).zfill(11)
        tract_name = props.get("NAMELSAD", f"Tract {props.get('NAME', geoid)}")
        val = data_lookup.get(geoid)
        if val is not None:
            formatted_val = fmt.format(val)
            html = (
                f"<b>Census Tract {props.get('NAME', geoid)}</b><br>"
                f"{display_name}: {formatted_val}<br>"
                f"<i>Source: {vintage}</i>"
            )
        else:
            html = "<b>Data not available for this tract.</b>"
        return folium.Popup(html, max_width=250)

    geojson_layer = folium.GeoJson(
        geojson,
        style_function=style_function,
        popup=folium.GeoJsonPopup(
            fields=["GEOID"],
            aliases=[""],
            labels=False,
            localize=False,
        ),
        name=display_name,
    )

    # Replace popup with custom popup function
    # We need to use a different approach for custom popups with GeoJson
    # Use GeoJsonTooltip for hover and custom popup via child features
    geojson_layer = folium.GeoJson(
        geojson,
        style_function=style_function,
        name=display_name,
    )

    # Add popups to each feature
    for feature in geojson.get("features", []):
        popup = popup_function(feature)
        coords = feature.get("geometry", {}).get("coordinates", [])
        # folium.GeoJson handles popups differently; we set popup on the layer
        # and use a highlight function

    # Instead, use the proper Folium popup approach via a custom GeoJson
    geojson_layer = folium.GeoJson(
        geojson,
        style_function=style_function,
        highlight_function=lambda x: {"weight": 2, "fillOpacity": 0.85},
        name=display_name,
    )

    # Add click popup for each feature via GeoJsonPopup won't work for custom HTML.
    # Use a different approach: iterate features and add popup HTML via child.
    # Actually, the proper way is to inject popup HTML into each feature's properties
    # and then use GeoJsonPopup.

    # Inject popup HTML into properties
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        geoid = str(props.get("GEOID", "")).zfill(11)
        val = data_lookup.get(geoid)
        if val is not None:
            formatted_val = fmt.format(val)
            props["_popup_html"] = (
                f"<b>Census Tract {props.get('NAME', geoid)}</b><br>"
                f"{display_name}: {formatted_val}<br>"
                f"<i>Source: {vintage}</i>"
            )
        else:
            props["_popup_html"] = "<b>Data not available for this tract.</b>"

    geojson_layer = folium.GeoJson(
        geojson,
        style_function=style_function,
        highlight_function=lambda x: {"weight": 2, "fillOpacity": 0.85},
        popup=folium.GeoJsonPopup(fields=["_popup_html"], labels=False),
        name=display_name,
    )

    return geojson_layer, colormap


def build_partner_markers(partners_df: pd.DataFrame) -> folium.FeatureGroup:
    """Build a FeatureGroup of CircleMarkers for geocoded partners.

    Rows with NaN lat/lon are skipped.
    Popup shows: organization name, plain-English type label, 'Nashville Food Project Partner'.
    """
    fg = folium.FeatureGroup(name="NFP Partners")

    for _, row in partners_df.iterrows():
        lat = row.get("latitude")
        lon = row.get("longitude")

        if pd.isna(lat) or pd.isna(lon):
            continue

        partner_type = str(row.get("partner_type", "")).strip()
        org_name = str(row.get("organization_name", "")).strip()

        color = config.PARTNER_TYPE_COLORS.get(partner_type, "#CCCCCC")
        if partner_type not in config.PARTNER_TYPE_COLORS:
            logger.warning("Unrecognized partner type: %s", partner_type)

        label = config.PARTNER_TYPE_LABELS.get(partner_type, partner_type)

        popup_html = (
            f"<b>{org_name}</b><br>"
            f"{label}<br>"
            f"<i>Nashville Food Project Partner</i>"
        )

        folium.CircleMarker(
            location=[float(lat), float(lon)],
            radius=8,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            popup=folium.Popup(popup_html, max_width=250),
        ).add_to(fg)

    return fg


def build_empty_tract_layer(geojson: dict) -> folium.GeoJson:
    """Build a GeoJson layer showing tract boundaries without choropleth data.

    Popup: 'Census Tract [number] -- Select a data layer to see values.'
    """
    # Inject popup HTML into properties
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        tract_name = props.get("NAME", props.get("GEOID", "Unknown"))
        props["_popup_html"] = (
            f"<b>Census Tract {tract_name}</b><br>"
            f"Select a data layer to see values."
        )

    layer = folium.GeoJson(
        geojson,
        style_function=lambda x: {
            "fillColor": "#EEEEEE",
            "color": "#999999",
            "weight": 0.5,
            "fillOpacity": 0.3,
        },
        highlight_function=lambda x: {"weight": 2, "fillOpacity": 0.5},
        popup=folium.GeoJsonPopup(fields=["_popup_html"], labels=False),
        name="Census Tracts",
    )

    return layer
