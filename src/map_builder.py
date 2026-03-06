from __future__ import annotations

import folium
import pandas as pd
import streamlit as st

from src import config
from src.layer_manager import (
    build_choropleth_layer,
    build_empty_tract_layer,
    build_partner_markers,
)


def build_base_map() -> folium.Map:
    """Create a base Folium Map centered on Davidson County.

    Uses config.DAVIDSON_COUNTY_CENTER and config.DEFAULT_ZOOM_LEVEL.
    """
    return folium.Map(
        location=list(config.DAVIDSON_COUNTY_CENTER),
        zoom_start=config.DEFAULT_ZOOM_LEVEL,
        tiles="cartodbpositron",
    )


def build_map(
    geojson: dict,
    partners_df: pd.DataFrame | None,
    selected_layer: dict | None,
    show_partners: bool,
) -> folium.Map:
    """Assemble the complete map with optional choropleth and partner markers.

    Args:
        geojson: GeoJSON dict for census tract boundaries.
        partners_df: Geocoded partners DataFrame (may be None if loading failed).
        selected_layer: One of config.CHOROPLETH_LAYERS dicts, or None for 'None' selection.
        show_partners: Whether to show partner markers.

    Returns:
        folium.Map ready for rendering via st_folium.

    Note:
        When selected_layer is not None, the caller must store the appropriate
        data DataFrame in st.session_state["choropleth_data"] before calling
        this function.
    """
    m = build_base_map()

    if selected_layer is not None:
        data_df = st.session_state.get("choropleth_data")
        if data_df is not None and not data_df.empty:
            geojson_layer, colormap = build_choropleth_layer(
                geojson, data_df, selected_layer
            )
            geojson_layer.add_to(m)
            colormap.add_to(m)
        else:
            build_empty_tract_layer(geojson).add_to(m)
    else:
        build_empty_tract_layer(geojson).add_to(m)

    if show_partners and partners_df is not None:
        build_partner_markers(partners_df).add_to(m)

    return m
