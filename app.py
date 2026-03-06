from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()  # MUST be before any src/ imports

import streamlit as st
import folium  # explicit import for folium type references
from streamlit_folium import st_folium
import pandas as pd
from datetime import date

from src import config
from src.data_loader import (
    DataLoadError,
    DataSchemaError,
    load_geojson,
    load_partners,
    load_census,
    load_cdc_places,
    load_geocode_cache,
    save_geocode_cache,
)
from src.geocoder import geocode_partners
from src.map_builder import build_map


def _build_partner_type_legend_html() -> str:
    """Build HTML for the partner type legend with colored swatches."""
    rows = []
    for ptype, label in config.PARTNER_TYPE_LABELS.items():
        color = config.PARTNER_TYPE_COLORS.get(ptype, "#CCCCCC")
        rows.append(
            f'<tr><td style="padding:2px 6px;">'
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background-color:{color};border-radius:50%;margin-right:6px;'
            f'vertical-align:middle;"></span>'
            f'</td><td style="padding:2px 4px;vertical-align:middle;">{label}</td></tr>'
        )
    return (
        '<table style="font-size:13px;line-height:1.6;">'
        + "".join(rows)
        + "</table>"
    )


def _get_data_source_text(selected_layer_config: dict | None) -> str:
    """Build the data sources notice text."""
    today_str = date.today().strftime("%B %Y")
    base = f"Partner locations updated {today_str}"
    if selected_layer_config is not None:
        name = selected_layer_config["display_name"]
        vintage = selected_layer_config["data_vintage"]
        return f"Data sources: {base}; {name} from {vintage}."
    return f"Data sources: {base}."


def main() -> None:
    """Streamlit application entry point."""
    st.set_page_config(
        page_title="Nashville Food Project \u2014 Food Insecurity Map",
        layout="wide",
    )

    # ------------------------------------------------------------------
    # Sidebar
    # ------------------------------------------------------------------
    with st.sidebar:
        st.title("Nashville Food Project")
        st.markdown(
            "Interactive map of Davidson County overlaying NFP partner locations "
            "with census-tract-level demographic, economic, and health data. "
            "Explore food insecurity patterns across Nashville."
        )

        st.divider()

        # --- Data Layers section ---
        st.subheader("Data Layers")

        show_partners = st.checkbox("Show NFP Partner Locations", value=True)

        layer_options = ["None"] + [
            layer["display_name"] for layer in config.CHOROPLETH_LAYERS
        ]
        # Default to Median Household Income
        default_idx = 0
        for i, layer in enumerate(config.CHOROPLETH_LAYERS):
            if layer["id"] == config.DEFAULT_CHOROPLETH_LAYER:
                default_idx = i + 1  # +1 because "None" is index 0
                break

        selected_layer_name = st.selectbox(
            "Background Data Layer",
            options=layer_options,
            index=default_idx,
        )

        st.divider()

        # --- Partner Type Legend ---
        st.subheader("Partner Type Legend")
        st.markdown(_build_partner_type_legend_html(), unsafe_allow_html=True)

        st.divider()

    # ------------------------------------------------------------------
    # Determine selected layer config
    # ------------------------------------------------------------------
    selected_layer_config: dict | None = None
    if selected_layer_name != "None":
        for layer in config.CHOROPLETH_LAYERS:
            if layer["display_name"] == selected_layer_name:
                selected_layer_config = layer
                break

    # ------------------------------------------------------------------
    # Load Data
    # ------------------------------------------------------------------
    try:
        with st.spinner("Loading map data..."):
            geojson = load_geojson(config.GEOJSON_PATH)

            partners_df = load_partners(config.USE_MOCK_DATA, config.MOCK_DATA_DIR)
            if partners_df.empty:
                st.warning(config.WARNING_NO_PARTNERS)

            census_df = load_census(config.USE_MOCK_DATA, config.MOCK_DATA_DIR)
            if census_df.empty:
                st.warning(config.WARNING_NO_CENSUS)

            cdc_df = load_cdc_places(config.USE_MOCK_DATA, config.MOCK_DATA_DIR)

            cache_df = load_geocode_cache(config.USE_MOCK_DATA, config.MOCK_DATA_DIR)

    except DataSchemaError as exc:
        st.error(str(exc))
        return
    except DataLoadError:
        st.error(config.ERROR_DATA_LOAD)
        return

    # ------------------------------------------------------------------
    # Geocode Partners
    # ------------------------------------------------------------------
    geocoded_df: pd.DataFrame | None = None
    if not partners_df.empty:
        with st.spinner("Geocoding partner addresses..."):
            geocoded_df, updated_cache = geocode_partners(partners_df, cache_df)
            save_geocode_cache(updated_cache, config.USE_MOCK_DATA, config.MOCK_DATA_DIR)

            # Count failures
            if geocoded_df is not None:
                nan_count = geocoded_df["latitude"].isna().sum()
                if nan_count > 0:
                    st.warning(
                        config.WARNING_GEOCODE_FAILURES.format(count=int(nan_count))
                    )

    # ------------------------------------------------------------------
    # Set choropleth data in session state for build_map
    # ------------------------------------------------------------------
    if selected_layer_config is not None:
        col = selected_layer_config["csv_column"]
        # Determine which DataFrame to use
        if col in ("poverty_rate", "median_household_income"):
            choropleth_data = census_df
        else:
            choropleth_data = cdc_df

        # Check GEOID join
        geojson_geoids = {
            str(f["properties"].get("GEOID", "")).zfill(11)
            for f in geojson.get("features", [])
        }
        data_geoids = set(choropleth_data["GEOID"].astype(str).str.zfill(11))
        if not geojson_geoids & data_geoids:
            st.warning(config.WARNING_GEOID_MISMATCH)

        st.session_state["choropleth_data"] = choropleth_data
    else:
        st.session_state["choropleth_data"] = None

    # ------------------------------------------------------------------
    # Build and Render Map
    # ------------------------------------------------------------------
    m = build_map(
        geojson=geojson,
        partners_df=geocoded_df,
        selected_layer=selected_layer_config,
        show_partners=show_partners,
    )

    st_folium(m, use_container_width=True, height=700, returned_objects=[])

    # ------------------------------------------------------------------
    # Sidebar: Export and Data Sources (after map is rendered)
    # ------------------------------------------------------------------
    with st.sidebar:
        # --- Export ---
        st.subheader("Export")
        try:
            map_html = m.get_root().render()
            filename = f"nfp_map_{date.today().isoformat()}.html"
            st.download_button(
                label="Download Map as HTML",
                data=map_html,
                file_name=filename,
                mime="text/html",
            )
        except Exception:
            st.error("Could not generate map export. Please try again.")

        st.divider()

        # --- Data Sources ---
        st.subheader("Data Sources")
        st.markdown(_get_data_source_text(selected_layer_config))


if __name__ == "__main__":
    main()
