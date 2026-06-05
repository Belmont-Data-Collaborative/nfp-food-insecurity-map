"""Food Insecurity Index pipeline step.

Computes the Tennessee census-tract food-insecurity index in-process using the
vendored calculation modules (`pipeline.food_insecurity`), then adapts the
statewide output to the map's conventions:

    1. run the vendored ETL (ACS profile + Feeding America -> tract FI),
    2. enforce the re-aggregation invariant (fail loud on violation),
    3. rename geo_id -> GEOID and zero-fill to 11 chars,
    4. filter the 95-county statewide result down to the MSA counties,
    5. apply per-variable display multipliers (0-1 rate -> percent),
    6. write data/{output_prefix}_tract.parquet for the export step / frontend.

Unlike the generic S3 loader (`load_source.process_data_source`), this source
is *computed*, so it has its own step here — mirroring how usda_lila has its own
crosswalk step. The vendored calculation stays a clean copy of the source repo;
all map-specific wiring lives in this file.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.food_insecurity import compute, io as fsi_io, predictors, validate

logger = logging.getLogger(__name__)

# Counties whose tracts get clipped to [0, 1] produce tiny non-zero residuals;
# 0.01 is the "real data" tolerance used by the source repo's build_index.
REAGGREGATION_TOLERANCE = 0.01


def _build_county_fips_set(geography: dict[str, Any]) -> set[str]:
    """Build set of 5-char state+county FIPS from geography config."""
    state = geography["state_fips"]
    msa = geography.get("msa_counties")
    if msa:
        return {state + c["fips"] for c in msa}
    return {state + geography.get("county_fips", "")}


def process_food_insecurity(
    source_config: dict[str, Any],
    geography: dict[str, Any],
) -> pd.DataFrame | None:
    """Compute the TN tract food-insecurity index, filter to the MSA, write Parquet.

    Args:
        source_config: food_insecurity_index config from project.yml.
        geography: Geography config with MSA counties.

    Returns:
        DataFrame keyed by GEOID with the index variables, or None on load error.

    Raises:
        AssertionError: if the re-aggregation invariant exceeds tolerance. This
            is intentional — a silently invalid index is worse than a failed build.
    """
    # 1. Run the vendored calculation (reads from bdaic-public-transform).
    try:
        acs_profile = fsi_io.load_acs_profile()
        fa_county = fsi_io.load_fa_county()
    except Exception as exc:
        logger.error("Failed to load food-insecurity inputs from S3: %s", exc)
        return None

    pred_df = predictors.to_standardized(acs_profile)
    logger.info(
        "FI: %d TN tracts read, %d kept after predictor/population NaN drop",
        len(acs_profile), len(pred_df),
    )

    fi_df = compute.build_tract_fi(pred_df, fa_county)
    logger.info("FI: %d statewide TN tracts computed", len(fi_df))

    # 2. Enforce the re-aggregation invariant — fail loud on violation.
    residuals = validate.check_reaggregation(fi_df, fa_county)
    logger.info(
        "FI: max abs re-aggregation residual = %.2e over %d counties",
        residuals["abs_residual"].max(), len(residuals),
    )
    validate.assert_within_tolerance(residuals, tolerance=REAGGREGATION_TOLERANCE)

    # 3. Conform GEOID to the map's 11-char convention.
    fi_df = fi_df.rename(columns={"geo_id": "GEOID"})
    fi_df["GEOID"] = fi_df["GEOID"].astype(str).str.zfill(11)

    # 4. Filter the statewide (95-county) result down to the MSA.
    county_fips_set = _build_county_fips_set(geography)
    before = len(fi_df)
    fi_df = fi_df[fi_df["GEOID"].str[:5].isin(county_fips_set)].copy()
    logger.info("FI: filtered %d -> %d tracts for %d-county MSA",
                before, len(fi_df), len(county_fips_set))

    # 5. Apply per-variable display multipliers (e.g. 0-1 rate -> percent) so the
    #    frontend's "{:.1f}%" formatter (which does not scale) shows the right number.
    var_configs = source_config.get("variables", []) or []
    for var in var_configs:
        col = var["column"]
        mult = var.get("multiplier")
        if mult and col in fi_df.columns:
            fi_df[col] = fi_df[col] * mult
            logger.info("FI: applied multiplier %s to %s", mult, col)

    # 6. Select GEOID + declared variable columns, write Parquet.
    keep = ["GEOID"] + [v["column"] for v in var_configs]
    fi_df = fi_df[[c for c in keep if c in fi_df.columns]]

    output_prefix = source_config.get("output_prefix", "food_insecurity")
    output_path = f"data/{output_prefix}_tract.parquet"
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fi_df.to_parquet(output_path, index=False)
    logger.info("FI: saved %d rows to %s", len(fi_df), output_path)

    return fi_df
