"""The equation: standardized predictors + FA county anchor -> tract FI output.

Pure pandas, no I/O. Input shape is what `predictors.to_standardized()` and
`io.load_fa_county()` produce; output shape matches the dataset schema in
`docs/dataset.md`.

The math (documented fully in `docs/plan.md`):

    FI(t) = FI_county(c) + Σ_k β_k · ( X_k(t) − X̄_k(c) )

where X̄_k(c) is the population-weighted county mean of predictor k, and β_k
is `config.MMG_COEFFICIENTS[k]`. By construction the deviation terms sum to
zero across each county, so the population-weighted tract FI re-aggregates
to the FA county anchor — the invariant that `validate.py` checks.
Clipping to [0, 1] can violate that invariant for any tract that gets
clipped; in practice for TN this is rare.
"""

from __future__ import annotations

import pandas as pd

from . import config


def build_tract_fi(predictors_df: pd.DataFrame, fa_county_df: pd.DataFrame) -> pd.DataFrame:
    """Apply the county-anchored deviation equation to produce tract FI.

    Parameters
    ----------
    predictors_df
        From `predictors.to_standardized()`: tract_geoid, county_fips,
        population, and the 7 standardized predictor columns whose names
        match the keys of `config.MMG_COEFFICIENTS`.
    fa_county_df
        From `io.load_fa_county()`: county_fips, fi_county (0-1 rate).

    Returns
    -------
    DataFrame with columns: geo_id, geo_type, year, county_fips,
    tract_population, food_insecurity_rate, food_insecure_count,
    low_confidence_flag, b14006_placeholder.
    """
    predictor_cols = list(config.MMG_COEFFICIENTS)
    df = predictors_df.copy()

    # Population-weighted county means, aligned back to each tract row via transform.
    county_pop = df.groupby("county_fips")["population"].transform("sum")
    county_means = {
        col: (df[col] * df["population"]).groupby(df["county_fips"]).transform("sum") / county_pop
        for col in predictor_cols
    }

    # Apply the equation: deviations × coefficients, summed, added to the anchor.
    df = df.merge(fa_county_df, on="county_fips", how="left")
    deviation_sum = sum(
        config.MMG_COEFFICIENTS[col] * (df[col] - county_means[col]) for col in predictor_cols
    )
    df["food_insecurity_rate"] = (df["fi_county"] + deviation_sum).clip(0.0, 1.0)

    df["food_insecure_count"] = (
        (df["food_insecurity_rate"] * df["population"]).round().astype("Int64")
    )
    df["tract_population"] = df["population"].round().astype("Int64")
    df["low_confidence_flag"] = df["population"] < config.LOW_CONFIDENCE_POP_THRESHOLD
    # v1: every row is a placeholder until B14006 is pulled (see docs/plan.md).
    df["b14006_placeholder"] = True
    df["geo_id"] = df["tract_geoid"]
    df["geo_type"] = "tract"
    df["year"] = config.YEAR

    output_cols = [
        "geo_id",
        "geo_type",
        "year",
        "county_fips",
        "tract_population",
        "food_insecurity_rate",
        "food_insecure_count",
        "low_confidence_flag",
        "b14006_placeholder",
    ]
    return df[output_cols].reset_index(drop=True)
