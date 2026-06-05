"""Unit-normalization layer: DP-coded ACS frame -> MMG-equation-ready predictors.

The MMG 2025 Technical Brief estimates coefficients against rate predictors
expressed as decimals (0-1) and median income expressed in thousands of dollars
(kUSD). The profile file gives us percents on a 0-100 scale and income in raw
dollars; this module rescales so `compute.py` can plug values directly into the
equation.

Unit conventions (sanity-checked against the published Davidson 2023 county
anchor of 0.148; if results come out wrong, this is the first knob to turn):
    poverty, unemployment, pct_hispanic, pct_black, homeownership, disability:
        DP percent / 100  ->  0-1 rate
    median_income:
        DP dollars / 1000 ->  kUSD
    population:
        unchanged (only used as a weighting denominator, not in the equation)

Rows with any missing predictor or population are dropped — they cannot be
computed. Low-population (<500) tracts are NOT dropped here; that gating
happens in `compute.py` via the low_confidence_flag so the rows still appear
in the output.
"""

from __future__ import annotations

import pandas as pd

from . import config

# Divisor to apply to each raw DP column to get MMG-equation-ready units.
_DIVISORS = {
    "poverty": 100.0,
    "unemployment": 100.0,
    "median_income": 1000.0,
    "pct_hispanic": 100.0,
    "pct_black": 100.0,
    "homeownership": 100.0,
    "disability": 100.0,
}


def to_standardized(acs_profile_df: pd.DataFrame) -> pd.DataFrame:
    """Convert a raw DP-coded ACS frame into the standardized predictor frame.

    Parameters
    ----------
    acs_profile_df
        The DataFrame returned by `io.load_acs_profile()` — has `tract_geoid`
        plus the 8 raw DP columns named in `config.PREDICTOR_COLUMNS` and
        `config.POPULATION_COLUMN`.

    Returns
    -------
    DataFrame with columns: tract_geoid, county_fips, population, poverty,
    unemployment, median_income, pct_hispanic, pct_black, homeownership,
    disability. Rate predictors are 0-1 decimals; median_income is in kUSD.
    Rows with NaN in any predictor or in population are dropped.
    """
    out = pd.DataFrame({"tract_geoid": acs_profile_df["tract_geoid"]})
    out["county_fips"] = out["tract_geoid"].str[:5]
    out["population"] = acs_profile_df[config.POPULATION_COLUMN]

    for predictor, dp_code in config.PREDICTOR_COLUMNS.items():
        out[predictor] = acs_profile_df[dp_code] / _DIVISORS[predictor]

    drop_subset = list(config.PREDICTOR_COLUMNS) + ["population"]
    return out.dropna(subset=drop_subset).reset_index(drop=True)
