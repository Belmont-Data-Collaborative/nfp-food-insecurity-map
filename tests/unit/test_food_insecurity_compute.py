"""Tests for the vendored food-insecurity calculation: predictors, compute, validate.

Vendored from bdaic-food-security-index-census-tract-level-etl/tests/test_compute.py,
with imports repointed at the in-repo `pipeline.food_insecurity` subpackage. These
guard the math so the vendored copy stays correct even though it no longer tracks
the source repo automatically (see pipeline/food_insecurity/__init__.py).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline.food_insecurity import compute, predictors, validate
from pipeline.food_insecurity.config import MMG_COEFFICIENTS

# ----- fixtures ------------------------------------------------------------


@pytest.fixture
def simple_county_predictors() -> pd.DataFrame:
    """1 county, 2 equal-pop tracts; only poverty varies.

    Other predictors are equal across tracts, so their weighted means equal
    the tract values and their deviations are zero — only the poverty term
    contributes to the equation, making FI(t) exactly hand-computable.
    """
    return pd.DataFrame(
        {
            "tract_geoid": ["47001000001", "47001000002"],
            "county_fips": ["47001", "47001"],
            "population": [1000.0, 1000.0],
            "poverty": [0.30, 0.10],
            "unemployment": [0.05, 0.05],
            "median_income": [50.0, 50.0],
            "pct_hispanic": [0.05, 0.05],
            "pct_black": [0.20, 0.20],
            "homeownership": [0.60, 0.60],
            "disability": [0.10, 0.10],
        }
    )


@pytest.fixture
def simple_county_fa() -> pd.DataFrame:
    """FA county anchor for the simple_county_predictors fixture."""
    return pd.DataFrame({"county_fips": ["47001"], "fi_county": [0.15]})


# ----- predictors.py -------------------------------------------------------


def test_predictors_unit_conversion() -> None:
    """DP percents are divided by 100; median income by 1000; county_fips derived."""
    raw = pd.DataFrame(
        {
            "tract_geoid": ["47001000001"],
            "DP03_0128PE": [30.0],
            "DP03_0009PE": [5.0],
            "DP03_0062E": [50000.0],
            "DP05_0076PE": [5.0],
            "DP05_0038PE": [20.0],
            "DP04_0046PE": [60.0],
            "DP02_0072PE": [10.0],
            "DP05_0001E": [1000.0],
        }
    )

    out = predictors.to_standardized(raw)
    row = out.iloc[0]

    assert len(out) == 1
    assert row["county_fips"] == "47001"
    assert row["population"] == 1000.0
    assert row["poverty"] == pytest.approx(0.30)
    assert row["unemployment"] == pytest.approx(0.05)
    assert row["median_income"] == pytest.approx(50.0)
    assert row["pct_hispanic"] == pytest.approx(0.05)
    assert row["pct_black"] == pytest.approx(0.20)
    assert row["homeownership"] == pytest.approx(0.60)
    assert row["disability"] == pytest.approx(0.10)


def test_predictors_drops_nan() -> None:
    """Tracts with NaN in any predictor or population are dropped."""
    raw = pd.DataFrame(
        {
            "tract_geoid": ["47001000001", "47001000002"],
            "DP03_0128PE": [30.0, np.nan],
            "DP03_0009PE": [5.0, 5.0],
            "DP03_0062E": [50000.0, 50000.0],
            "DP05_0076PE": [5.0, 5.0],
            "DP05_0038PE": [20.0, 20.0],
            "DP04_0046PE": [60.0, 60.0],
            "DP02_0072PE": [10.0, 10.0],
            "DP05_0001E": [1000.0, 1000.0],
        }
    )

    out = predictors.to_standardized(raw)

    assert len(out) == 1
    assert out.iloc[0]["tract_geoid"] == "47001000001"


# ----- compute.py ----------------------------------------------------------


def test_compute_handworked_tract(
    simple_county_predictors: pd.DataFrame, simple_county_fa: pd.DataFrame
) -> None:
    """FI(T1) = fi_county + β_poverty · (pov(T1) − county_mean).

    With pov(T1)=0.30, county_mean=0.20, β_poverty=0.332, fi_county=0.15:
        FI(T1) = 0.15 + 0.0332 = 0.1832
    All other predictor terms are zero (equal across tracts).
    """
    out = compute.build_tract_fi(simple_county_predictors, simple_county_fa)
    t1 = out.loc[out["geo_id"] == "47001000001"].iloc[0]

    expected = 0.15 + MMG_COEFFICIENTS["poverty"] * (0.30 - 0.20)
    assert t1["food_insecurity_rate"] == pytest.approx(expected, abs=1e-12)


# ----- validate.py ---------------------------------------------------------


def test_validate_invariant_holds_on_fixture(
    simple_county_predictors: pd.DataFrame, simple_county_fa: pd.DataFrame
) -> None:
    """Pop-weighted tract FI per county equals fi_county within roundoff."""
    fi_df = compute.build_tract_fi(simple_county_predictors, simple_county_fa)
    residuals = validate.check_reaggregation(fi_df, simple_county_fa)

    assert residuals["abs_residual"].max() < 1e-10
    validate.assert_within_tolerance(residuals, tolerance=1e-10)


def test_validate_raises_on_violation() -> None:
    """assert_within_tolerance raises an AssertionError naming the county."""
    bad = pd.DataFrame(
        {
            "county_fips": ["47001"],
            "fi_county_anchor": [0.15],
            "fi_reaggregated": [0.10],
            "residual": [0.05],
            "abs_residual": [0.05],
            "n_tracts": [2],
        }
    )

    with pytest.raises(AssertionError, match="47001"):
        validate.assert_within_tolerance(bad, tolerance=1e-6)
