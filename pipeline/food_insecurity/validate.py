"""Trust-but-verify gate: re-aggregation invariant of the tract FI output.

Pure pandas, no I/O. The math (`docs/plan.md` Validation §1): for each county,
the population-weighted average of the tract food-insecurity rates must equal
the published Feeding America county rate, modulo the [0, 1] clip applied in
`compute.py`. The deviation terms in the MMG equation sum to zero by
construction, so for any county with NO clipped tracts the residual should
be at float-roundoff scale (~1e-12 at our problem size).

External validation (CDC PLACES tract `FOODINSECU` cross-check) is deferred —
see `docs/backlog.md`.
"""

from __future__ import annotations

import pandas as pd


def check_reaggregation(tract_fi_df: pd.DataFrame, fa_county_df: pd.DataFrame) -> pd.DataFrame:
    """Per-county residual between FA anchor and pop-weighted tract FI.

    Parameters
    ----------
    tract_fi_df
        Output of `compute.build_tract_fi` — must have `county_fips`,
        `tract_population`, `food_insecurity_rate`.
    fa_county_df
        Output of `io.load_fa_county` — `county_fips`, `fi_county`.

    Returns
    -------
    DataFrame with columns county_fips, fi_county_anchor, fi_reaggregated,
    residual, abs_residual, n_tracts, sorted by abs_residual descending.
    """
    weighted = tract_fi_df["food_insecurity_rate"] * tract_fi_df["tract_population"]
    g = tract_fi_df.groupby("county_fips")
    per_county = pd.DataFrame(
        {
            "fi_reaggregated": weighted.groupby(tract_fi_df["county_fips"]).sum()
            / g["tract_population"].sum(),
            "n_tracts": g.size(),
        }
    ).reset_index()

    out = fa_county_df.rename(columns={"fi_county": "fi_county_anchor"}).merge(
        per_county, on="county_fips", how="inner"
    )
    out["residual"] = out["fi_county_anchor"] - out["fi_reaggregated"]
    out["abs_residual"] = out["residual"].abs()
    return out.sort_values("abs_residual", ascending=False).reset_index(drop=True)


def assert_within_tolerance(residuals_df: pd.DataFrame, tolerance: float = 1e-6) -> None:
    """Raise AssertionError if any county's abs_residual exceeds `tolerance`.

    Error message lists the worst 5 offenders so the failure is debuggable
    from the traceback alone. Counties whose tracts hit the [0, 1] clip in
    `compute.py` are NOT auto-excused — for TN this is expected to be rare,
    and a violation worth a human look.
    """
    worst = residuals_df[residuals_df["abs_residual"] > tolerance]
    if worst.empty:
        return
    head = worst.head(5)[
        ["county_fips", "fi_county_anchor", "fi_reaggregated", "residual", "n_tracts"]
    ]
    raise AssertionError(
        f"{len(worst)} of {len(residuals_df)} counties exceed tolerance "
        f"{tolerance:g}. Worst offenders:\n{head.to_string(index=False)}"
    )
