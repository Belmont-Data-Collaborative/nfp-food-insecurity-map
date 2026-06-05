"""Vendored TN census-tract food-insecurity index calculation.

This subpackage is a faithful copy of the calculation modules from
`bdaic-food-security-index-census-tract-level-etl` (the index ETL):

    config.py      project constants (MMG coefficients, ACS DP codes, S3 keys)
    io.py          S3 reads for the ACS profile + Feeding America county anchor
    predictors.py  unit normalization (DP percents -> decimals, income -> kUSD)
    compute.py     the county-anchored deviation equation -> tract FI
    validate.py    the re-aggregation invariant gate

PROVENANCE / RE-SYNC
--------------------
These files are vendored (copied), not imported as a package, so the map has
no build-time dependency on the index ETL. The trade-off is drift: changes to
the methodology in the source repo do NOT flow here automatically.

Source repo:  Belmont-Data-and-AI-Collaborative/bdaic-food-security-index-census-tract-level-etl
Vendored at:  v1 methodology (YEAR=2023, MMG 2025 Technical Brief coefficients)

To re-sync after an upstream methodology change, re-copy the four pure-pandas
modules (config, predictors, compute, validate) and the two S3 read functions
in io.py, then re-run `tests/unit/test_food_insecurity_compute.py`.

The map-specific wiring (statewide -> MSA filter, percent scaling, Parquet
output) lives OUTSIDE this subpackage in `pipeline/process_food_insecurity.py`,
so this directory stays a clean mirror of the source.
"""
