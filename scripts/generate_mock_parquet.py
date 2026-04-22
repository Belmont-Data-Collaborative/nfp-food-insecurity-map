"""Generate mock Parquet files from existing mock CSVs for local development.

STANDALONE script — NO imports from src/ modules.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd


def main() -> None:
    mock_dir = Path("data/mock")
    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Census ACS mock -> parquet (tract, plus a stand-in zip copy for mock)
    census_path = mock_dir / "mock_census_tract_data.csv"
    if census_path.exists():
        df = pd.read_csv(census_path)
        df["GEOID"] = df["GEOID"].astype(str).str.zfill(11)

        rename_map = {
            "median_household_income": "DP03_0062E",
            "poverty_rate": "DP03_0119PE",
        }
        df_out = df[["GEOID"]].copy()
        for old_name, new_name in rename_map.items():
            if old_name in df.columns:
                df_out[new_name] = df[old_name]

        rng = np.random.default_rng(42)
        df_out["DP05_0001E"] = rng.integers(1000, 15000, size=len(df_out))

        df_out.to_parquet(out_dir / "acs_tract.parquet", index=False)
        print(f"Saved {len(df_out)} rows to data/acs_tract.parquet")

        df_out.to_parquet(out_dir / "acs_zip.parquet", index=False)
        print(f"Saved {len(df_out)} rows to data/acs_zip.parquet")
    else:
        print(f"WARNING: {census_path} not found")

    # CDC PLACES mock -> parquet (tract + zip stand-in)
    cdc_path = mock_dir / "mock_cdc_places_data.csv"
    if cdc_path.exists():
        df = pd.read_csv(cdc_path)
        df["GEOID"] = df["GEOID"].astype(str).str.zfill(11)

        df_out = df[["GEOID"]].copy()
        if "DIABETES" in df.columns:
            df_out["DIABETES"] = df["DIABETES"]

        rng = np.random.default_rng(43)
        df_out["BPHIGH"] = np.round(rng.uniform(20, 45, size=len(df_out)), 1)
        df_out["OBESITY"] = np.round(rng.uniform(25, 42, size=len(df_out)), 1)

        df_out.to_parquet(out_dir / "health_lila_tract.parquet", index=False)
        print(f"Saved {len(df_out)} rows to data/health_lila_tract.parquet")

        df_out.to_parquet(out_dir / "health_lila_zip.parquet", index=False)
        print(f"Saved {len(df_out)} rows to data/health_lila_zip.parquet")
    else:
        print(f"WARNING: {cdc_path} not found")

    # USDA LILA mock -> parquet (tract only)
    lila_path = mock_dir / "mock_usda_lila_tract.csv"
    if lila_path.exists():
        df = pd.read_csv(lila_path)
        df["GEOID"] = df["GEOID"].astype(str).str.zfill(11)
        df.to_parquet(out_dir / "usda_lila_tract.parquet", index=False)
        print(f"Saved {len(df)} rows to data/usda_lila_tract.parquet")
    else:
        # Synthesize a minimal LILA mock so the exporter has something to export.
        tract_csv = mock_dir / "mock_census_tract_data.csv"
        if tract_csv.exists():
            df = pd.read_csv(tract_csv)
            df["GEOID"] = df["GEOID"].astype(str).str.zfill(11)
            rng = np.random.default_rng(44)
            out = pd.DataFrame({
                "GEOID": df["GEOID"],
                "LILATracts_1And10": rng.choice([0, 1], size=len(df), p=[0.75, 0.25]),
                "lapop1": rng.integers(0, 3500, size=len(df)),
                "lalowi1": rng.integers(0, 1800, size=len(df)),
            })
            out.to_parquet(out_dir / "usda_lila_tract.parquet", index=False)
            print(f"Saved {len(out)} rows to data/usda_lila_tract.parquet (synthesized)")

    print("\nSUCCESS: Mock parquet files generated")


if __name__ == "__main__":
    main()
