"""Pipeline CLI entry point.

Usage:
    python -m pipeline                      # Run full pipeline
    python -m pipeline --step geo           # Download geographic data only
    python -m pipeline --step census_acs    # Process Census ACS only
    python -m pipeline --step health_lila   # Process CDC PLACES only
    python -m pipeline --step usda_lila     # Process USDA LILA data
    python -m pipeline --step giving_matters # Process Giving Matters (graceful skip if disabled/missing)
    python -m pipeline --step partners      # Process partner data only
    python -m pipeline --step export        # Export Parquet→CSV + project.yml→config.json (frontend refresh)
    python -m pipeline --inspect census_acs # Inspect a data source
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Add project root to path so we can import src and pipeline modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config_loader import (
    get_data_sources,
    get_geography,
    get_granularities,
    get_partner_config,
)
from src import config
from pipeline.load_source import process_data_source
from pipeline.process_partners import run as run_partners
from pipeline.process_usda_lila import process_usda_lila
from pipeline.process_giving_matters import process_giving_matters
from pipeline.export_csv import export_all_csv
from pipeline.export_config import export_config

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure root logger."""
    logging.basicConfig(
        level=config.LOG_LEVEL,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )


def run_geo_step() -> None:
    """Run geographic data processing script."""
    script = Path("scripts/process_geographic_data.py")
    if not script.exists():
        logger.error("Geographic data script not found: %s", script)
        sys.exit(1)

    logger.info("Running geographic data processing...")
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("Geographic data processing failed:\n%s", result.stderr)
        sys.exit(1)
    logger.info("Geographic data processing complete")
    if result.stdout:
        print(result.stdout)


def run_data_step(source_key: str) -> None:
    """Run a single data source processing step."""
    sources = get_data_sources()
    geography = get_geography()
    granularities = get_granularities()
    gran_lookup = {g["id"]: g for g in granularities}

    if source_key not in sources:
        logger.error("Unknown data source: %s", source_key)
        logger.info("Available sources: %s", list(sources.keys()))
        sys.exit(1)

    source_config = sources[source_key]

    for granularity in ["tract", "zip"]:
        logger.info("Processing %s / %s ...", source_key, granularity)
        gran_config = gran_lookup.get(granularity)
        process_data_source(
            source_key, source_config, geography, granularity,
            granularity_config=gran_config,
        )


def run_usda_lila_step() -> None:
    """Run USDA LILA pipeline step with 2010→2020 crosswalk."""
    sources = get_data_sources()
    geography = get_geography()
    if "usda_lila" not in sources:
        logger.warning("usda_lila not configured in project.yml — skipping")
        return
    process_usda_lila(sources["usda_lila"], geography)


def run_giving_matters_step() -> None:
    """Run the Giving Matters pipeline step (graceful skip when disabled)."""
    sources = get_data_sources()
    if "giving_matters" not in sources:
        logger.info("giving_matters not configured in project.yml — skipping")
        return
    process_giving_matters(
        sources["giving_matters"],
        use_mock=config.USE_MOCK_DATA,
        mock_dir=config.MOCK_DATA_DIR,
    )


def run_partners_step() -> None:
    """Run partner data processing pipeline."""
    partner_config = get_partner_config()
    use_mock = config.USE_MOCK_DATA
    mock_dir = config.MOCK_DATA_DIR

    run_partners(partner_config, use_mock, mock_dir)


def run_export_step() -> None:
    """Refresh frontend artifacts: data/config.json and all Parquet→CSV mirrors.

    Always runs at the tail of a full pipeline so the static site picks up
    schema/config changes in the same run.
    """
    logger.info("Exporting frontend config and CSV mirrors...")
    export_config()
    export_all_csv()


def inspect_source(source_key: str) -> None:
    """Inspect a data source by printing its processed output."""
    import pandas as pd

    for granularity in ["tract", "zip"]:
        sources = get_data_sources()
        if source_key not in sources:
            print(f"Unknown source: {source_key}")
            return

        prefix = sources[source_key].get("output_prefix", source_key)
        path = f"data/{prefix}_{granularity}.parquet"

        if not Path(path).exists():
            print(f"  {path}: not found (run pipeline first)")
            continue

        df = pd.read_parquet(path)
        print(f"\n=== {source_key} / {granularity} ===")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {list(df.columns)}")
        print(df.head())
        print()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="NFP Food Insecurity Map — Data Pipeline"
    )
    parser.add_argument(
        "--step",
        choices=[
            "geo", "census_acs", "health_lila", "usda_lila",
            "giving_matters", "partners", "export",
        ],
        help="Run a specific pipeline step",
    )
    parser.add_argument(
        "--inspect",
        metavar="SOURCE",
        help="Inspect processed data for a source",
    )
    args = parser.parse_args()

    setup_logging()

    if args.inspect:
        inspect_source(args.inspect)
        return

    if args.step:
        if args.step == "geo":
            run_geo_step()
        elif args.step == "partners":
            run_partners_step()
        elif args.step == "usda_lila":
            run_usda_lila_step()
        elif args.step == "giving_matters":
            run_giving_matters_step()
        elif args.step == "export":
            run_export_step()
        else:
            run_data_step(args.step)
        return

    # Full pipeline
    logger.info("Running full pipeline...")
    run_geo_step()

    sources = get_data_sources()
    for source_key in sources:
        if source_key in ("usda_lila", "giving_matters"):
            # These have their own dedicated pipeline steps below.
            continue
        run_data_step(source_key)

    # USDA LILA (separate crosswalk-based pipeline)
    run_usda_lila_step()

    # Giving Matters (graceful skip when disabled / S3 key absent)
    run_giving_matters_step()

    run_partners_step()

    # Always emit frontend artifacts at the tail of a full run.
    run_export_step()

    logger.info("Full pipeline complete")


if __name__ == "__main__":
    main()
