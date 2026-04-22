"""Export every data/*.parquet to a matching CSV for the static frontend.

Pipeline stages write Parquet (typed, smaller, faster). The Leaflet
frontend reads CSV via ``fetch()`` + Papa-less parsing, so after every
full pipeline run we mirror each Parquet to a CSV alongside it.

Stem rename for frontend compatibility: ``health_lila_*`` → ``health_*``.
The original column names are preserved.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src import config as app_config
from pipeline.load_source import upload_file_to_s3

logger = logging.getLogger(__name__)

DATA_DIR = Path("data")
STEM_RENAMES = {
    "health_lila_tract": "health_tract",
    "health_lila_zip": "health_zip",
}


def export_all_csv(data_dir: Path = DATA_DIR) -> list[Path]:
    """Mirror every Parquet in ``data_dir`` to a CSV with the same stem.

    Returns the list of CSV paths written.
    """
    if not data_dir.exists():
        logger.warning("data directory %s does not exist — nothing to export", data_dir)
        return []

    written: list[Path] = []
    parquets = sorted(data_dir.glob("*.parquet"))
    if not parquets:
        logger.warning("No *.parquet files in %s — run the pipeline first", data_dir)
        return []

    for parquet in parquets:
        stem = parquet.stem
        out_stem = STEM_RENAMES.get(stem, stem)
        csv_path = data_dir / f"{out_stem}.csv"
        try:
            df = pd.read_parquet(parquet)
        except Exception as exc:
            logger.error("Could not read %s: %s", parquet, exc)
            continue

        # Preserve GEOID as a zero-padded string so the CSV round-trips cleanly
        # in the browser (fetch+parse would otherwise see "00037001700" → 37001700).
        if "GEOID" in df.columns:
            df["GEOID"] = df["GEOID"].astype(str).str.zfill(11)

        df.to_csv(csv_path, index=False)
        written.append(csv_path)
        logger.info("%s → %s (%d rows)", parquet.name, csv_path.name, len(df))
        if app_config.S3_OUTPUT_BUCKET:
            key = f"{app_config.S3_OUTPUT_PREFIX.rstrip('/')}/{csv_path.name}"
            upload_file_to_s3(str(csv_path), app_config.S3_OUTPUT_BUCKET, key)

    logger.info("Exported %d Parquet files to CSV", len(written))
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    export_all_csv()
