# NFP Food Insecurity Map

Interactive Leaflet map of food-insecurity indicators across the 14-county Nashville MSA, built for the **Nashville Food Project** and **Belmont's Data & AI Collaborative**. A Python data pipeline ingests Census ACS, CDC PLACES, USDA LILA, NFP partners, and CFMT *Giving Matters* nonprofits and emits flat GeoJSON/CSV/JSON files; a pure-static HTML/JS frontend reads them in the browser.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                 # keep USE_MOCK_DATA=true for local dev

# Seed mock inputs
python scripts/generate_mock_data.py
python scripts/generate_mock_parquet.py

# Run the full pipeline (writes to data/)
python -m pipeline

# Preview the static site
python -m http.server 8000
# Open http://localhost:8000
```

## What the pipeline produces

Every successful `python -m pipeline` run populates `data/` with:

| File | Purpose |
|---|---|
| `config.json` | Frontend-consumable derivative of `project.yml` (indicators, palettes, partner types) |
| `tracts.geojson` / `zipcodes.geojson` | Census 2020 boundaries at tract / ZCTA granularity |
| `counties.geojson` / `msa.geojson` | 14 MSA counties and the dissolved MSA boundary |
| `partners.geojson` | NFP partner locations (geocoded via Nominatim) |
| `giving_matters.geojson` | ~1,487 CFMT nonprofits, categorized into NFP partner types by LLM |
| `acs_{tract,zip}.csv` + `.parquet` | ACS median income, poverty rate, population |
| `health_{tract,zip}.csv` + `.parquet` | CDC PLACES diabetes, hypertension, obesity |
| `usda_lila_tract.csv` + `.parquet` | USDA LILA flag, low-access pop, low-income/low-access pop (2010→2020 crosswalked) |

Each source also has a `--step` flag: `python -m pipeline --step census_acs` etc. See `python -m pipeline --help`.

## Adding a new data layer

1. Add a new entry under `data_sources:` in [project.yml](project.yml) (S3 paths, column names) and its display metadata (palette, format string, caption).
2. Run `python -m pipeline --step <new_source>` to produce the Parquet.
3. Run `python -m pipeline --step export` to refresh `data/config.json` and the CSV mirror.
4. Reload the browser — the indicator appears in the sidebar automatically. **Do not edit `map.js`.**

## Deployment

Static hosting only. Deploy `data/` alongside the HTML/JS. Run the pipeline on a schedule (cron / GitHub Action) and publish the resulting `data/` to the same origin.

## See also

- [CLAUDE.md](CLAUDE.md) — developer reference: architecture, invariants, gotchas
- [ASSUMPTIONS.md](ASSUMPTIONS.md) — architectural decisions (LILA crosswalk math, geocoding failure semantics, etc.)
- [project.yml](project.yml) — single source of truth for all config

## Next steps

- **Decouple the pipeline from the application.** The data pipeline and the static frontend should live and deploy independently. The pipeline is a batch ETL job; the frontend is a static site. Keeping them in the same repo and deploy unit couples their release cycles unnecessarily.
- **Pipeline writes directly to S3.** Once decoupled, the pipeline should write all output files (`*.geojson`, `*.csv`, `*.parquet`, `config.json`) to the appropriate S3 location. The frontend fetches from S3 (via CloudFront or a public bucket URL) rather than from a co-deployed `data/` folder.
- **Upgrade ACS data to the 2024 release.** The pipeline currently pulls from the 2023 ACS release. Update `project.yml` and the relevant pipeline step to target the 2024 release.
- **Confirm and upgrade CDC PLACES to the 2025 release.** Verify whether the 2025 PLACES dataset is available in the `public-transform` S3 bucket. If so, update the pipeline to pull the 2025 release instead of 2024.
