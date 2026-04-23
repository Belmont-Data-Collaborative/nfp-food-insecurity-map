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

Deployed on **Vercel** (team `databelmonts-projects`, project `nfp-food-insecurity-map`). Production alias: https://nfp-food-insecurity-map-ecru.vercel.app.

### How it works

The site is pure static HTML/JS. Vercel's build step runs `scripts/sync-data.mjs`, which mirrors the pre-built `data/` folder from S3 into the deploy:

```
Vercel build:
  npm install @aws-sdk/client-s3
  node scripts/sync-data.mjs      # pulls s3://nfp-food-insecurity-map-data/current/
  serve .
```

The Python pipeline **does not** run during the Vercel build — `sync-data.mjs` only mirrors pre-built files. The pipeline runs separately (locally or as a scheduled job) and uploads its output to `s3://nfp-food-insecurity-map-data/current/`.

### S3 buckets (two, on purpose)

| Bucket | Role | Who reads/writes it |
|---|---|---|
| `bdaic-public-transform` | Raw SOURCE inputs (Census ACS, CDC PLACES, USDA LILA, partner CSVs) | Pipeline reads. See `s3_bucket:` entries in [project.yml](project.yml). |
| `nfp-food-insecurity-map-data` | Built OUTPUT served to the website (under `current/` prefix) | Pipeline writes; Vercel build reads. |

The `current/` prefix namespaces the live deploy. Peer prefixes (e.g. `archive/<date>/`, `staging/`) can be added later without disrupting the served files.

### Redeploying

- **Data changed only:** upload new files to `s3://nfp-food-insecurity-map-data/current/`, then trigger a Vercel redeploy (dashboard button or `vercel --prod --scope databelmonts-projects`).
- **Code changed:** `vercel --prod --scope databelmonts-projects` from the repo root. GitHub auto-deploy is not wired yet — enable via Vercel dashboard → Project → Settings → Git if desired.

### Required Vercel env vars (Production)

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`

These grant read access to `s3://nfp-food-insecurity-map-data/current/` only; they are not the pipeline's credentials. Set once per environment with `vercel env add <NAME> production --scope databelmonts-projects`.

## See also

- [CLAUDE.md](CLAUDE.md) — developer reference: architecture, invariants, gotchas
- [ASSUMPTIONS.md](ASSUMPTIONS.md) — architectural decisions (LILA crosswalk math, geocoding failure semantics, etc.)
- [project.yml](project.yml) — single source of truth for all config

## Next steps

- **Automate the pipeline run.** Today the pipeline is invoked manually and its output uploaded to `nfp-food-insecurity-map-data` out of band. A scheduled GitHub Action (weekly/monthly) that runs `python -m pipeline` and `aws s3 sync data/ s3://nfp-food-insecurity-map-data/current/` would close the loop.
- **Wire GitHub auto-deploy on Vercel.** Each push to `main` should trigger a production deploy. One-time setup in the Vercel dashboard.
- **Upgrade ACS data to the 2024 release.** The pipeline currently pulls from the 2023 ACS release. Update `project.yml` and the relevant pipeline step to target the 2024 release.
- **Confirm and upgrade CDC PLACES to the 2025 release.** Verify whether the 2025 PLACES dataset is available in `bdaic-public-transform`. If so, update the pipeline to pull the 2025 release instead of 2024.
