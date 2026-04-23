# Operations Guide

Practical reference for deploying, running, and handing off the NFP Food Insecurity Map. If you just want to hack on the code locally, [README.md](README.md) is enough. This document covers what's behind the deploy: S3 buckets, IAM, Vercel, and the out-of-band pipeline.

For architectural context and invariants, see [CLAUDE.md](CLAUDE.md). For design decisions (crosswalk math, geocoding semantics), see [ASSUMPTIONS.md](ASSUMPTIONS.md).

---

## 1. What runs where

| Piece | Where it lives | Who/what runs it |
|---|---|---|
| Static site (HTML/JS/Leaflet) | Vercel, project `nfp-food-insecurity-map` under team `databelmonts-projects` | Served from Vercel's CDN |
| Built data files (geojson/csv/parquet) | `s3://nfp-food-insecurity-map-data/current/` | Vercel's build step downloads these via `scripts/sync-data.mjs` |
| Raw source data (Census ACS, CDC PLACES, USDA LILA, partner CSVs) | `s3://bdaic-public-transform/nfp-mapping/...` | Read by the Python pipeline |
| Python data pipeline | Any machine with AWS creds — laptop, scheduled job, or EC2 | Invoked manually via `python -m pipeline`. Writes locally to `data/`, then the operator uploads to the output bucket |
| Geocoding (partners only) | OpenStreetMap Nominatim (public API, no key) | Called from `pipeline/process_partners.py` during a pipeline run |

There is **no runtime backend** — Vercel serves only the static HTML/JS and the pre-built `data/` files. The Python pipeline runs out of band and does not run during the Vercel build.

**Production URL:** https://nfp-food-insecurity-map-ecru.vercel.app

---

## 2. AWS setup

### 2.1 Buckets

| Bucket | Purpose | Access pattern |
|---|---|---|
| `bdaic-public-transform` | **SOURCE** — raw inputs shared with other Belmont data projects. Under the `nfp-mapping/` prefix: partner CSVs, geocode caches. Census/PLACES/LILA live at their own prefixes (see `s3_prefix:` entries in [project.yml](project.yml)). | Pipeline reads. Do NOT delete or rename. |
| `nfp-food-insecurity-map-data` | **OUTPUT** — dedicated to this project. Served files live under `current/`. | Pipeline writes; Vercel reads. |

The `current/` prefix exists so peer prefixes can be added without disrupting the live deploy:
- `archive/<YYYY-MM-DD>/` — optional point-in-time snapshots before a pipeline rerun (none created yet).
- `staging/` — optional pre-production copy to test a new build before promoting.
- `logs/` — reserved in case S3 access logs are enabled later.

### 2.2 IAM — two separate credential sets

The project involves two distinct "who is doing what" roles. **Do not share keys between them.**

**a) Pipeline operator credentials**
Used when a human (or scheduled job) runs `python -m pipeline` and then uploads the output. Requires:
- `s3:GetObject` and `s3:ListBucket` on `bdaic-public-transform` (source reads)
- `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on `nfp-food-insecurity-map-data` (output writes)

Today these are the operator's own IAM user keys in `~/.aws/credentials`. If a scheduled job takes over, create a dedicated IAM user for it.

**b) Vercel build credentials**
Used only by `scripts/sync-data.mjs` during the Vercel build. Read-only on `nfp-food-insecurity-map-data/current/`.

**Current state:** Dedicated IAM user `nfp-map-vercel-reader` (account `324727022201`), created 2026-04-23. No console access, no group membership. Single inline policy `NfpMapVercelS3Read`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucketCurrentPrefix",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::nfp-food-insecurity-map-data",
      "Condition": {"StringLike": {"s3:prefix": ["current/*", "current/"]}}
    },
    {
      "Sid": "GetObjectsInCurrentPrefix",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::nfp-food-insecurity-map-data/current/*"
    }
  ]
}
```

**History:** Before 2026-04-23 the Vercel build was using the operator's personal admin keys (`pranish.bhagat`, member of the `Administrators` group). That gave the build full S3 + admin-wide access for a read-only job. The dedicated user replaced it.

**Verify the user's current state:**
```bash
aws iam get-user --user-name nfp-map-vercel-reader
aws iam get-user-policy --user-name nfp-map-vercel-reader --policy-name NfpMapVercelS3Read
aws iam list-access-keys --user-name nfp-map-vercel-reader
```

### 2.3 Rotating the Vercel credentials

Rotate at least annually, or immediately on suspected compromise. AWS allows two active access keys per user, so you can stage the new key before removing the old one.

```bash
# 1. Create a second access key on the same user (JSON output — capture it!)
aws iam create-access-key --user-name nfp-map-vercel-reader > new-key.json

# 2. Update Vercel env vars. IMPORTANT: pipe values via file to avoid a
#    trailing newline corrupting the stored secret. Do NOT use `echo` without -n.
vercel env rm AWS_ACCESS_KEY_ID production --scope databelmonts-projects --yes
vercel env rm AWS_SECRET_ACCESS_KEY production --scope databelmonts-projects --yes

printf '%s' "$(jq -r .AccessKey.AccessKeyId new-key.json)" \
  | vercel env add AWS_ACCESS_KEY_ID production --scope databelmonts-projects
printf '%s' "$(jq -r .AccessKey.SecretAccessKey new-key.json)" \
  | vercel env add AWS_SECRET_ACCESS_KEY production --scope databelmonts-projects

# 3. Redeploy and confirm the build succeeds
vercel --prod --scope databelmonts-projects

# 4. After the new key is confirmed working, disable and then delete the old one
aws iam update-access-key --user-name nfp-map-vercel-reader --access-key-id <OLD_AKIA> --status Inactive
# wait a day or two to make sure nothing else breaks, then:
aws iam delete-access-key --user-name nfp-map-vercel-reader --access-key-id <OLD_AKIA>

# 5. Shred the temp file
shred -u new-key.json    # or rm on macOS
```

**Gotcha we hit once:** Piping a secret to `vercel env add` via `echo "$v"` or Python `input=val+"\n"` embeds the trailing newline into the stored value, which the AWS SDK then passes to the Authorization header — the build fails with `ERR_INVALID_CHAR: Invalid character in header content ["authorization"]`. Use `printf '%s'` (no `-n` needed — `%s` doesn't append newline) or pipe from a file written with no trailing newline.

---

## 3. Vercel setup

**Project:** `nfp-food-insecurity-map`
**Team:** `databelmonts-projects` (ID `team_AakWrNVJWWsQjEPgWlPMqXD6`)
**Repo is linked** via `.vercel/project.json` (gitignored). If you clone fresh, either run `vercel link` or copy that file.

### 3.1 Build configuration

From [vercel.json](vercel.json):
```json
{
  "buildCommand": "npm install @aws-sdk/client-s3 && node scripts/sync-data.mjs",
  "installCommand": "",
  "outputDirectory": "."
}
```

Each build installs `@aws-sdk/client-s3` fresh (no `package.json` lockfile — noted as a gap in §6). Then `scripts/sync-data.mjs` downloads every object from `s3://nfp-food-insecurity-map-data/current/` into local `data/`. Vercel then serves the repo root as static files.

### 3.2 Environment variables (Production only)

| Name | Purpose |
|---|---|
| `AWS_ACCESS_KEY_ID` | Vercel build credentials (read-only on output bucket) |
| `AWS_SECRET_ACCESS_KEY` | paired secret |
| `AWS_DEFAULT_REGION` | `us-east-1` |

List them with:
```bash
vercel env ls production --scope databelmonts-projects
```

### 3.3 Deploying

- **Code change (what you just edited):**
  ```bash
  vercel --prod --scope databelmonts-projects
  ```
  This uploads the current working tree — **it does not read from GitHub**. Commit first so git and prod match; GitHub auto-deploy is not wired yet.

- **Data change only (pipeline rerun):** upload the new data to `s3://nfp-food-insecurity-map-data/current/`, then redeploy. Vercel won't detect new S3 files on its own.

- **CLI gotcha:** `vercel link --yes` can fail in non-interactive mode with "missing_scope" even when `--scope` is passed. Workaround: write `.vercel/project.json` manually using `projectId` from `GET https://api.vercel.com/v9/projects/<name>?teamId=<id>`.

---

## 4. Running the pipeline

### 4.1 Local, against real S3

```bash
source .venv/bin/activate
cp .env.example .env
# Edit .env:
#   USE_MOCK_DATA=false
#   AWS_ACCESS_KEY_ID=<pipeline operator creds>
#   AWS_SECRET_ACCESS_KEY=<pipeline operator creds>
#   AWS_BUCKET_NAME=bdaic-public-transform   # source bucket

python -m pipeline
```

The pipeline writes to `data/*.parquet`, `data/*.geojson`, `data/*.csv`, and `data/config.json`. See [CLAUDE.md](CLAUDE.md) for the data contract.

### 4.2 Uploading results to the output bucket

After the pipeline succeeds:
```bash
aws s3 sync data/ s3://nfp-food-insecurity-map-data/current/ --exclude "mock/*"
```

Then redeploy Vercel (§3.3).

### 4.3 Local-only (mock mode)

`USE_MOCK_DATA=true` reads from `data/mock/*.csv` (regenerated via `scripts/generate_mock_data.py` + `scripts/generate_mock_parquet.py`). Useful for UI work without AWS access.

---

## 5. Runbook

### 5.1 "The site is showing old data"

1. Check when the pipeline was last run — look at the modified-timestamp of any file in `s3://nfp-food-insecurity-map-data/current/` with `aws s3 ls`.
2. If the S3 files are fresh but Vercel shows old data: Vercel didn't rebuild. Redeploy with `vercel --prod --scope databelmonts-projects`.
3. If the S3 files are stale: rerun the pipeline (§4.1), upload (§4.2), redeploy.

### 5.2 "Vercel build is failing with S3 errors"

Likely causes, in order of likelihood:
1. **Missing or wrong env vars.** `vercel env ls production --scope databelmonts-projects` — all three AWS vars should be present.
2. **IAM policy too narrow.** The build user needs `s3:ListBucket` on the bucket AND `s3:GetObject` on the `current/` prefix. A 403 in the build log points here.
3. **Wrong bucket or prefix.** `scripts/sync-data.mjs:10-11` is hardcoded — double-check against the actual S3 layout.
4. **AWS region mismatch.** If the bucket is not in `us-east-1`, update `AWS_DEFAULT_REGION` on Vercel.

### 5.3 "A partner is missing from the map"

The partner was probably dropped by the geocoder — Nominatim couldn't resolve the address, so the pipeline sets lat/lon to NaN and skips the feature during GeoJSON writing. Check the pipeline logs for `Geocoding failed for ...`. Fix the source CSV (clean up the address) and rerun.

### 5.4 "I need to add a new indicator"

Follow the "Adding a new data layer" section in [README.md](README.md). Do **not** edit `map.js` constants — they're regenerated from `project.yml`.

### 5.5 "I accidentally deleted files from the S3 output bucket"

They are reproducible — rerun the pipeline and re-upload. If versioning is enabled on the bucket (check with `aws s3api get-bucket-versioning --bucket nfp-food-insecurity-map-data`), restore from a prior version. If not enabled, enabling it is cheap insurance.

---

## 6. Known gaps (pick up when convenient)

None of these block production, but they are the load-bearing items the next operator should know about:

1. **Pipeline can exit 0 with partial output.** `process_data_source()` returns `None` on source-load failure ([pipeline/load_source.py:322-324](pipeline/load_source.py)) and the caller ([pipeline/__main__.py:90-96](pipeline/__main__.py)) does not check. In practice this means: if one source's S3 read fails, the overall pipeline logs the error but continues, and the step's parquet may be missing (and a stale one could get re-uploaded). Mitigation: always scan the pipeline's stderr after a full run, or add an output-validation pass that fails the step if `len(df) == 0`.

2. **No CI.** There is no `.github/workflows/`. Tests exist (`pytest tests/ -v`) but nobody runs them automatically. Adding a PR-level workflow that runs pytest + lint is ~30 minutes of work.

3. **No `package.json` / lockfile for Vercel build.** [vercel.json](vercel.json) runs `npm install @aws-sdk/client-s3` with no version pin — each build pulls whatever's latest. Low-probability breakage but not reproducible. Fix: commit a minimal `package.json` with a pinned SDK version and a `package-lock.json`.

4. **`sync-data.mjs` streams directly to the target path.** If a download fails mid-stream, the partial file sits where the frontend would `fetch()` it. Low-probability (Vercel retries builds that exit non-zero), but writing to a temp file and renaming on success is a one-line fix.

5. **Pipeline is manual.** Out-of-band operator runs. A scheduled GitHub Action (weekly?) that does `python -m pipeline && aws s3 sync ...` would close the loop. README "Next steps" lists this already.

6. **S3 bucket versioning status unknown.** Worth enabling on `nfp-food-insecurity-map-data` for cheap accidental-delete recovery.

**Resolved** (2026-04-23): Vercel build now uses a dedicated least-privilege IAM user (`nfp-map-vercel-reader`) instead of the operator's admin keys — see §2.2.

---

## 7. Onboarding checklist for a new engineer

- [ ] Clone the repo, read [README.md](README.md) + [CLAUDE.md](CLAUDE.md) + this doc.
- [ ] Get added to the `databelmonts-projects` Vercel team (ask current owner).
- [ ] Get AWS pipeline-operator credentials (ask current owner) with access to both buckets.
- [ ] Install Vercel CLI: `npm i -g vercel`, then `vercel login`.
- [ ] Run the pipeline once in mock mode (`USE_MOCK_DATA=true`) to verify local setup.
- [ ] Run the pipeline once against real S3 to verify AWS creds work end-to-end.
- [ ] Do a no-op `vercel --prod --scope databelmonts-projects` to verify deploy access.
- [ ] Read through `pipeline/__main__.py` and one source module (e.g. `pipeline/process_census_acs.py`) to understand the step pattern.
- [ ] Read `map.js` top-to-bottom once — it's ~1k lines and is the single frontend file.
