# Phase 2 Spec — Data Pipeline P1 Fixes
## TNFP Food Access Tool

| Field | Value |
|---|---|
| **Spec ID** | phase2-data-pipeline-p1 |
| **Status** | draft |
| **Date** | 2026-05-28 |
| **Author** | BDAIC / Belmont Data Collaborative |
| **Awaiting approval from** | Tommy |
| **Branch** | `feature/phase2-p1-data-pipeline` |
| **Type** | DATA — pipeline + new data source |

---

## Context

The Phase 1 prototype ([https://nfp-food-insecurity-map-ecru.vercel.app/](https://nfp-food-insecurity-map-ecru.vercel.app/)) was reviewed by the TNFP Executive Committee, staff, and garden partners in April 2026. The full set of feedback was triaged and prioritized in:

- **Feedback triage:** `tnfp-feedback-triage.md` (repo root)
- **Prioritized implementation list:** `tnfp-prioritized-implementation-phase-2.md` (repo root)

This spec covers the two P1 DATA items that require backend pipeline work. They are separate from the frontend spec (`phase2-frontend-p1`) because they touch the Python pipeline, partner CSV inputs, and S3 data, whereas the frontend spec is JavaScript-only.

---

## Scope

### Item 7 — PO Box data issue
**Source:** Exec Cmte ("A lot of data is PO Box driven, i.e., downtown"). — 1 note. Priority: P1 M.

**Problem:** Some partner records use PO Box mailing addresses instead of physical street addresses. When Nominatim geocodes a PO Box, it typically resolves to the post office location (often downtown Nashville). This clusters unrelated partners at the same downtown coordinates, creating a misleading density spike and obscuring the true geographic distribution of services.

**Scope of the fix:** Detect PO Box addresses in the partner geocoding step, skip geocoding for those records, and log them as warnings rather than placing them on the map.

**Affected files:**
- `pipeline/process_partners.py` — primary geocoding logic for NFP partners
- `pipeline/process_giving_matters.py` — geocoding logic for Giving Matters records
- `data/mock/` — mock data should be updated if any mock records contain PO Box addresses

**Required changes:**

**7a. Add a PO Box detection helper** (shared logic, can be a module-level function in each processor or a shared utility):

```python
import re

def _is_po_box(address: str) -> bool:
    """Return True if address is a PO Box and should not be geocoded."""
    if not address:
        return False
    return bool(re.search(
        r'\b(p\.?\s*o\.?\s*box|po\s+box|post\s+office\s+box)\b',
        address,
        re.IGNORECASE
    ))
```

**7b. In `pipeline/process_partners.py`**, before the geocoding call for each partner record, check `_is_po_box(address)`. If True:
- Set `lat = None`, `lon = None` for the record (matching the existing behavior for geocoding failures)
- Log a warning: `logger.warning("Skipping PO Box address (will not appear on map): %s — %s", partner_name, address)`
- Do **not** call the geocoding API; do **not** raise an exception

The existing behavior for `lat == None` records is to drop them from the GeoJSON output (confirmed in `CLAUDE.md`: "Per-address failures set lat/lon to NaN; the point feature is dropped in the GeoJSON writer"). That behavior is the correct outcome here — PO Box partners simply do not appear on the map.

**7c. Apply the same check in `pipeline/process_giving_matters.py`** — same logic, same logging pattern.

**7d. Pipeline summary logging:** At the end of each processing step, the pipeline already logs a summary count. Include PO Box skips in that summary:
```
Partners geocoded: 42, failed: 1, skipped (PO Box): 3
```

**7e. Do not add a fallback geocoding strategy** (e.g., trying to strip "PO Box 123" and geocode the city/state). A PO Box record without a physical address carries no useful location information and should remain off the map. If TNFP later provides physical addresses for PO Box partners, those records will geocode correctly on the next pipeline run.

**What this does NOT fix:** Partners that have a legitimate street address geocoded correctly are unaffected. This fix only suppresses the downtown-clustering artifact from PO Box records.

---

### Item 8 — TNFP hub sites and CAN network partners
**Source:** Garden team ("TNFP hub sites, and CAN network partners"). — 1 note. Priority: P1 M.

**Problem:** TNFP hub sites and CAN (Community Agriculture Network) network partners are not currently on the map. These are important to TNFP's expansion strategy and were explicitly requested by the garden team.

**Dependency note:** This item requires TNFP to supply the data (addresses or coordinates for hub sites and CAN partners). The pipeline work can be scoped and implemented against placeholder/mock data, but the actual data must come from TNFP before a production deploy. **Coordinate with Tommy on data delivery before starting this item.**

**Implementation approach:** Add hub sites and CAN network partners as new `partner_type` entries within the existing NFP partners data model, rather than as a separate data source. This is the lowest-friction approach — the frontend already renders all `partner_type` values from `config.json`, and no new fetch paths or map layers are needed.

**Required changes:**

**8a. Add new partner types to `project.yml`** under `partners.types`:

```yaml
hub_site:
  label: "TNFP Hub Site"
  color: "#006064"       # deep teal — visually distinct from existing types
  icon: "map-marker"

can_partner:
  label: "CAN Network Partner"
  color: "#4A148C"       # deep purple — visually distinct
  icon: "leaf"
```

Exact colors and icons are suggestions — Tommy / the team may have brand preferences. The colors must be visually distinguishable from the existing ten partner types in the legend.

**8b. Update the NFP partners CSV on S3** (`s3://bdaic-public-transform/nfp-mapping/partners/nfp_partners.csv`) to include rows for hub sites and CAN partners with `partner_type` set to `hub_site` and `can_partner` respectively. This CSV update is a data operations task, not a code task — it must be done by the team before running the pipeline in production. For local development, add representative mock rows to `data/mock/nfp_partners.csv`.

**8c. Update `scripts/generate_mock_data.py`** to include at least two `hub_site` and two `can_partner` mock records with Nashville-area addresses so local development and test runs include the new types.

**8d. No frontend code changes are needed.** The frontend reads partner types from `config.json` (which is generated from `project.yml`) and renders them automatically. After running `python -m pipeline --step export`, the new types appear in the sidebar org list with their labels and colors.

**8e. Verify the geocoding cache** (`nfp-mapping/partners/geocode_cache.csv` on S3) doesn't need manual updates — hub site and CAN partner addresses will be geocoded on first pipeline run and written to the cache automatically.

**8f. Run `python -m pipeline --step partners && python -m pipeline --step export`** after the CSV is updated to regenerate `data/partners.geojson` and `data/config.json`.

---

## Acceptance Criteria

### Item 7 — PO Box fix

| # | Criterion | How to verify |
|---|-----------|---------------|
| 7.1 | PO Box addresses do not appear on the map as markers | Run pipeline with a partner CSV containing PO Box records; confirm no marker at a post office location |
| 7.2 | A warning is logged for each skipped PO Box | Run pipeline; check stderr for "Skipping PO Box address" messages |
| 7.3 | Partners with real street addresses are unaffected | Confirm existing partner count (non-PO-Box records) stays the same after the change |
| 7.4 | Pipeline summary log includes PO Box skip count | Check pipeline stdout for skip count line |
| 7.5 | Same detection runs for Giving Matters records | Add a PO Box GM record to mock data; confirm it is skipped |
| 7.6 | Regex matches common PO Box formats | Unit test: `P.O. Box 123`, `PO Box 123`, `P O Box 456`, `Post Office Box 789` all return True; `123 Main St` returns False |

To test locally with mock data:
```bash
# Add a PO Box entry to data/mock/nfp_partners.csv, then:
python -m pipeline --step partners
# Verify no downtown-cluster artifact in data/partners.geojson
```

### Item 8 — TNFP hub sites and CAN partners

| # | Criterion | How to verify |
|---|-----------|---------------|
| 8.1 | `config.json` includes `hub_site` and `can_partner` entries in `partner_types` | `grep hub_site data/config.json` |
| 8.2 | Sidebar org list displays "TNFP Hub Site" and "CAN Network Partner" rows with correct colors | Load Map.html; visual inspection of Organizations section |
| 8.3 | Hub site markers render on the map with the correct color | Enable hub_site filter; zoom to Nashville |
| 8.4 | CAN partner markers render on the map with the correct color | Enable can_partner filter; zoom to Nashville |
| 8.5 | Marker popup shows correct `partner_type` label (not the key `hub_site`) | Click a hub site marker; check "Category" row in popup |
| 8.6 | Clicking a hub site marker in the org count still filters correctly | Toggle hub_site checkbox off; confirm hub site dots disappear |
| 8.7 | Mock data includes at least 2 hub_site and 2 can_partner records | Check `data/mock/nfp_partners.csv` |

---

## Branch and PR Strategy

**Branch:** `feature/phase2-p1-data-pipeline` off `main`

**Separate from the frontend branch** — these changes touch Python pipeline files and S3 data. Keeping them separate avoids mixing backend and frontend review concerns, and allows them to be merged independently (e.g., Item 7 can ship before Item 8 if hub site data isn't ready yet).

**Two commits or sub-branches are acceptable** within this branch (one for each item), but a single PR is fine if both items are ready together.

**PR title:** `feat: phase2 P1 data pipeline — PO Box skip, TNFP hub sites + CAN partners`

**Do NOT merge to main** until Tommy approves this spec (status: draft → approved) AND (for Item 8) TNFP has delivered the hub site and CAN partner address data.

**Deployment after merge:**
```bash
# After CSV update on S3 and code merge:
python -m pipeline --step partners
python -m pipeline --step export
aws s3 sync data/ s3://nfp-food-insecurity-map-data/current/ --exclude "mock/*"
git push origin main
vercel --prod --scope databelmonts-projects
```

---

## Out of Scope

| Item | Priority | Reason deferred |
|---|---|---|
| Update LILA / USDA data to post-2019 | P2 M | Requires sourcing newer data; no replacement confirmed |
| Distinguish TNFP vs. non-TNFP community meal sites | P2 L | Classification rubric work needed first |
| Food pantry / food bank data (Second Harvest) | P3 L | Requires outside data sourcing |
| Add food access points (gardens, markets, grocery) | P3 L | Requires outside data sourcing; CJ to send sources |
| TNCOMMGARD Mutual Aid Map | P3 M | Accuracy unverified; requires evaluation |
| Public transit data (WeGo) | P3 L | Requires outside data sourcing |
| All UI/MAP P1 items | P1 S | Covered in companion frontend spec |
| Back-end data ownership | P1 S | Non-code organizational deliverable |
| Define "partners" and maintenance plan | P1 M | Non-code organizational deliverable |

---

## Open Questions (resolve before implementation)

| # | Question | Owner |
|---|----------|-------|
| Q1 | Can TNFP provide a CSV or spreadsheet of hub site addresses for Item 8? | Tommy |
| Q2 | Can TNFP provide a CSV or spreadsheet of CAN network partner addresses? | Tommy |
| Q3 | Are there preferred brand colors for hub site and CAN markers (vs. the suggestions in 8a)? | Tommy |
| Q4 | Should hub sites appear as a separate visual layer (larger icons) vs. same-size as regular partners? | Tommy |
| Q5 | How many partner records in the current CSV use PO Box addresses? (Run `grep -i "po box" nfp_partners.csv` on the S3 copy to estimate scope before Item 7.) | BDAIC |

---

## References

- **Phase 1 prototype:** https://nfp-food-insecurity-map-ecru.vercel.app/
- **Repo:** https://github.com/Belmont-Data-Collaborative/nfp-food-insecurity-map
- **Feedback triage:** `tnfp-feedback-triage.md` (repo root)
- **Prioritized P1 list:** `tnfp-prioritized-implementation-phase-2.md` (repo root)
- **CLAUDE.md:** `CLAUDE.md` (project instructions for Claude Code)
- **Companion spec (frontend):** `docs/specs/2026-05-28_food_insecurity_mapping_tool_phase2_spec_v1_frontend.md`
- **Build companion:** `docs/specs/claude_md.md`
- **Pipeline geocoding:** `pipeline/process_partners.py`, `pipeline/process_giving_matters.py`
- **Partner types config:** `project.yml` → `partners.types`
- **Mock data generator:** `scripts/generate_mock_data.py`
