# Phase 2 Spec — Frontend P1 Fixes
## TNFP Food Access Tool

| Field | Value |
|---|---|
| **Spec ID** | phase2-frontend-p1 |
| **Status** | draft |
| **Date** | 2026-05-28 |
| **Author** | BDAIC / Belmont Data Collaborative |
| **Awaiting approval from** | Tommy |
| **Branch** | `feature/phase2-p1-frontend` |
| **Type** | UI/MAP + Data Layer Metadata |

---

## Context

The Phase 1 prototype ([https://nfp-food-insecurity-map-ecru.vercel.app/](https://nfp-food-insecurity-map-ecru.vercel.app/)) was reviewed by the TNFP Executive Committee, staff, and garden partners in April 2026. The full set of feedback was triaged and prioritized in:

- **Feedback triage:** `tnfp-feedback-triage.md` (in repo root)
- **Prioritized implementation list:** `tnfp-prioritized-implementation-phase-2.md` (in repo root)

This spec covers all P1 frontend items from that list: five UI/MAP quick fixes plus one data-layer-metadata item. These six items share a single branch because they all touch `map.js`, `Map.html`, or `project.yml` only — no backend pipeline changes are needed beyond a minor `export_config.py` update for the data-year field.

A companion spec (`phase2-p1-data-pipeline`) covers the P1 pipeline items (PO Box fix, TNFP hub sites).

---

## Scope

This spec covers the following six P1 items in order of implementation effort (smallest first):

### Item 1 — Label fix: "TNFP" not "NFP"
**Source:** C.J. ("It's 'TNFP' not 'NFP'"). — 1 note. Priority: P1 S.

The sidebar partner toggle chip and all UI-facing references to NFP-as-organization use "NFP" where they should say "TNFP" (The Nashville Food Project). This is a rename of display strings only — CSS class names (`.nfp-green-*`), element IDs (`nfp-count`), variable names, and the project slug are intentionally left unchanged.

**Affected display strings (not CSS/IDs/variables):**
- `Map.html:376` — `NFP Partners` → `TNFP Partners`
- `Map.html:387` — `marks NFP partners` → `marks TNFP partners`
- `map.js:313` — `"NFP Partner"` (affiliation label in marker popup) → `"TNFP Partner"`
- `map.js:404` — `Nearby NFP partners` (detail panel section heading) → `Nearby TNFP partners`
- `map.js:415` — `No NFP partners within 15 miles` (detail panel fallback text) → `No TNFP partners within 15 miles`
- `map.js:557` — `${n} NFP · ${g} Giving Matters` (tooltip on org count badge) → `${n} TNFP · ${g} Giving Matters`
- `map.js:579` — `${visible}/${total} NFP` (count chip text) → `${visible}/${total} TNFP`
- `map.js:638` — `kind: "NFP"` (search result kind badge) → `kind: "TNFP"`

Do **not** change:
- `project.yml` `primary_org: "Nashville Food Project"` — this is correct
- `project.yml` `name: "NFP Food Insecurity Map"` — this is the tool's name, not the org abbreviation
- Any CSS class names, variable names, or element IDs
- The `Map.html` page `<title>` ("NFP Food Insecurity Map — Map") — this is the product name, not the org label

---

### Item 2 — LILA as default layer
**Source:** Exec Cmte, DZ (2 notes). Priority: P1 S.

Change the map's default indicator from `median_household_income` to `lila_flag` (LILA Designation). This better reflects TNFP's expansion strategy, which is LILA-oriented.

**Affected location:** `map.js:71` — `indicator: "median_household_income"` → `indicator: "lila_flag"`

**Edge case to handle:** LILA is tract-only (`granularities: ["tract"]`). If a user switches to ZIP view while `lila_flag` is the active indicator, the map should not silently render blank. The current code renders LILA tracts with `fillOpacity: 0.15` on null values — for ZIP view this means a near-invisible wash over all ZIP polygons, which is confusing.

Required behavior when user switches geo to ZIP with a tract-only indicator active:
- Auto-switch `state.indicator` to `"median_household_income"` (the next best default for ZIP view)
- Re-render the choropleth and legend
- Show a brief status note or console.warn (a `console.warn` is sufficient; no toast required)

This auto-switch logic should live in the geo-toggle `click` handler (currently wired in `Map.html` around line 358 or in a `setupGeoToggle()` function in `map.js` — locate the actual handler and add the guard there).

---

### Item 3 — Sticky tract highlights (bug fix)
**Source:** Added during triage review. Priority: P1 S.

**Observed bug:** After mousing over tracts, some tracts retain a heavier green border after the cursor has moved away. This is most noticeable when moving the mouse quickly across several tracts.

**Root cause:** When the cursor moves quickly between adjacent tracts, Leaflet may fire `mouseover` on the new tract before `mouseout` fires on the old one. The `mouseout` handler (`map.js:241–243`) has a guard `if (layer !== highlightLayer)` that was intended to preserve the click-selected highlight, but the interaction between fast mouse movement and this guard can leave non-selected tracts highlighted.

**Required fix:** Track the currently-hovered layer in a module-level variable `hoveredLayer` (initialized to `null`, declared alongside `highlightLayer` at `map.js:186`). On each `mouseover`, reset the previous `hoveredLayer` before setting the new one; on `mouseout`, clear `hoveredLayer`. The `highlightLayer` guard (for click-selected tracts) must be preserved.

Concretely:
1. Add `let hoveredLayer = null;` near `let highlightLayer;` at `map.js:186`.
2. In `drawChoropleth()`'s `onEachFeature` callback:
   - `mouseover` handler: before setting style on `layer`, if `hoveredLayer` exists and `hoveredLayer !== highlightLayer`, call `choroplethLayer.resetStyle(hoveredLayer)`. Then set `hoveredLayer = layer`.
   - `mouseout` handler: set `hoveredLayer = null` before the existing `highlightLayer` guard.
3. In `drawChoropleth()`, when rebuilding the layer (i.e., when a new `choroplethLayer` is assigned), reset `hoveredLayer = null` so stale references from the previous render don't persist.

---

### Item 4 — Define the "Food Access" score
**Source:** Exec Cmte, DZ, Garden team (3 notes). Priority: P1 S.

Three separate stakeholders asked what the "food access score" or "food access number" means. The confusion stems from:
1. "LILA Designation" is not self-explanatory to non-specialists.
2. The legend caption (`ind.caption`) currently says "USDA low-income + low-access census tracts (tract-only)." — accurate but brief.
3. There is no in-UI explanation of what "low income" and "low access" mean quantitatively.

**Required changes:**

**4a. Expand captions in `project.yml`** for the three LILA indicators so the legend box is self-explanatory:

```yaml
# lila_flag — replace existing caption:
caption: >
  USDA designation: tract where ≥500 residents (or ≥33% of population)
  live >1 mile from a supermarket AND tract median income ≤80% of
  area median. Urban thresholds; rural uses 10-mile cutoff.
  Source: USDA ERS Food Access Research Atlas, 2019.

# lapop1 — replace existing caption:
caption: >
  Number of residents living more than 1 mile (urban) or 10 miles
  (rural) from the nearest supermarket, regardless of income level.
  Source: USDA ERS Food Access Research Atlas, 2019.

# lalowi1 — replace existing caption:
caption: >
  Low-income residents (HH income ≤80% of area median) who also live
  more than 1 mile (urban) or 10 miles (rural) from the nearest
  supermarket. Source: USDA ERS Food Access Research Atlas, 2019.
```

**4b. Add an `info` tooltip or label to LILA indicator rows in the sidebar** (`renderIndicatorList` in `map.js`). When rendering an indicator whose `src === "lila"`, append a small `ⓘ` character or icon after the label. Bind a native `title` attribute to the element set to the first sentence of the caption. This is a low-cost way to expose the explanation on hover without a full modal.

Example HTML fragment to add inside the indicator row for LILA indicators only:
```html
<span title="${ind.caption}" style="font-size:0.7rem;color:var(--ink-400);cursor:help;margin-left:2px;">ⓘ</span>
```

Do **not** add this tooltip to non-LILA indicators (the existing captions are shown in the legend and are sufficient).

---

### Item 5 — Zoom / dot clustering
**Source:** Exec Cmte, DZ (2 notes). Priority: P1 S.

Users report needing to zoom in too far (zoom 14+) before partner dots separate into individual markers. The current `maxClusterRadius: 50` and no `disableClusteringAtZoom` mean that even at zoom 13, dots still cluster.

**Required changes in `map.js::makeClusterGroup` (around line 272):**

1. Add `disableClusteringAtZoom: 13` to both the NFP and Giving Matters cluster groups. This causes all markers to render individually at zoom 13 and above, regardless of density.
2. Reduce `maxClusterRadius` from `50` to `40` for a less aggressive cluster at lower zooms.
3. Add `spiderfyDistanceMultiplier: 1.4` to spread spiderfied markers further apart when the user clicks a cluster (avoids marker overlap when a cluster spiderfies at lower zooms).

After this change, a user at zoom 12 may still see clusters for dense areas (e.g., downtown Davidson County) but at zoom 13 all markers are individual. This is the acceptance threshold.

---

### Item 6 — Data-year labeling
**Source:** PS ("Input the data year(s) in the legend box or data layer list…"). Priority: P1 S.

Users want to know how current each data layer is, per-layer rather than only in the sidebar's global "Data Freshness" section.

The legend box already shows `ind.caption` (which contains year info after Item 4 above), but the **indicator list rows in the sidebar** show only label and unit — no year. The ask is to add the data year to each indicator row so users can scan the list and immediately see data vintage without selecting a layer.

**Required changes:**

**6a. Add a `data_year` field to each indicator in `project.yml`:**

```yaml
# census_acs indicators:
data_year: "2020–2024"   # all four ACS variables

# health_lila indicators:
data_year: "2024"        # all three CDC PLACES variables

# usda_lila indicators:
data_year: "2019"        # all three USDA LILA variables
```

**6b. Update `pipeline/export_config.py`** to pass `data_year` through to `config.json` alongside the existing indicator fields. The indicator loop already iterates over `project.yml` indicator definitions — add `"data_year": var.get("data_year", "")` to the output dict.

**6c. Update `renderIndicatorList` in `map.js`** to show the year in the indicator row. Add a small `<span class="year">` element after `.unit`:

```html
<div class="year">${ind.data_year || ""}</div>
```

Style it in the existing `<style>` block in `Map.html` (within the `.indicator-list .row` rules):
```css
.indicator-list .row .year {
  font-size: 0.68rem;
  font-family: var(--font-mono);
  color: var(--ink-400);
  margin-left: auto;
  flex-shrink: 0;
}
```

The year should be right-aligned in the row, replacing or complementing the existing `.unit` element. If `data_year` is empty, the element renders but is invisible.

**6d.** After updating `project.yml`, run `python -m pipeline --step export` to regenerate `data/config.json`. This is not part of the Claude Code build run — document it in the PR notes.

---

## Acceptance Criteria

Each item is independently verifiable. QA should:
1. Serve the site locally: `python -m http.server 8000`, open `http://localhost:8000/Map.html`
2. Inspect the live site at [https://nfp-food-insecurity-map-ecru.vercel.app/Map.html](https://nfp-food-insecurity-map-ecru.vercel.app/Map.html) after deployment

| # | Item | Acceptance Criterion | How to verify |
|---|------|---------------------|---------------|
| 1.1 | TNFP label | Sidebar org toggle chip reads "TNFP Partners" | Visual inspection |
| 1.2 | TNFP label | Count chip below org toggle shows `N/T TNFP` not `N/T NFP` | Visual inspection |
| 1.3 | TNFP label | Detail panel section heading reads "Nearby TNFP partners" | Click any tract |
| 1.4 | TNFP label | Detail panel fallback reads "No TNFP partners within 15 miles" | Click a rural tract with no partners |
| 1.5 | TNFP label | Marker popup "Source" row reads "TNFP Partner" | Click any green partner dot |
| 1.6 | TNFP label | Search result kind badge for partner search results reads "TNFP" | Type a partner name in search |
| 2.1 | LILA default | Map loads with LILA Designation active (tracts colored rust/green dichotomy) | Load Map.html fresh |
| 2.2 | LILA default | Sidebar indicator list shows LILA Designation row with `.on` class on load | Inspect DOM or visual |
| 2.3 | LILA default | Switching geo to ZIP switches active indicator to Median Household Income | Click "ZIP Codes" geo toggle |
| 2.4 | LILA default | ZIP view shows a choropleth (not a blank or near-invisible wash) | Visual inspection after geo switch |
| 3.1 | Sticky highlights | Mousing over 10+ tracts rapidly leaves no tracts highlighted after cursor leaves | Move mouse quickly across tracts |
| 3.2 | Sticky highlights | Clicking a tract still keeps it highlighted (click-selected highlight is preserved) | Click a tract, move cursor elsewhere |
| 3.3 | Sticky highlights | Clicking a second tract removes highlight from the first | Click two different tracts in sequence |
| 4.1 | Food Access score | LILA Designation legend caption begins with "USDA designation: tract where…" | Select LILA Designation layer |
| 4.2 | Food Access score | Low Access Population legend caption begins with "Number of residents living more than 1 mile…" | Select Low Access Population layer |
| 4.3 | Food Access score | LILA Designation row in sidebar has a `ⓘ` icon after the label | Visual inspection |
| 4.4 | Food Access score | Hovering over the `ⓘ` icon shows a tooltip with the metric explanation | Hover over `ⓘ` on LILA Designation row |
| 5.1 | Clustering | At zoom 13, partner markers render individually (no cluster bubbles) | Zoom to 13, check Davidson County |
| 5.2 | Clustering | At zoom 12, clusters still form but are less aggressive than before (visually fewer giant clusters) | Compare zoom 12 before/after |
| 5.3 | Clustering | Clicking a spiderfied cluster at any zoom shows well-spaced individual markers | Click a cluster at zoom 11 |
| 6.1 | Data-year label | Each indicator row in the sidebar shows a year badge (e.g., "2020–2024", "2024", "2019") | Visual inspection of indicator list |
| 6.2 | Data-year label | Year badge is right-aligned, monospace, and smaller than the indicator label | Visual inspection |
| 6.3 | Data-year label | `data/config.json` indicators array includes `data_year` field after pipeline export | `cat data/config.json \| grep data_year` |

---

## Branch and PR Strategy

**Branch:** `feature/phase2-p1-frontend` off `main`

**Single PR:** Bundle all six items into one PR. They are all small, all touch the same files (map.js, Map.html, project.yml, export_config.py), and are unlikely to conflict. Splitting into six PRs would add review overhead without benefit.

**PR title:** `feat: phase2 P1 frontend fixes (TNFP label, LILA default, sticky hover, food access score, clustering, data-year labels)`

**Do NOT merge to main** until Tommy approves this spec (status: draft → approved).

**Deployment after merge:**
```bash
# After merge to main:
python -m pipeline --step export   # regenerate data/config.json with data_year fields
aws s3 sync data/ s3://nfp-food-insecurity-map-data/current/ --exclude "mock/*"
git push origin main
vercel --prod --scope databelmonts-projects
```

---

## Out of Scope

The following items from the prioritized list are explicitly **not** covered by this spec. Do not implement them in this pass.

| Item | Reason deferred |
|---|---|
| Zip code / county views (P2 M) | Significant UI restructuring; not P1 |
| "Other" category breakdown (P2 M) | Requires classification rubric decision; not P1 |
| LILA Designation vs. Low Income/Low Access filter clarification (P2 S) | Needs content decision; not P1 |
| Magnitude / need delta visualization (P3 L) | Major new feature; out of scope for P1 pass |
| PO Box data issue (P1 M) | Pipeline work; covered in companion spec `phase2-p1-data-pipeline` |
| TNFP hub sites and CAN network partners (P1 M) | New data source; covered in companion spec |
| Back-end data ownership (P1 S) | Non-code organizational deliverable; not a build task |
| Define "partners" and maintenance plan (P1 M) | Non-code organizational deliverable; not a build task |

---

## References

- **Phase 1 prototype:** https://nfp-food-insecurity-map-ecru.vercel.app/
- **Repo:** https://github.com/Belmont-Data-Collaborative/nfp-food-insecurity-map
- **Feedback triage:** `tnfp-feedback-triage.md` (in repo root)
- **Prioritized P1 list:** `tnfp-prioritized-implementation-phase-2.md` (in repo root)
- **CLAUDE.md:** See `CLAUDE.md` (project instructions for Claude Code)
- **Companion spec:** `docs/specs/2026-05-28_food_insecurity_mapping_tool_phase2_spec_v1_data_pipeline.md`
- **Build companion:** `docs/specs/claude_md.md`
