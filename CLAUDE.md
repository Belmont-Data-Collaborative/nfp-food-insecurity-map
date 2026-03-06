# NFP Food Insecurity Map (v3) — Build Instructions

**App ID**: nfp-food-insecurity-map-v3
**Runtime**: streamlit

## Technology Constraints

RUNTIME CONSTRAINT — PYTHON / STREAMLIT APP:
The spec declares runtime "streamlit". This means:
- techStack.frontend MUST be "Streamlit".
- techStack.backend MUST be "Python 3.11".
- techStack.deployment MUST be "Streamlit Community Cloud".
- The app MUST be a Python application using Streamlit as the web framework.
- Entry point MUST be app.py at the project root.
- Source modules MUST live under src/ (e.g. src/config.py, src/data_loader.py).
- Tests MUST live under tests/ using pytest.
- A requirements.txt with pinned dependency versions MUST be included.
- A .env.example documenting all required environment variables MUST be included.

ALLOWED LIBRARIES (pin versions in requirements.txt):
- streamlit, streamlit-folium, folium (for mapping)
- pandas (for data manipulation)
- boto3 (for AWS S3 access)
- geopy (for geocoding via Nominatim)
- python-dotenv (for local .env loading)
- branca (for Folium legends/colormaps)
- pytest, pytest-cov (for testing)
- Any other Python library explicitly mentioned in the spec requirements.

DO NOT USE:
- Node.js, npm, JavaScript frameworks, or any JS build tools.
- Flask, FastAPI, Django, Dash, or any other web framework besides Streamlit.
- Plotly, Mapbox, Google Maps, or any mapping library besides Folium.

ERROR HANDLING PATTERN:
- Define typed exceptions (e.g. DataLoadError, DataSchemaError) in the appropriate module.
- Use st.error() for user-facing errors — never expose Python tracebacks.
- Use st.warning() for non-fatal issues (e.g. geocoding failures).
- Credentials via os.environ with st.secrets fallback (for Streamlit Cloud deployment).

## Required Project Structure

Required directory structure:
- app.py — Streamlit entrypoint (at project root; first Streamlit call must be st.set_page_config())
- src/__init__.py — Package init (empty file)
- src/config.py — Constants source of truth (zero internal imports; check os.environ before st.secrets)
- src/<module>.py — Source modules (define custom exceptions in the module that raises them)
- tests/__init__.py — Test package init (empty file)
- tests/test_<module>.py — pytest test files (test functions that actually exist in source)
- tests/fixtures/ — Test fixture CSV and data files
- requirements.txt — Pinned Python dependencies (include pytest, pytest-cov)
- .env.example — All required environment variables documented
- Optional: scripts/ — Standalone utility scripts (must NOT import from src/)
- Optional: data/ — Data directories (populated by scripts/)

## Contract Rules

Contract type: `moduleContract`

config.py is the SOURCE OF TRUTH for all constants and configuration. The architect teammate must define a moduleContract in CONTRACT.md listing every Python module, public function signature, shared constant, custom exception, and data flow description. All teammates must use EXACT function names and signatures from this contract.

## Code Patterns and Anti-Patterns

- Add "from __future__ import annotations" as the VERY FIRST LINE of every .py file.
- In app.py: "from dotenv import load_dotenv" then "load_dotenv()" BEFORE any src/ imports.
- In app.py: first Streamlit call in main() MUST be st.set_page_config() — no Streamlit calls before it.
- In config.py: _get_secret() MUST check os.environ.get(key) FIRST, then fall back to st.secrets.
  If st.secrets is checked at module import time it crashes with StreamlitAPIException.
- config.py must have ZERO internal imports (no imports from other src/ modules).
- config.py must define ALL user-facing error/warning message string constants (e.g. ERROR_DATA_LOAD).
- Custom exceptions (DataLoadError, DataSchemaError, etc.) belong in the module that raises them, NOT in config.py.
- app.py imports exceptions from the module that defines them (e.g. "from src.data_loader import DataLoadError").
- Every function imported must match the EXACT name and signature from CONTRACT.md.
- CRITICAL: Do NOT rename functions with synonyms (create/build/make) — use the EXACT name from the contract.
- Scripts in scripts/ must be fully standalone — NO imports from src/ modules (src/config.py imports streamlit).
- Do NOT use Node.js, npm, Flask, FastAPI, Django, Dash, or any framework other than Streamlit.

## QA Validation Rules

PYTHON / STREAMLIT QA CHECKS:

CRITICAL META-RULE — ONLY REPORT ACTUAL PROBLEMS:
- Each issue in the issues list MUST require an actual code change to fix.
- Do NOT include issues where the suggested fix is "no fix required" or "implementation is correct".
- If a requirement is correctly implemented, do NOT add it to the issues list at all.
- Every issue you report will be sent to a fixer agent. If there is nothing to fix, do not report it.

IMPORT CONSISTENCY (HIGHEST PRIORITY — #1 cause of Python build failures):
- For every "from src.X import Y" statement, verify Y ACTUALLY EXISTS as a function/class/constant in src/X.py.
- For every "from src.config import Z", verify Z is defined in config.py.
- Flag as HIGH severity if a module imports a name that does not exist in the target module.

COMMON PHANTOM IMPORT PATTERNS TO CHECK:
- app.py imports error MESSAGE CONSTANTS (e.g. ERROR_DATA_LOAD, WARNING_GEOCODE_FAILURES) from config.py
  → Verify these string constants are actually defined in config.py. If not, flag as HIGH.
- app.py imports EXCEPTION CLASSES (e.g. DataLoadError, DataSchemaError) from config.py
  → These should be imported from the module that defines them (usually data_loader.py), NOT config.py.
  → If app.py imports an exception from config.py but it is defined in data_loader.py, flag as HIGH.
  → The fix is to change the import source in app.py, not to move the class.
- Any "from src.X import Y" where Y exists in a DIFFERENT module than X → flag as HIGH.

FUNCTION SIGNATURE AND RETURN TYPE CONSISTENCY:
- If app.py calls load_csv_from_s3(bucket, key), verify data_loader.py defines that function with matching parameters.
- Flag as HIGH severity if call signatures do not match definitions.
- CHECK RETURN TYPE UNPACKING: if any caller does "a, b = some_func()", verify some_func() returns a tuple.
  If the function returns a single object (e.g. GeoJson | None), the caller will crash with "cannot unpack".
  Flag as HIGH severity if a caller unpacks a return value but the function does not return a tuple.

FUNCTION NAME PREFIX CONSISTENCY:
- Check that module-internal function names use consistent prefixes.
  If layer_manager.py defines build_choropleth_layer and build_partner_markers,
  but another module imports create_base_tract_layer — that is a mismatch.
- Flag as HIGH severity if an imported function name uses a different prefix than the actual definition.

CONSTANT NAME EXACT MATCH:
- If config.py defines LAYOUT but app.py imports PAGE_LAYOUT, that will crash.
- For EVERY constant imported from config, verify the EXACT name exists in config.py.
- Common mistake: adding/removing a prefix (PAGE_LAYOUT vs LAYOUT, MAP_ZOOM vs DEFAULT_ZOOM).
- Flag as HIGH severity if a constant name does not match exactly.

REQUIREMENTS COMPLETENESS:
- Every "import X" in source code should have X (or its PyPI name) in requirements.txt.
- Flag as MEDIUM severity if a dependency is missing from requirements.txt.

TEST VALIDITY:
- Test files must import functions that actually exist in the source modules.
- Tests must not reference classes/methods that do not exist.
- Flag as HIGH severity if tests will fail due to import errors.

EXCEPTION HANDLING:
- Custom exceptions (DataLoadError, DataSchemaError, etc.) must be defined before being raised/caught.
- st.error() should be used for user-facing errors in app.py, not bare raise statements.

CONFIG REFERENCES:
- Every config.CONSTANT referenced in code must be defined in config.py.
- Flag as HIGH severity if a constant is referenced but not defined.

STREAMLIT STARTUP ORDER:
- app.py MUST call load_dotenv() BEFORE importing from src/ (so os.environ is populated when config.py loads).
- app.py MUST "import folium" explicitly if it references folium.Map or any folium type.
- config.py _get_secret() MUST check os.environ.get() FIRST, then fall back to st.secrets.
  If st.secrets is called at module import time it triggers Streamlit before set_page_config() and crashes.
- Flag as CRITICAL severity ONLY if config.py calls st.secrets BEFORE checking os.environ (i.e., st.secrets is the first path).
- Flag as CRITICAL severity ONLY if app.py does NOT call load_dotenv() before src imports.
- Flag as HIGH severity if app.py uses folium types without an explicit "import folium".
- IMPORTANT: If the startup order IS correct (load_dotenv first, os.environ checked first in _get_secret), do NOT create an issue for it AT ALL.
  Do NOT include it in the issues list, even to say "no fix required" — that inflates the issue count and breaks the fix loop.
  Only include issues that ACTUALLY NEED FIXING. A correct implementation is not an issue.

GEOCODING ERROR HANDLING:
- geocode_partners() MUST NOT raise exceptions for individual unresolvable addresses.
- It should silently skip addresses where geocoding returns None or throws per-address exceptions.
- The caller counts NaN lat/lon rows to determine failures, then shows st.warning().
- Flag as HIGH severity if geocode_partners() raises GeocodingError from within the per-address loop.
- Flag as HIGH severity if tests expect GeocodingError for unresolvable addresses (tests should check for NaN instead).
- If geocode_partners() correctly catches per-address errors and returns NaN, do NOT create an issue for it.
  A correct implementation is not an issue — only flag things that NEED FIXING.

MAP EXPORT — READ CAREFULLY:
- PNG export is IMPOSSIBLE in pure Streamlit (no headless browser). HTML download is the CORRECT implementation.
- If the code uses st.download_button with map HTML, the export requirement IS FULLY SATISFIED.
- The spec says "Download Map as PNG" but this is a known limitation. HTML export is the approved workaround.
- Do NOT flag HTML export as an issue AT ANY SEVERITY — not critical, not high, not medium, not low.
- Do NOT suggest implementing PNG export, Selenium, or headless browsers.
- Flag ONLY if no download/export functionality exists at all (MEDIUM severity).

STRUCTURED REQUIREMENT VERIFICATION (when structuredRequirements and fileRequirementMap are provided):
- For each file, check the requirements listed in its implementsRequirements.
- For each such requirement, verify every acceptance criterion against the source code.
- Populate requirementCoverage with per-requirement status.
- When an issue maps to a specific requirement, set requirementId and acceptanceCriterion on the issue.

ENVIRONMENT FILE SETUP:
- If .env.example exists but .env does not, flag as HIGH severity.
- The tester should copy .env.example to .env before running the app.
- Without .env, the app cannot read configuration and will crash at runtime.

UTILITY SCRIPT INDEPENDENCE:
- Scripts in scripts/ (e.g. generate_mock_data.py, import_shapefiles.py) must NOT import from src/ modules.
- src/config.py imports streamlit, which triggers ScriptRunContext warnings when run outside the Streamlit app.
- If a script imports from src.config, src.data_loader, or any src/ module, flag as HIGH severity.
- Scripts should define any needed constants locally or read from data files.

UTILITY SCRIPT DATA ASSERTIONS:
- If scripts assert data counts (e.g. assert 200 <= tract_count <= 300), verify the range is realistic.
- Real-world data often differs from spec estimates. A too-narrow range causes script failures.
- Flag as MEDIUM severity if assertion ranges are too tight (e.g. exact match or <20% tolerance).

UTILITY SCRIPT EXECUTION AND DATA COMPLETENESS:
- If scripts/ directory contains .py files (e.g. generate_mock_data.py, import_shapefiles.py), they MUST be executed during testing.
- After running scripts, verify data directories (data/, data/mock/, data/shapefiles/) are NOT empty.
- Flag as CRITICAL severity if scripts exist but their output data directories are empty — the app will crash at runtime with missing data.

SMOKE TEST — APP ACTUALLY RUNS:
- After pytest passes, the tester MUST start the Streamlit app and verify it responds.
- Start: streamlit run app.py --server.port 8599 --server.headless true &
- Verify: curl -s http://localhost:8599 should contain "streamlit" (case-insensitive).
- Flag as CRITICAL severity if the app crashes on startup or does not respond.
- A passing "import app" test and passing pytest do NOT guarantee the app actually runs.

## Fix Guidelines

You are a senior Python engineer fixing bugs found by a QA review.
You are given:
- The refined app spec.
- ALL files in the project with their current contents.
- ALL QA issues found across the project.
- The ONE file you need to fix.

Return the COMPLETE corrected contents of that ONE file.

CRITICAL RULES:
- Respond with the raw file contents ONLY: no markdown fences, no JSON, no commentary.
- Return the COMPLETE file, not a diff.
- Follow PEP8. Use type hints. Add concise docstrings.

DO NOT BREAK CROSS-MODULE CONSISTENCY — THIS IS THE #1 RULE:
- NEVER rename functions, classes, constants, or exceptions that exist in other files.
- NEVER change function signatures (parameter names, order, types) — other files depend on them.
- NEVER change import paths — other files are importing from these exact paths.
- If app.py imports "load_csv_from_s3" from data_loader, that name MUST stay exactly the same.
- If config.py defines BUCKET_NAME, do NOT rename it to S3_BUCKET or anything else.
- Fix ONLY the logic within existing function bodies. Preserve all public interfaces exactly.
- If a fix requires changing a public interface, make the MINIMUM change and document it clearly.

CONSERVATIVE FIXES ONLY:
- Fix the specific QA issues listed. Do NOT refactor unrelated code.
- Do NOT reorganize imports, reorder functions, or rename variables that are not part of the issue.
- Do NOT add new functions or classes unless the QA issue specifically requires it.
- Keep the file as close to the original as possible while fixing the flagged issues.

GEOCODER FIX PATTERN (if fixing geocoder.py):
- geocode_partners() must NEVER raise for individual address failures.
- Catch ALL per-address exceptions inside the loop and silently continue.
- Return (df, cache) with NaN for failed addresses. The caller counts NaN to show warnings.

IMPORT SOURCE FIX PATTERN (if fixing import errors in app.py):
- Constants (ERROR_DATA_LOAD, WARNING_GEOCODE_FAILURES, color maps) → import from src.config.
- Exception classes (DataLoadError, DataSchemaError) → import from the module that DEFINES them
  (usually src.data_loader), NOT from src.config.
- If a constant is missing from config.py, ADD it there (config.py owns all constants).
- If an exception is imported from the wrong module, change the import source in app.py.

TECHNOLOGY: Python / Streamlit only. No Node.js or JS frameworks.

REQUIREMENT-DRIVEN FIXES:
- Use the acceptance criteria to understand what "correct" means.
- When an issue references a requirementId, check the corresponding acceptance criteria.
- Fix the code to satisfy the specific criteria listed, not just the vague issue description.

## Deployment

Strategy: manual
Streamlit apps use manual deployment to Streamlit Community Cloud.
Build output should be a complete, runnable Python project.

## Application Requirements

1. Map: Render an interactive Folium map centered on Davidson County, TN at a zoom level that displays all census tract boundaries without scrolling. Load census tract boundaries from data/shapefiles/davidson_county_tracts.geojson. Use streamlit-folium to render the map. Map height minimum 700px, use_container_width=True.
2. Utility Script - import_shapefiles.py: Download TIGER/Line shapefile from https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_47_tract.zip. Filter to Davidson County (COUNTYFP=037), reproject to EPSG:4326, retain GEOID/NAME/NAMELSAD/geometry. Zero-pad GEOID to 11 chars. Output to data/shapefiles/davidson_county_tracts.geojson. Assert 150-300 tracts (flexible range for real-world data). MUST be standalone — no imports from src/ modules. Dependencies: geopandas, requests, pyogrio.
3. Utility Script - generate_mock_data.py: Generate mock_nfp_partners.csv (30 rows, 2 blank addresses), mock_census_tract_data.csv (one row per GEOID, 3 rows with empty poverty/income), mock_cdc_places_data.csv in data/mock/. Read GEOIDs from the GeoJSON file directly. Partner names: '[Neighborhood] [Type] [Org]' pattern with real Davidson County neighborhoods (Antioch, Bordeaux, Donelson, East Nashville, Germantown, Madison, Napier, North Nashville, Rivergate, Sylvan Park). Addresses: real Nashville streets with DAVIDSON_COUNTY_ZIP_CODES. Partner types: 8 values, weighted school_summer/community_development 20% each. Census: poverty_rate 3-45% (right-skewed via lognormal), income negatively correlated (22000-120000). CDC: DIABETES_CrudePrev 5-22% (mild positive correlation with poverty). --seed flag for reproducibility (default 42). MUST be standalone — no imports from src/ modules. Dependencies: faker, numpy.
4. Local Development Mode: When USE_MOCK_DATA=true, data_loader loads from MOCK_DATA_DIR (data/mock/) instead of S3. No AWS credentials required. Geocode cache from data/mock/mock_geocode_cache.csv or empty. S3 cache writes skipped when mock mode. Add USE_MOCK_DATA and MOCK_DATA_DIR to .env.example with descriptive comments.
5. Data Loading: Fetch from S3 when USE_MOCK_DATA=false: nfp_partners.csv, census_tract_data.csv, cdc_places_data.csv, nfp_partners_geocoded_cache.csv from s3://[BDAIC_BUCKET]/nfp-mapping/. On S3 fetch failure, display 'Map data could not be loaded. Please refresh the page or contact BDAIC support.' and do not render a broken map. Use @st.cache_data for DataFrame loads and @st.cache_resource for boto3 client.
6. GEOID Normalization: Census ACS CSV and CDC PLACES CSV GEOIDs must be zero-padded to exactly 11 characters using str.zfill(11) immediately after loading in data_loader.py. GeoJSON GEOID values must also be normalized. This is critical for choropleth join accuracy.
7. Geocoding: Geocode NFP partner addresses using Nominatim (geopy) with User-Agent 'BDAIC-NFP-Mapping-Tool/1.0 (belmont.edu)'. Append 'Davidson County, TN, USA' to each query. Cache in nfp_partners_geocoded_cache.csv on S3 (or local when mock). Rate limit: 1 request/second (mandatory). Skip blank/unresolvable addresses silently (no exceptions); display 'X partner location(s) could not be mapped due to address lookup errors.' geocode_partners() must NEVER raise for individual address failures — catch all per-address exceptions and continue.
8. Partner Markers: Render each geocoded partner as a Folium CircleMarker colored by partner_type. 8 types with ColorBrewer Set1 colors: school_summer (#E41A1C), medical_health (#377EB8), transitional_housing (#4DAF4A), senior_services (#984EA3), community_development (#FF7F00), homeless_outreach (#A65628), workforce_development (#F781BF), after_school (#999999). Unrecognized types: #CCCCCC (light gray). Log warning for unrecognized types.
9. Partner Popup: On partner marker click, show: organization name (plain text), partner type (plain-English display label from mapping: school_summer='School / Summer Programs', medical_health='Medical & Health Services', transitional_housing='Transitional Housing', senior_services='Senior Services', community_development='Community Development', homeless_outreach='Homeless Outreach', workforce_development='Workforce Development', after_school='After-School Programs'), and 'Nashville Food Project Partner'. Never expose raw field names or database identifiers.
10. Choropleth Layers: Sidebar selectbox labeled 'Background Data Layer' with options: 'None', 'Poverty Rate (Census)', 'Median Household Income (Census)', CDC health indicator (default DIABETES_CrudePrev). DEFAULT selection on first load: 'Median Household Income (Census)' — render choropleth without user interaction. YlOrRd sequential color scale (light=low, dark=high). Join by GEOID (zero-pad to 11 chars). Tracts with no data: neutral gray #EEEEEE with popup 'Data not available for this tract.' Selecting a new layer replaces (not stacks) the previous choropleth.
11. Tract Popup: On census tract click, show: tract ID formatted as 'Census Tract [number]', value of selected choropleth (poverty: '{:.1f}%', income: '${:,.0f}', CDC: '{:.1f}%'), and data vintage year. If no choropleth selected: 'Census Tract [number] — Select a data layer to see values.'
12. Layer Controls: Sidebar checkbox 'Show NFP Partner Locations' (default: checked). Choropleth selectbox replaces previous layer. Legend in bottom-right corner with color scale, min/max values, and plain-English variable name with unit. Legend disappears when 'None' selected. Use branca colormap or custom HTML macro for legend — must not overlap zoom controls.
13. Data Freshness: Persistent sidebar section 'Data Sources' with notice: 'Data sources: Partner locations updated [date]; [Layer] from [source] [year]'. Updates dynamically when choropleth changes. Visible without scrolling on 1920x1080.
14. Export: 'Download Map as HTML' button (PNG export is impossible in pure Streamlit without headless browser — HTML is the approved workaround). Use st.download_button with map HTML from map_obj.get_root().render(). Filename: nfp_map_YYYY-MM-DD.html. On failure, show inline error.
15. Partner Type Legend: Sidebar section 'Partner Type Legend' with all 8 types as colored swatches with plain-English display labels. Colors match markers exactly. Implement as HTML table or st.markdown with inline color boxes.
16. Error Handling: Missing CSV column → 'Partner data is missing required column: [name]. Please check the data file.' via st.error(). Define typed exceptions DataLoadError and DataSchemaError in data_loader.py. No Python tracebacks to users. Use st.error() for fatal errors, st.warning() for non-fatal issues. Zero-row partner CSV → st.warning('No partner locations found in the data file.'). Zero-row census CSV → st.warning('No census data available for Davidson County.'). GEOID join zero matches → st.warning('No data could be matched to map boundaries.').
17. Tech Stack: Python 3.11, Streamlit (min 1.30), Folium (min 0.15.1), streamlit-folium (min 0.18), geopy, boto3, pandas, geopandas, faker, numpy, pyogrio, python-dotenv, branca, pytest, pytest-cov. requirements.txt with pinned versions. All pip, no npm/node.
18. File Structure: app.py (entrypoint), src/__init__.py, src/config.py, src/data_loader.py, src/geocoder.py, src/layer_manager.py, src/map_builder.py, tests/__init__.py, tests/unit/__init__.py, tests/unit/test_data_loader.py, tests/unit/test_geocoder.py, tests/unit/test_map_builder.py, tests/unit/test_layer_manager.py, tests/unit/test_scripts.py, tests/integration/__init__.py, tests/integration/test_full_pipeline.py, tests/fixtures/ (sample_partners.csv, sample_census.csv, sample_cdc_places.csv, sample_geocode_cache.csv), data/shapefiles/, data/mock/ (.gitignore), scripts/import_shapefiles.py, scripts/generate_mock_data.py, .env.example, .gitignore, requirements.txt.
19. Config.py Constants: S3 bucket name (from env), S3 file paths, expected CSV column names, PARTNER_TYPE_COLORS dict, PARTNER_TYPE_LABELS dict, CHOROPLETH_LAYERS list of dicts (id, display_name, csv_column, unit, format_string, data_vintage), DAVIDSON_COUNTY_CENTER coordinates, DEFAULT_ZOOM_LEVEL, DEFAULT_CHOROPLETH_LAYER='median_income', CDC_PLACES_INDICATOR_COLUMN='DIABETES_CrudePrev', NOMINATIM_USER_AGENT. _get_secret() checks os.environ FIRST, then st.secrets fallback.
20. Caching: @st.cache_data for all DataFrame loads and GeoJSON loading. @st.cache_resource for boto3 client and Nominatim geolocator. Pass returned_objects=[] to st_folium() to minimize re-renders. Use st.session_state for UI toggle state. Wrap long operations in st.spinner().
21. Accessibility: All controls have visible text labels. Color never sole info carrier — every choropleth has numeric legend, every partner color has text label. Min 4.5:1 contrast ratio. Use Streamlit st.error/st.warning for screen-reader compatibility. No icon-only buttons without descriptive text.
22. Sidebar Layout: App title 'Nashville Food Project — Food Insecurity Map'. Brief 2-sentence description. Sections in order: Data Layers (checkbox + selectbox), Partner Type Legend, Export, Data Sources. Sections separated by st.divider(). All visible without scrolling on 1920x1080.
23. Audience: Non-technical NFP staff and city policymakers. Self-explanatory interface, plain English only, zero reliance on documentation. Polished civic data product suitable for Mayor's office and foundation funders. Every label in plain English — never variable names, Census field codes, or internal identifiers.

## Full Description

Interactive web map of Davidson County, Tennessee, overlaying Nashville Food Project partner locations with census-tract-level demographic, economic, and health data. Uses Streamlit, Folium, and S3 or mock data. v3 builds from the detailed updated spec with phased build plan, comprehensive testing, and utility scripts for local development without AWS credentials.
