"""Classify Giving Matters organizations into NFP partner categories.

Reads ``giving_matters.xlsx``, sends batches to Claude for classification
against the 10 partner categories defined in ``project.yml`` (9 NFP types
plus ``other``), and writes a CSV compatible with the giving_matters
pipeline to ``data/mock/giving_matters.csv``.

Run:
    python scripts/classify_giving_matters.py

Requires ``ANTHROPIC_API_KEY`` in the environment (or ``.env``).
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Literal

import yaml

import anthropic
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = PROJECT_ROOT / "giving_matters.xlsx"
OUTPUT_PATH = PROJECT_ROOT / "data" / "mock" / "giving_matters.csv"
GEOJSON_PATH = PROJECT_ROOT / "data" / "giving_matters.geojson"
PROJECT_YML = PROJECT_ROOT / "project.yml"

BATCH_SIZE = 30
MAX_WORKERS = 5
MODEL_ID = "claude-opus-4-7"

PARTNER_TYPES: list[tuple[str, str, str]] = [
    # ── Food-insecurity core categories ────────────────────────────────────────
    ("community_meals", "Community Meals",
     "Food pantries, meal delivery programs, food banks, community kitchens, "
     "nutrition-assistance nonprofits, gleaning and food-rescue orgs"),
    ("homeless_outreach", "Homeless Outreach",
     "Direct services for people experiencing homelessness, street outreach, day shelters, "
     "homeless-prevention case management"),
    ("transitional_housing", "Transitional Housing",
     "Shelters, transitional or supportive housing, housing stability programs, "
     "domestic-violence safe houses, recovery housing"),
    ("workforce_development", "Workforce Development",
     "Job training, adult education and GED programs, career readiness, "
     "employment support, re-entry job programs"),
    ("medical_health", "Medical & Health Services",
     "Clinics, hospitals, mental-health services, disability services, addiction recovery "
     "and treatment, free medical care, health-focused nonprofits"),
    ("senior_services", "Senior Services",
     "Elderly care, senior centers, aging services, retirement support, hospice for seniors"),
    ("school_summer", "School & Summer Programs",
     "K-12 schools, PTOs/PTAs, school foundations, summer camps and programs for children, "
     "school-system-affiliated nonprofits"),
    ("after_school", "After-School Programs",
     "After-school tutoring, youth enrichment programs, mentoring programs for children and "
     "teens OUTSIDE regular school hours (not a K-12 school itself)"),
    ("community_development", "Community Development",
     "Neighborhood associations, civic groups, community centers, economic development orgs, "
     "community foundations, cultural/identity-based community-building groups"),
    # ── Adjacent categories (displayed on map) ─────────────────────────────────
    ("child_advocacy", "Child Advocacy & CASA",
     "Court-Appointed Special Advocate (CASA) programs, child advocacy centers serving "
     "abuse or neglect victims, guardianship and child welfare nonprofits. These orgs serve "
     "children involved in the court or foster care system, not general youth enrichment. "
     "Use after_school for general youth programs; use child_advocacy only when the org "
     "explicitly works within the court, CASA, or child protective services system."),
    ("legal_advocacy", "Legal Aid & Advocacy",
     "Organizations providing free or low-cost civil legal services to low-income clients "
     "(housing, benefits, immigration, family law). Policy and advocacy orgs working on "
     "economic justice, food access, housing stability, wage policy, immigration rights, "
     "or criminal justice reform. Includes voting rights and civic participation orgs "
     "focused on underserved populations."),
    ("veterans_military", "Veterans & Military Services",
     "Nonprofits providing direct services, benefits navigation, or community support "
     "specifically to veterans, active-duty military, or their families. Includes "
     "memorial and recognition organizations for veterans."),
    ("faith_ministry", "Faith & Ministry",
     "Religious congregations, ministries, and faith-based organizations whose primary "
     "mission is spiritual or religious and that do NOT clearly specialize in food, housing, "
     "health, or another named category. Use community_meals if the org explicitly operates "
     "a food pantry or kitchen. Use transitional_housing if it explicitly operates a shelter. "
     "Only use faith_ministry when no dominant social-service specialization is apparent "
     "from the name."),
    ("environment_conservation", "Environment & Conservation",
     "Land trusts, conservation nonprofits, waterway and greenspace stewardship orgs, "
     "parks advocacy groups (e.g. Friends of [Park]), urban sustainability nonprofits, "
     "environmental policy organizations, transportation and active-mobility nonprofits "
     "(walking, biking, transit advocacy). Includes wildlife and nature education orgs."),
    ("higher_education", "Higher Education",
     "Colleges, universities, and their affiliated foundations or support organizations. "
     "Includes community colleges and their foundations. Does NOT include K-12 schools "
     "(use school_summer) or after-school tutoring programs (use after_school)."),
    # ── Suppressed categories (classified but filtered from map output) ─────────
    ("arts_culture", "Arts & Culture",
     "Performing arts companies (theater, dance, opera, symphony, ballet, choir), "
     "visual art organizations and galleries, music institutions and ensembles, "
     "film festivals, museums, arts education nonprofits, and fiscally-sponsored creative "
     "projects. Does NOT include schools or after-school programs that use arts as a "
     "teaching tool — use school_summer or after_school for those."),
    ("animal_welfare", "Animal Welfare",
     "Animal rescue organizations, shelters and humane societies, wildlife rehabilitation "
     "centers, livestock and farm animal sanctuaries, pet assistance nonprofits. Includes "
     "organizations that provide pet food or veterinary assistance to low-income pet owners."),
    ("sports_recreation", "Sports & Recreation",
     "Athletic clubs, sports leagues and foundations, recreational nonprofits, fitness "
     "organizations, and outdoor recreation groups not primarily focused on youth enrichment "
     "or after-school programming. Includes adult amateur sports, equestrian and specialty "
     "sports organizations, and athletic event foundations."),
    ("historical_preservation", "Historical & Cultural Preservation",
     "Historical societies, preservation nonprofits, heritage foundations, and organizations "
     "that maintain or advocate for historic sites, buildings, cemeteries, or genealogical "
     "records."),
    ("public_media", "Public Media & Broadcasting",
     "Public television and radio stations, nonprofit media organizations, journalism "
     "nonprofits, and film or media production nonprofits with a public-interest mission."),
    # ── True residual ───────────────────────────────────────────────────────────
    ("other", "Other",
     "Use ONLY when no other category applies after careful consideration of all 20 "
     "categories above. This should be a true residual for organizations with unusual "
     "missions that fit no named bucket. Do not use other as a default for vague or "
     "unfamiliar names — assign the closest plausible category instead. "
     "Examples of genuine others: international development foundations with no local "
     "service component, professional associations, trade groups, foundations giving to "
     "a wide and unrelated mix of causes."),
]

SYSTEM_PROMPT = (
    "You classify Nashville-area nonprofit organizations into partner-type categories "
    "for a food-insecurity mapping tool built by the Nashville Food Project (NFP).\n\n"
    "You are given a numbered list of organizations (name, optional city, optional county). "
    "For each, pick EXACTLY ONE category id from the 21 categories listed below, based "
    "primarily on the organization name. Use city and county only as local-context "
    "disambiguators (e.g., to distinguish a food pantry from a similarly named arts org).\n\n"
    "TIEBREAKER RULES — apply in order when an organization fits more than one category:\n"
    "1. If the organization explicitly operates a food pantry, meal program, or food "
    "assistance service, always use community_meals regardless of other characteristics.\n"
    "2. If the organization serves people experiencing homelessness with direct services, "
    "use homeless_outreach over community_development or faith_ministry.\n"
    "3. If the organization is a K-12 school or school foundation, use school_summer "
    "over after_school or community_development.\n"
    "4. If the organization is a CASA program or child advocacy center (abuse/neglect "
    "victims), use child_advocacy over community_development or after_school.\n"
    "5. If the organization is a university, college, or their foundation, use "
    "higher_education over community_development or workforce_development.\n"
    "6. If the organization is faith-based and also runs a shelter, use transitional_housing. "
    "If it runs a food pantry, use community_meals. Only use faith_ministry when the "
    "mission is primarily religious with no dominant social-service function apparent "
    "from the name.\n"
    "7. For all remaining ties, choose the category most relevant to food insecurity "
    "and economic hardship.\n"
    "8. Use other only as a last resort when no category fits after applying all "
    "tiebreakers — not as a default for vague or unfamiliar names.\n\n"
    "Categories:\n"
    + "\n".join(f"- {tid} ({label}): {desc}" for tid, label, desc in PARTNER_TYPES)
    + "\n\nReturn a classifications array with one entry per input index, in order."
)


CATEGORY_IDS = Literal[
    "school_summer", "medical_health", "transitional_housing", "senior_services",
    "community_development", "homeless_outreach", "workforce_development",
    "after_school", "community_meals",
    "child_advocacy", "legal_advocacy", "veterans_military", "faith_ministry",
    "environment_conservation", "higher_education",
    "arts_culture", "animal_welfare", "sports_recreation", "historical_preservation",
    "public_media",
    "other",
]


class Classification(BaseModel):
    """One organization's classification."""

    index: int = Field(description="0-based index from the input list")
    partner_type: CATEGORY_IDS


class ClassificationBatch(BaseModel):
    """The array returned for a batch of organizations."""

    classifications: list[Classification]


def _build_user_message(batch: list[dict]) -> str:
    lines = []
    for i, row in enumerate(batch):
        suffix = ""
        city = (row.get("city") or "").strip()
        county = (row.get("county") or "").strip()
        if city or county:
            bits = [b for b in (city, f"{county} County" if county else "") if b]
            suffix = f"  [{', '.join(bits)}]"
        lines.append(f"[{i}] {row['partner_name']}{suffix}")
    return (
        "Classify the following organizations. Return exactly "
        f"{len(batch)} entries with indices 0..{len(batch) - 1} in order.\n\n"
        + "\n".join(lines)
    )


def _classify_batch(client: anthropic.Anthropic, batch: list[dict]) -> list[str]:
    """Classify one batch; returns a list of partner_type ids aligned to batch order."""
    response = client.messages.parse(
        model=MODEL_ID,
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": _build_user_message(batch)}],
        output_format=ClassificationBatch,
    )
    parsed = response.parsed_output
    mapping = {c.index: c.partner_type for c in parsed.classifications}
    return [mapping.get(i, "other") for i in range(len(batch))]


def _classify_with_retry(client: anthropic.Anthropic, batch: list[dict]) -> list[str]:
    """Retry-safe wrapper around _classify_batch for transient failures."""
    for attempt in range(1, 5):
        try:
            return _classify_batch(client, batch)
        except (anthropic.RateLimitError, anthropic.APIConnectionError,
                anthropic.InternalServerError) as exc:
            print(f"  transient error ({type(exc).__name__}); retry {attempt}/4 ...")
            time.sleep(2 ** attempt)
    # Final attempt, propagate if it fails
    return _classify_batch(client, batch)


def _run_classification(client: anthropic.Anthropic, records: list[dict]) -> list[str]:
    """Classify a list of records; returns labels aligned to records order."""
    batches = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]
    print(f"Classifying via {MODEL_ID} in {len(batches)} batches of up to {BATCH_SIZE}"
          f" (max_workers={MAX_WORKERS})...")

    results: list[str | None] = [None] * len(records)

    def _process(idx: int, batch: list[dict]) -> tuple[int, list[str]]:
        labels = _classify_with_retry(
            client,
            [{"partner_name": r["partner_name"], "city": r.get("city", ""), "county": r.get("county", "")}
             for r in batch],
        )
        return idx, labels

    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_process, i, b): i for i, b in enumerate(batches)}
        for fut in as_completed(futures):
            i, labels = fut.result()
            base = i * BATCH_SIZE
            for j, label in enumerate(labels):
                results[base + j] = label
            done += 1
            if done % 5 == 0 or done == len(batches):
                print(f"  progress: {done}/{len(batches)} batches "
                      f"({done * 100 // len(batches)}%)")

    missing = [i for i, r in enumerate(results) if r is None]
    if missing:
        print(f"WARNING: {len(missing)} rows missing labels; defaulting to 'other'")
        for i in missing:
            results[i] = "other"

    return results  # type: ignore[return-value]


def main() -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY not set (check .env)", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic()

    if INPUT_PATH.exists():
        # --- Mode A: xlsx present (original behavior) ---
        print(f"Reading {INPUT_PATH}...")
        df = pd.read_excel(INPUT_PATH)
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns={
            "Organizations - Name": "partner_name",
            "Organizations - Address": "address",
            "Organizations - Address - City": "city",
            "Organizations - Address - State": "state",
            "County": "county",
        })
        for col in ("partner_name", "address", "city", "state", "county"):
            if col not in df.columns:
                df[col] = ""
            df[col] = df[col].fillna("").astype(str).str.strip()
        df = df[["partner_name", "address", "city", "state", "county"]]
        print(f"Loaded {len(df)} organizations")

        records = df.to_dict(orient="records")
        start = time.monotonic()
        results = _run_classification(client, records)
        elapsed = time.monotonic() - start
        print(f"Classified {len(records)} organizations in {elapsed:.1f}s")

        df["partner_type"] = results
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_PATH, index=False)
        print(f"Wrote {OUTPUT_PATH}")

        print("\nCategory distribution:")
        print(df["partner_type"].value_counts().to_string())

    else:
        # --- Mode B: GeoJSON fallback ---
        print(f"giving_matters.xlsx not found — falling back to {GEOJSON_PATH}")
        if not GEOJSON_PATH.exists():
            print(f"ERROR: {GEOJSON_PATH} not found either", file=sys.stderr)
            sys.exit(1)

        with open(GEOJSON_PATH, encoding="utf-8") as f:
            geojson = json.load(f)
        features = geojson["features"]
        print(f"Loaded {len(features)} features")

        records = [
            {
                "partner_name": feat["properties"].get("partner_name", ""),
                "city": "",
                "county": feat["properties"].get("county", ""),
            }
            for feat in features
        ]

        start = time.monotonic()
        results = _run_classification(client, records)
        elapsed = time.monotonic() - start
        print(f"Classified {len(records)} organizations in {elapsed:.1f}s")

        for feat, label in zip(features, results):
            feat["properties"]["partner_type"] = label

        with open(PROJECT_YML, encoding="utf-8") as f:
            project_cfg = yaml.safe_load(f)
        suppress = set(
            project_cfg
                .get("data_sources", {})
                .get("giving_matters", {})
                .get("suppress_types", [])
        )

        features_out = [f for f in features if f["properties"]["partner_type"] not in suppress]
        geojson["features"] = features_out

        with open(GEOJSON_PATH, "w", encoding="utf-8") as f:
            json.dump(geojson, f, indent=2)

        suppressed_count = len(features) - len(features_out)
        print(f"Wrote {len(features_out)} features ({suppressed_count} suppressed)")

        dist = Counter(feat["properties"]["partner_type"] for feat in features_out)
        suppressed_dist = Counter(
            feat["properties"]["partner_type"]
            for feat in features
            if feat["properties"]["partner_type"] in suppress
        )
        print("\nFinal distribution:")
        for tid, count in sorted(dist.items(), key=lambda x: (x[0] == "other", -x[1])):
            print(f"  {tid}: {count}")
        if suppressed_dist:
            print("\nSuppressed (not written):")
            for tid, count in sorted(suppressed_dist.items(), key=lambda x: -x[1]):
                print(f"  {tid}: {count}")


if __name__ == "__main__":
    main()
