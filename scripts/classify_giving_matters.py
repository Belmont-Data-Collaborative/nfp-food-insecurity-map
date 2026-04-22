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

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Literal

import anthropic
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = PROJECT_ROOT / "giving_matters.xlsx"
OUTPUT_PATH = PROJECT_ROOT / "data" / "mock" / "giving_matters.csv"

BATCH_SIZE = 30
MAX_WORKERS = 5
MODEL_ID = "claude-opus-4-7"

PARTNER_TYPES: list[tuple[str, str, str]] = [
    ("school_summer", "School & Summer Programs",
     "K-12 schools, PTOs/PTAs, school foundations, summer camps and programs for children, "
     "school-system-affiliated nonprofits"),
    ("medical_health", "Medical & Health Services",
     "Clinics, hospitals, mental-health services, disability services, addiction recovery "
     "and treatment, free medical care, health-focused nonprofits"),
    ("transitional_housing", "Transitional Housing",
     "Shelters, transitional or supportive housing, housing stability programs, "
     "domestic-violence safe houses, recovery housing"),
    ("senior_services", "Senior Services",
     "Elderly care, senior centers, aging services, retirement support, hospice for seniors"),
    ("community_development", "Community Development",
     "Neighborhood associations, civic groups, community centers, economic development orgs, "
     "community foundations, cultural/identity-based community-building groups"),
    ("homeless_outreach", "Homeless Outreach",
     "Direct services for people experiencing homelessness, street outreach, day shelters, "
     "homeless-prevention case management"),
    ("workforce_development", "Workforce Development",
     "Job training, adult education and GED programs, career readiness, "
     "employment support, re-entry job programs"),
    ("after_school", "After-School Programs",
     "After-school tutoring, youth enrichment programs, mentoring programs for children and "
     "teens OUTSIDE regular school hours (not a K-12 school itself)"),
    ("community_meals", "Community Meals",
     "Food pantries, meal delivery programs, food banks, community kitchens, "
     "nutrition-assistance nonprofits, gleaning and food-rescue orgs"),
    ("other", "Other",
     "Anything that does NOT clearly fit a category above — e.g. arts and culture, animal "
     "welfare, sports/athletics, environmental, advocacy/policy, research, generic religious "
     "organizations without a clear service category, foundations giving to unrelated causes"),
]

SYSTEM_PROMPT = (
    "You classify Nashville-area nonprofit organizations into Nashville Food Project (NFP) "
    "partner-type categories for a food-insecurity mapping tool.\n\n"
    "You are given a numbered list of organizations (name, optional city, optional county). "
    "For each, pick EXACTLY ONE category id from the list below based on the organization "
    "name (use city/county only as local-context disambiguators). If an organization clearly "
    "serves multiple functions, choose the category most relevant to food-insecurity "
    "partnership work. If no category clearly applies, return 'other' — do not force-fit.\n\n"
    "Categories:\n"
    + "\n".join(f"- {tid} ({label}): {desc}" for tid, label, desc in PARTNER_TYPES)
    + "\n\nReturn a classifications array with one entry per input index, in order."
)


CATEGORY_IDS = Literal[
    "school_summer", "medical_health", "transitional_housing", "senior_services",
    "community_development", "homeless_outreach", "workforce_development",
    "after_school", "community_meals", "other",
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


def main() -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY not set (check .env)", file=sys.stderr)
        sys.exit(1)
    if not INPUT_PATH.exists():
        print(f"ERROR: input file not found: {INPUT_PATH}", file=sys.stderr)
        sys.exit(1)

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
    batches = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]
    print(f"Classifying via {MODEL_ID} in {len(batches)} batches of up to {BATCH_SIZE}"
          f" (max_workers={MAX_WORKERS})...")

    client = anthropic.Anthropic()
    results: list[str | None] = [None] * len(records)

    def _process(idx: int, batch: list[dict]) -> tuple[int, list[str]]:
        labels = _classify_with_retry(
            client,
            [{"partner_name": r["partner_name"], "city": r["city"], "county": r["county"]}
             for r in batch],
        )
        return idx, labels

    start = time.monotonic()
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

    elapsed = time.monotonic() - start
    print(f"Classified {len(records)} organizations in {elapsed:.1f}s")

    df["partner_type"] = results
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {OUTPUT_PATH}")

    print("\nCategory distribution:")
    print(df["partner_type"].value_counts().to_string())


if __name__ == "__main__":
    main()
