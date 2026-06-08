"""Keyword-based reclassification of 'other' orgs in giving_matters.geojson.

Reads the existing data/giving_matters.geojson, re-classifies features whose
partner_type is 'other' using pattern rules (no API key required), applies the
suppress_types filter from project.yml, and re-writes the GeoJSON in-place.

Run:
    python scripts/reclassify_other.py
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GEOJSON_PATH = PROJECT_ROOT / "data" / "giving_matters.geojson"
PROJECT_YML = PROJECT_ROOT / "project.yml"


def _match(name: str, patterns: list[str]) -> bool:
    n = name.lower()
    return any(re.search(p, n) for p in patterns)


# Rules are checked in order; first match wins.
# Suppressed categories are included so they get named before being dropped.
RULES: list[tuple[str, list[str]]] = [

    # ── Child Advocacy & CASA ─────────────────────────────────────────────────
    ("child_advocacy", [
        r"\bcasa\b",
        r"child advocacy center",
        r"guardian ad litem",
        r"davis house",
    ]),

    # ── Legal Aid & Advocacy ──────────────────────────────────────────────────
    ("legal_advocacy", [
        r"legal aid",
        r"legal services",
        r"innocence project",
        r"fair housing",
        r"immigration advocate",
        r"living wage",
        r"justice for our neighbors",
        r"bail fund",
        r"tennessee justice center",
        r"league of women voters",
        r"tennessee alliance for legal",
        r"mid-south immigration",
        r"choosing justice",
        r"foundation for justice.*freedom.*mercy",
        r"tennessee voices for victims",
        r"silent no longer",
        r"everybody vs racism",
        r"tennesseans for alternatives to the death penalty",
        r"nashville conflict resolution",
        r"nashville community bail",
        r"nashville peace",
        r"nashville peacemakers",
        r"nashville living wage",
        r"stand up nashville",
        r"thinkten",
        r"sycamore institute",
        r"tennessee fair housing",
        r"voices for a safer",
        r"tennessee innocence",
        r"tennessee justice for our neighbors",
        r"civic tn",
    ]),

    # ── Veterans & Military ───────────────────────────────────────────────────
    ("veterans_military", [
        r"\bveteran",
        r"\bmilitary\b",
        r"wounded warrior",
        r"honor flight",
        r"orange hat medal",
        r"orange heart medal",
        r"wear blue",
        r"united service organizations",
        r"\busos?\b",
        r"fallen heroes.*camp",
        r"mtsu charlie and hazel daniels",
        r"strength for service",
        r"songwriting.?with.?soldiers",
    ]),

    # ── Public Media (before arts_culture to avoid overlap) ───────────────────
    ("public_media", [
        r"\bwfsk\b",
        r"\bwmot\b",
        r"nashville pbs",
        r"nashville public radio",
        r"nashville education community.*television",
        r"states newsroom",
        r"tennessee lookout",
    ]),

    # ── Higher Education ──────────────────────────────────────────────────────
    ("higher_education", [
        r"\buniversity\b",
        r"vanderbilt",
        r"\bfisk\b",
        r"\blipscomb\b",
        r"belmont university",
        r"aquinas college",
        r"cumberland university",
        r"welch college",
        r"williamson christian college",
        r"volunteer state college",
        r"american baptist theological",
        r"disciples divinity",
        r"middle tennessee state",
        r"\bmtsu\b",
        r"tennessee state university foundation",
        r"middle tennessee state university foundation",
        r"volunteer state.*foundation",
    ]),

    # ── Environment & Conservation ────────────────────────────────────────────
    ("environment_conservation", [
        r"conservancy",
        r"\bconservation\b",
        r"land trust",
        r"greenway",
        r"watershed",
        r"cumberland river compact",
        r"environmental council",
        r"environmental law center",
        r"southern environmental",
        r"tennessee environmental",
        r"wild ones",
        r"naturalist program",
        r"walk.?bike",
        r"transit alliance",
        r"urban green lab",
        r"green interchange",
        r"tree conservation",
        r"tree foundation",
        r"tenn.?green",
        r"scenic tennessee",
        r"tennessee wildlife federation",
        r"tennessee wildlife resources foundation",
        r"tennessee women in green",
        r"larkspur conservation",
        r"owl.s hill nature",
        r"network for sustainable",
        r"friends of.*park",
        r"friends of.*lake",
        r"friends of.*state park",
        r"friends of.*river",
        r"friends of.*trail",
        r"friends of.*woods",
        r"friends of.*bottoms",
        r"friends of.*creek",
        r"friends of radnor",
        r"friends of the warner",
        r"friends of shelby",
        r"centennial park conservancy",
        r"greenways for nashville",
        r"creative parks nashville",
        r"trails please",
        r"metro nashville parks foundation",
        r"transportation management association",
    ]),

    # ── Arts & Culture ────────────────────────────────────────────────────────
    ("arts_culture", [
        r"symphony",
        r"\borchestra\b",
        r"philharmonic",
        r"\bballet\b",
        r"\bopera\b",
        r"performing arts center",
        r"repertory theatre",
        r"repertory theater",
        r"shakespeare festival",
        r"\bjazz\b",
        r"\bchoir\b",
        r"\bchoral\b",
        r"barbershop",
        r"\bbluegrass\b",
        r"\btaiko\b",
        r"pipes and drums",
        r"bel canto",
        r"\bbaroque\b",
        r"art song",
        r"vocal arts",
        r"belcourt theatre",
        r"frist art museum",
        r"cheekwood",
        r"adventure science center",
        r"country music foundation",
        r"national museum of african american music",
        r"musicians hall of fame",
        r"discovery center at murfree",
        r"nashville ballet",
        r"nashville symphony",
        r"nashville children.s theatre",
        r"nashville repertory",
        r"nashville film festival",
        r"nashville opera",
        r"nashville jazz workshop",
        r"nashville design week",
        r"nashville book connection",
        r"nashville singers",
        r"nashville in harmony",
        r"nashville notes",
        r"nashville taiko",
        r"nashville pipes",
        r"nashville school of traditional country",
        r"borderless arts",
        r"bravo creative arts",
        r"buchanan arts",
        r"creative artists of tennessee",
        r"creative.*reuse",
        r"daybreak arts",
        r"early music city",
        r"ethos youth ensemble",
        r"fairview arts council",
        r"franklin light opera",
        r"franklin theatrical",
        r"harmonize artists",
        r"humanities tennessee",
        r"jazz empowers",
        r"love learning music",
        r"monthaven arts",
        r"music city baroque",
        r"music city review",
        r"music city youth in the arts",
        r"musical bridges",
        r"music neighbors",
        r"music for the soul",
        r"new dialect",
        r"north nashville arts",
        r"numinous flux",
        r"oz arts",
        r"portara ensemble",
        r"rutherford arts alliance",
        r"street theatre",
        r"sumner county theater",
        r"two rivers community orchestra",
        r"urban musical theatre",
        r"verge theater",
        r"vox grata",
        r"willow oak center for arts",
        r"women in theatre",
        r"sonus choir",
        r"operation song",
        r"freedom sings",
        r"the art song society",
        r"the arts place",
        r"the gift of song",
        r"abrasive.?media",
        r"act too players",
        r"actors bridge",
        r"actors point",
        r"catharsis nashville",
        r"audience of one",
        r"community arts.*bellevue",
        r"arts bellevue",
        r"classical 615",
        r"department of curious things",
        r"hendersonville performing",
        r"immerge",
        r"make nashville",
        r"metro nashville chorus",
        r"sweet adelines",
        r"the sing me a story",
        r"turnip green creative",
        r"nashville arcade arts",
        r"sound foundations",
        r"guitars 4 gifts",
        r"helping music foundation",
        r"inner light.*famil.*theatre",
        r"international bluegrass",
        r"international folkloric",
        r"tunes for kidz",
        r"monroe mandolin",
        r"chatterbird",
        r"the rabbit room",
        r"carnegie writers",
        r"arts & business council",
        r"arts business council",
        r"what departs with us",
        r"who speaks for earth",
        r"tennessee arts academy",
        r"tennessee artist",
        r"tennessee association of craft",
        r"tennessee performing arts center",
        r"tennessee philharmonic",
        r"tennessee presenters",
        r"tennessee theatre educators",
        r"tennessee youth symphony",
        r"move music city",
        r"nashville bel canto",
        r"sing me a story",
        r"music.*cares",
        r"musicares",
        r"friend2friend book",
        r"japanese.*society",
        r"japan.america society",
        r"sister cities",
        r"alliance francaise",
        r"theater bug",
        r"the theater bug",
        r"little blue theatre",
        r"playmakers nashville",
    ]),

    # ── Animal Welfare ────────────────────────────────────────────────────────
    ("animal_welfare", [
        r"animal rescue",
        r"animal welfare",
        r"animal shelter",
        r"animal humane",
        r"animal sanctuary",
        r"humane association",
        r"humane society",
        r"cat rescue",
        r"dog rescue",
        r"pet community center",
        r"spay.*neuter",
        r"neuter.*alliance",
        r"\bpaws\b",
        r"pawster",
        r"walden.s puddle",
        r"harmony wildlife",
        r"exotic avian",
        r"global sanctuary for elephant",
        r"fabled farm rescue",
        r"freedom farm animal",
        r"safe harbor equine",
        r"mules n more",
        r"lantern lane farm",
        r"gentle barn",
        r"old friends senior dogs",
        r"\bfluff\b",
        r"agape animal rescue",
        r"city of.*animal",
        r"country.*k-9 rescue",
        r"country k9 rescue",
        r"critter fixers",
        r"bent whisker",
        r"beacon rescue",
        r"cat colony food pantry",
        r"safe place for animals",
        r"saving cheatham animals",
        r"snooty giggles",
        r"wags and walks",
        r"williamson animal services",
        r"yorkshire terrier",
        r"rutherford county cat",
        r"sumner spay neuter",
        r"people for animals",
        r"nashville cat rescue",
        r"nashville humane",
        r"nashville wildlife conservation center",
        r"jesse.*beesley.*animal",
        r"fifth gospel equine",
        r"white fawn farm",
        r"ruby.s happy farm",
        r"safe harbor.*livestock",
        r"volunteer equine",
        r"archies promise",
        r"redemption ranch",
        r"music city pet partners",
        r"pet.*partner",
    ]),

    # ── Sports & Recreation ───────────────────────────────────────────────────
    ("sports_recreation", [
        r"lacrosse",
        r"disc golf",
        r"roller derby",
        r"\bhockey\b",
        r"steeplechase",
        r"golf foundation",
        r"tennessee golf",
        r"boxing resource center",
        r"running raccoon",
        r"nashville wolverines",
        r"nashville rowing",
        r"nashville youth sports",
        r"mid.?tn lacrosse",
        r"abc sports foundation",
        r"bellevue sports athletic",
        r"endure athletics",
        r"special needs sports",
        r"special olympics",
        r"achilles international",
        r"catalyst sports",
        r"nashville junior roller derby",
        r"volunteer state horsemens",
        r"iroquois steeplechase",
    ]),

    # ── Historical Preservation ───────────────────────────────────────────────
    ("historical_preservation", [
        r"historical society",
        r"historical association",
        r"history associates",
        r"preservation society",
        r"preserve.*nashville",
        r"preserve.*lindsley",
        r"preserve.*mallory",
        r"historic rock castle",
        r"historic cragfont",
        r"historic.*lebano",
        r"belle meade",
        r"belmont mansion",
        r"travellers rest",
        r"genealogical",
        r"lotz house",
        r"tennessee historical",
        r"fiddlers grove",
        r"oaklands association",
        r"sumner county museum",
        r"oscar.*farris.*museum",
        r"cheatham county historical",
        r"robertson county historical",
        r"heritage foundation of williamson",
        r"nashville historical foundation",
        r"bellevue harpeth historic",
        r"nashville steam preservation",
        r"history.*wilson county",
    ]),

    # ── Faith & Ministry (lowest priority — broad patterns) ───────────────────
    ("faith_ministry", [
        r"\bministr(ies|y)\b",
        r"\bchurch\b",
        r"\bchapel\b",
        r"\bseminar(y|ies)\b",
        r"\bgospel\b",
        r"\bchristian\b",
        r"ministerial alliance",
        r"faith enrichment",
        r"lambscroft",
        r"beech creek",
        r"caleb global",
        r"cross strength",
        r"\bcrossbridge\b",
        r"crossroads campus",
        r"delight ministries",
        r"elijah.s heart",
        r"family affair",
        r"fellowship of christian",
        r"from your father",
        r"gto conferences",
        r"great mercy",
        r"heartbound",
        r"his children foundation",
        r"hope restored",
        r"international outreach ministr",
        r"\bisaiah\s*(117|58)\b",
        r"life of victory.*christian",
        r"new life fellowship",
        r"new life outreach",
        r"penuel ridge",
        r"presbyterian campus",
        r"proceeding word",
        r"red frogs",
        r"rejoice ministr",
        r"renewed life",
        r"rescue 1 global",
        r"right road ministr",
        r"voice of thunder",
        r"vanderbilt hillel",
        r"agape to the nations",
        r"another chance unlimited",
        r"awake nashville",
        r"benchmark adventure ministr",
        r"bridge family ministr",
        r"broken restored redeemed",
        r"echoes of hope",
        r"faithfully restored",
        r"global hope myanmar",
        r"grace and glory",
        r"grace ministr",
        r"grace place",
        r"mission discovery",
        r"missions development",
        r"the operation andrew",
        r"shining stars international",
        r"seed india",
        r"the joshua movement",
        r"the king.s village",
        r"the branch of nashville",
        r"the center for student missions",
        r"water walkers",
        r"working mission",
        r"american baptist theological",
        r"disciples divinity",
        r"the ayin project",
        r"the barnabas vision",
        r"the estuary",
        r"the forge nashville",
        r"jacob.s audible",
        r"kiunga",
        r"j\.?c\.? movement",
        r"hope and life",
        r"day 7\b",
        r"the nook",
        r"time to rise",
        r"the mosaic institute",
    ]),
]


def classify_name(name: str) -> str | None:
    """Return a new category for the org, or None to leave it unchanged."""
    for category, patterns in RULES:
        if _match(name, patterns):
            return category
    return None


def main() -> None:
    with open(PROJECT_YML) as f:
        project = yaml.safe_load(f)

    suppress_types: set[str] = set(
        project.get("data_sources", {})
               .get("giving_matters", {})
               .get("suppress_types", [])
    )
    print(f"Suppress list: {sorted(suppress_types)}")

    with open(GEOJSON_PATH) as f:
        geojson = json.load(f)

    features = geojson["features"]
    before_count = len(features)

    reclassified: dict[str, int] = defaultdict(int)
    unchanged_other = 0

    for feat in features:
        props = feat["properties"]
        if props.get("partner_type") != "other":
            continue
        new_type = classify_name(props["partner_name"])
        if new_type:
            props["partner_type"] = new_type
            reclassified[new_type] += 1
        else:
            unchanged_other += 1

    # Apply suppress filter
    features_out = [
        f for f in features
        if f["properties"].get("partner_type") not in suppress_types
    ]
    suppressed_count = before_count - len(features_out)

    geojson["features"] = features_out
    with open(GEOJSON_PATH, "w") as f:
        json.dump(geojson, f, indent=2)

    print(f"\nReclassification results (from 'other'):")
    for cat, n in sorted(reclassified.items(), key=lambda x: -x[1]):
        marker = " [suppressed]" if cat in suppress_types else ""
        print(f"  {cat}: {n}{marker}")
    print(f"  other (no match): {unchanged_other}")
    print(f"\nSuppressed and removed: {suppressed_count} features")
    print(f"Output: {len(features_out)} features  (was {before_count})")

    print("\nFinal distribution:")
    final = Counter(f["properties"]["partner_type"] for f in features_out)
    for t, n in final.most_common():
        print(f"  {t}: {n}")


if __name__ == "__main__":
    main()
