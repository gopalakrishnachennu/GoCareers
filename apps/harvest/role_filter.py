from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any


STRONG = "STRONG"
POSSIBLE = "POSSIBLE"
COLD = "COLD"
UNKNOWN = "UNKNOWN"
NO_MATCH = "NO_MATCH"

SENIORITY_PATTERNS = [
    r"\bsenior\b",
    r"\bjunior\b",
    r"\bstaff\b",
    r"\bprincipal\b",
    r"\blead\b",
    r"\bsr\.?\b",
    r"\bjr\.?\b",
    r"\bhead of\b",
    r"\bdirector of\b",
    r"\bvp of\b",
    r"\bdistinguished\b",
    r"\bassociate\b",
    r"\bmid-?level\b",
    r"\bentry-?level\b",
    r"\bl[3-9]\b",
    r"\bic[3-9]\b",
    r"\be[3-9]\b",
    r"\bi{1,3}v?\b",
    r"\b\d+\b",
]

TECH_DEPARTMENT_SIGNALS = [
    "engineering",
    "technology",
    "data",
    "cloud",
    "platform",
    "infrastructure",
    "security",
    "information technology",
    "software",
    "product engineering",
    "devops",
    "it",
    "systems",
    "computing",
    "artificial intelligence",
    "machine learning",
    "research and development",
    "r&d",
    "digital",
]

NON_TECH_DEPARTMENT_SIGNALS = [
    # Healthcare / clinical — bedside & hospital operations
    "nursing",
    "patient care",
    "patient services",
    "clinical operations",
    "clinical services",
    "pharmacy",
    "pharmacology",
    "dental",
    "radiology",
    "laboratory",
    "pathology",
    "oncology",
    "cardiology",
    "pediatrics",
    "orthopedics",
    "neurology",
    "rehabilitation",
    "therapy services",
    "surgical services",
    "emergency medicine",
    "primary care",
    "behavioral health",
    "mental health",
    "hospice",
    "home health",
    # Note: "clinical" alone is intentionally excluded here — "clinical informatics"
    # and "clinical systems" are IT departments.  Use hard_negative_phrases in
    # HarvestEngineConfig for title-level blocking of pure clinical roles.
    # Food / hospitality / facilities
    "food service",
    "culinary",
    "housekeeping",
    "environmental services",
    "facilities management",
    "laundry",
    # Retail / warehouse
    "retail operations",
    "warehouse operations",
    "store operations",
    "distribution center",
    # Other non-tech
    "human resources",
    "payroll",
    "legal",
    "compliance",
    "audit",
    "finance",
    "accounting",
    "marketing",
    "sales",
    "customer service",
]

GENERIC_TECH_SIGNALS = [
    # These are last-resort POSSIBLE signals when no category phrase matched.
    # Keep these as multi-word phrases — single words like "engineer" or
    # "developer" are too broad and cause false positives on non-tech roles.
    "software engineer",
    "software developer",
    "data engineer",
    "data analyst",
    "data scientist",
    "machine learning engineer",
    "ml engineer",
    "ai engineer",
    "artificial intelligence engineer",
    "quality engineer",
    "test engineer",
    "test automation engineer",
    "technical lead",
    "technical architect",
    "platform engineer",
    "systems engineer",
    "security engineer",
    "network engineer",
    "cloud architect",
    "solutions architect",
    "product engineer",
    "research engineer",
    "engineering manager",
    "tech lead",
    # Enterprise platform roles (catch-all before dedicated categories are set up)
    "servicenow developer",
    "salesforce developer",
    "sap consultant",
    "workday consultant",
    "oracle consultant",
    # Healthcare IT
    "healthcare it",
    "ehr analyst",
    "emr analyst",
    "epic analyst",
    "cerner analyst",
]


# ── Compound-word normalizations ─────────────────────────────────────────────
# Tech ops portmanteaus are written both as one word ("mlops") and two words
# ("ml ops").  Job boards use both forms inconsistently.  Apply these after
# basic cleanup so that "ML Ops Engineer" and "MLOps Engineer" both collapse
# to the same canonical token before phrase matching.
#
# Applied in both normalize() and normalize_phrase() so titles and stored
# phrases always reach the same form regardless of how they were typed.
# ─────────────────────────────────────────────────────────────────────────────
COMPOUND_JOINS = [
    # ops portmanteaus — order matters: longer patterns first
    (r"\bdev\s+sec\s+ops\b",   "devsecops"),
    (r"\bdev\s+ops\b",         "devops"),
    (r"\bml\s+ops\b",          "mlops"),
    (r"\bdata\s+ops\b",        "dataops"),
    (r"\bsec\s+ops\b",         "secops"),
    (r"\bfin\s+ops\b",         "finops"),
    (r"\bcloud\s+ops\b",       "cloudops"),
    (r"\bit\s+ops\b",          "itops"),
    # Other common split forms
    (r"\bfull\s+stack\b",      "full stack"),   # keep as two words (already two-word phrase)
    (r"\bback\s+end\b",        "backend"),
    (r"\bfront\s+end\b",       "frontend"),
    (r"\bopen\s+ai\b",         "openai"),
]


def _apply_compound_joins(text: str) -> str:
    for pattern, replacement in COMPOUND_JOINS:
        text = re.sub(pattern, replacement, text)
    return text


@dataclass(frozen=True)
class ClassifyResult:
    decision: str
    category: str | None
    matched_phrase: str | None
    matched_negative: str | None
    reason: str
    snapshot_id: str | None


def normalize(text: str) -> str:
    """Normalize a job TITLE for matching.

    Steps:
      1. Lowercase + strip
      2. Separators (- _ / | backslash) → space
      3. Punctuation → space
      4. Compound-join ops portmanteaus: "ml ops" → "mlops", "dev ops" → "devops"
      5. Strip seniority prefixes/suffixes so "Senior ML Ops Engineer" and
         "MLOps Engineer" both collapse to "mlops engineer"
    """
    if not text:
        return ""
    text = str(text).lower().strip()
    text = re.sub(r"[-_/\\|]", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = _apply_compound_joins(text)
    for pattern in SENIORITY_PATTERNS:
        text = re.sub(pattern, " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_phrase(phrase: str) -> str:
    """Normalize a phrase from the category include/exclude bank.

    Same basic cleanup as normalize() + compound joins, but intentionally does
    NOT strip seniority words.  This means:
      - Phrases should be written without seniority prefixes ("mlops engineer"
        not "senior mlops engineer").
      - "ml ops engineer" entered as a phrase normalizes to "mlops engineer"
        so it matches titles written either way.
    """
    if not phrase:
        return ""
    text = str(phrase).lower().strip()
    text = re.sub(r"[-_/\\|]", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = _apply_compound_joins(text)
    return re.sub(r"\s+", " ", text).strip()


def phrase_match(normalized_text: str, phrase: str) -> bool:
    """Return True if *phrase* appears as a whole word in *normalized_text*.

    *normalized_text* must already be the output of ``normalize()`` (title with
    seniority stripped).  The phrase is normalized with ``normalize_phrase()``
    which keeps the phrase words intact so that "staff engineer" only matches
    titles that literally say "staff engineer" after basic cleanup — not every
    title containing the single word "engineer".
    """
    p = normalize_phrase(phrase)
    if not p:
        return False
    pattern = r"(?<!\w)" + re.escape(p) + r"(?!\w)"
    return bool(re.search(pattern, normalized_text))


def _first_phrase_match(normalized_text: str, phrases: list[str]) -> str | None:
    for phrase in phrases or []:
        if phrase_match(normalized_text, str(phrase)):
            return str(phrase)
    return None


def _category_list(categories: list[Any]) -> list[dict]:
    return [c for c in (categories or []) if isinstance(c, dict)]


def compute_phrase_hash(payload: dict) -> str:
    body = json.dumps(payload or {}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def classify_title(
    *,
    title: str,
    department: str = "",
    categories: list[dict] | None = None,
    hard_negatives: list[str] | None = None,
    custom_phrases: list[str] | None = None,
    snapshot_id: str | None = None,
) -> ClassifyResult:
    title_raw = title or ""
    if not title_raw.strip():
        return ClassifyResult(UNKNOWN, None, None, None, "empty or null title - cannot classify", snapshot_id)

    if not re.search(r"[A-Za-z]", title_raw):
        return ClassifyResult(UNKNOWN, None, None, None, "non-ASCII title - cannot match English phrases", snapshot_id)

    normalized_title = normalize(title_raw)
    normalized_department = normalize(department or "")
    cats = _category_list(categories or [])

    include_hit: tuple[dict, str] | None = None
    for category in cats:
        phrase = _first_phrase_match(normalized_title, category.get("include_phrases") or [])
        if phrase:
            include_hit = (category, phrase)
            break

    negative = _first_phrase_match(normalized_title, hard_negatives or [])
    if negative and include_hit is None:
        return ClassifyResult(NO_MATCH, None, None, negative, f"matched hard negative: {negative}", snapshot_id)

    custom_hit = _first_phrase_match(normalized_title, custom_phrases or [])
    if custom_hit:
        return ClassifyResult(STRONG, None, custom_hit, negative, f"company-specific phrase: {custom_hit}", snapshot_id)

    if include_hit is not None:
        category, phrase = include_hit
        category_exclude = _first_phrase_match(normalized_title, category.get("exclude_phrases") or [])
        category_slug = str(category.get("slug") or "") or None
        category_name = str(category.get("name") or category_slug or "")
        if category_exclude:
            return ClassifyResult(
                POSSIBLE,
                category_slug,
                phrase,
                negative or category_exclude,
                f"include '{phrase}' and exclude '{category_exclude}' both matched - keeping as POSSIBLE",
                snapshot_id,
            )
        reason = f"matched phrase: {phrase} | category: {category_name}"
        if negative:
            reason = f"ambiguous: negative '{negative}' and include phrase '{phrase}' both matched - keeping"
        return ClassifyResult(STRONG, category_slug, phrase, negative, reason, snapshot_id)

    tech_department = _first_phrase_match(normalized_department, TECH_DEPARTMENT_SIGNALS)
    non_tech_department = _first_phrase_match(normalized_department, NON_TECH_DEPARTMENT_SIGNALS)
    generic_hit = _first_phrase_match(normalized_title, GENERIC_TECH_SIGNALS)

    if generic_hit and non_tech_department:
        return ClassifyResult(
            COLD,
            None,
            generic_hit,
            None,
            f"generic title but non-tech department: {department}",
            snapshot_id,
        )

    if tech_department:
        return ClassifyResult(
            POSSIBLE,
            None,
            tech_department,
            None,
            f"no title match but department signals tech: {department}",
            snapshot_id,
        )

    if generic_hit:
        return ClassifyResult(POSSIBLE, None, generic_hit, None, f"generic tech signal: {generic_hit}", snapshot_id)

    return ClassifyResult(COLD, None, None, None, "no tech signal in title or department", snapshot_id)
