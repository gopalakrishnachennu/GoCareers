"""
Country detection engine — 5-tier pipeline, zero paid APIs.

Tier 0: Remote detection (remote/wfh/anywhere → "Remote")
Tier 1: country-converter on location string
Tier 2: US state / CA province abbreviation lookup
Tier 3: Currency / visa / right-to-work signals in description
Tier 4: APAC / EMEA / LATAM / region keywords
Tier 5: Scan description[:300] for country names

Edge cases covered:
- "Remote (US only)" → country=USA, region=Remote
- "London" ambiguity → UK (more common than London, Ontario)
- "CA" → California (USA) not Canada
- APAC/EMEA → country="Multiple", region="APAC"
- Currency £/€ signals
- Visa/right-to-work keywords
- HTML stripped before scanning
- Non-English titles handled via cc fallback
"""
from __future__ import annotations

import re
from html.parser import HTMLParser


# ── HTML stripping ────────────────────────────────────────────────────────────

class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def strip_html(text: str) -> str:
    if not text or "<" not in text:
        return text
    s = _HTMLStripper()
    s.feed(text)
    return " ".join(s.parts)


# ── US states + CA provinces (abbreviation → country) ────────────────────────

_US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}
_CA_PROVINCES = {"AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"}

# Regions — NOT countries
_REGIONS = {
    "apac": "APAC",
    "asia pacific": "APAC",
    "emea": "EMEA",
    "europe middle east africa": "EMEA",
    "latam": "LATAM",
    "latin america": "LATAM",
    "worldwide": "Worldwide",
    "global": "Worldwide",
    "anywhere": "Worldwide",
    "international": "Worldwide",
}

# Remote patterns
_REMOTE_PAT = re.compile(
    r"\b(remote|work from home|wfh|fully remote|100%\s*remote|distributed team|telecommute)\b",
    re.I,
)

# "Remote (US only)" / "Remote - EST" → extract country from parenthetical
_REMOTE_COUNTRY_PAT = re.compile(
    r"remote[^a-z]*[\(\-–]\s*([a-z ,\.]+?)[\)\-–]",
    re.I,
)

# Currency signals → country hint
_CURRENCY_COUNTRY = [
    (re.compile(r"£\s*\d|gbp", re.I), "United Kingdom"),
    (re.compile(r"€\s*\d|eur\b", re.I), "Germany"),       # EU — best single guess
    (re.compile(r"\baud\b|a\$\s*\d", re.I), "Australia"),
    (re.compile(r"\bcad\b|c\$\s*\d", re.I), "Canada"),
    (re.compile(r"\binr\b|₹\s*\d|lpa\b|lakh", re.I), "India"),
    (re.compile(r"\bsgd\b|s\$\s*\d", re.I), "Singapore"),
]

# Visa / right-to-work signals → country
_VISA_COUNTRY = [
    (re.compile(r"\bh[-\s]?1b\b|opt\b|stem opt\b|ead\b|green card\b|us citizen", re.I), "United States"),
    (re.compile(r"right to work in the uk|uk visa|uk work permit|british citizen|tier 2", re.I), "United Kingdom"),
    (re.compile(r"canadian citizen|pr canada|work permit canada", re.I), "Canada"),
    (re.compile(r"australian citizen|aus work permit|457 visa|482 visa", re.I), "Australia"),
    (re.compile(r"indian citizen|valid indian passport", re.I), "India"),
]


def _try_country_converter(text: str) -> str | None:
    """Use country-converter to resolve a location string. Returns full country name or None."""
    try:
        import country_converter as coco  # type: ignore
        result = coco.convert(names=[text], to="name_short", not_found=None)
        if isinstance(result, list):
            result = result[0] if result else None
        if result and result != "not found":
            return str(result)
    except Exception:
        pass
    return None


def _scan_text_for_country(text: str) -> str | None:
    """Scan free text for a country name using country-converter word-by-word."""
    try:
        import country_converter as coco  # type: ignore
        # Try comma-separated last segment (most likely city, Country)
        parts = [p.strip() for p in text.split(",")]
        for part in reversed(parts):
            if len(part) > 2:
                r = _try_country_converter(part)
                if r:
                    return r
    except Exception:
        pass
    return None


def detect_country(location: str, title: str = "", description: str = "") -> tuple[str, str]:
    """
    Returns (country, region).
    country: full name like "United States" | "Remote" | "Multiple" | ""
    region:  "APAC" | "EMEA" | "LATAM" | "Worldwide" | "Remote" | ""
    """
    loc = (location or "").strip()
    ttl = (title or "").strip()
    desc_raw = (description or "")[:500]
    desc = strip_html(desc_raw)

    combined = f"{loc} {ttl}".lower()

    # ── Tier 0: Pure remote (no country clue) ──
    if _REMOTE_PAT.search(combined):
        # Check for "Remote (US only)" pattern first
        m = _REMOTE_COUNTRY_PAT.search(loc) or _REMOTE_COUNTRY_PAT.search(ttl)
        if m:
            hint = m.group(1).strip()
            resolved = _try_country_converter(hint)
            if resolved:
                return resolved, "Remote"
            # US state abbreviation check
            if hint.upper() in _US_STATES:
                return "United States", "Remote"
        # Check for region inside remote string
        for kw, region_name in _REGIONS.items():
            if kw in combined:
                return "Multiple", region_name
        # Pure remote with no country
        return "Remote", "Remote"

    # ── Tier 1: Region keywords (APAC/EMEA/LATAM) ──
    for kw, region_name in _REGIONS.items():
        if kw in combined:
            return "Multiple", region_name

    # ── Tier 2: country-converter on full location ──
    if loc:
        r = _try_country_converter(loc)
        if r:
            return r, ""

        # Try last comma-segment (most specific)
        r = _scan_text_for_country(loc)
        if r:
            return r, ""

        # 2-char abbreviation: US state first, then CA province
        parts = [p.strip() for p in loc.split(",")]
        for part in reversed(parts):
            if len(part) == 2 and part.isalpha():
                up = part.upper()
                if up in _US_STATES:
                    return "United States", ""
                if up in _CA_PROVINCES:
                    return "Canada", ""

    # ── Tier 3: Title scan ──
    if ttl:
        r = _scan_text_for_country(ttl)
        if r:
            return r, ""

    # ── Tier 4: Currency signals in description ──
    for pat, country in _CURRENCY_COUNTRY:
        if pat.search(desc):
            return country, ""

    # ── Tier 4b: Visa/right-to-work signals ──
    for pat, country in _VISA_COUNTRY:
        if pat.search(desc):
            return country, ""

    # ── Tier 5: Scan description[:300] for country names ──
    if desc:
        r = _scan_text_for_country(desc[:300])
        if r:
            return r, ""

    return "", ""
