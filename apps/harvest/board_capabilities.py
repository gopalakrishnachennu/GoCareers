"""
Board Capability Matrix
=======================
Defines what fields each ATS platform's API/scraper CAN provide, independent
of whether our parser currently extracts them.

Use this to separate two different problems:
  "source lacks it"  → capability=False  → not extractable without scraping detail pages
  "parser missed it" → capability=True, coverage=low → fixable with parser work

Usage:
    from harvest.board_capabilities import get_capabilities, BOARD_CAPABILITIES

    caps = get_capabilities("greenhouse")
    if caps["salary"]:
        # source provides salary — low coverage means parser gap
    else:
        # source doesn't surface salary — expected 0%
"""

from __future__ import annotations

# Each entry:
#   jd                 — full job description
#   requirements       — dedicated requirements / qualifications section
#   responsibilities   — dedicated responsibilities / duties section
#   department         — department / team field
#   geo                — city/state/country fields (not just "Remote" in title)
#   salary             — salary range (min/max)
#   employment_type    — full-time / part-time / contract
#   education          — education requirement
#   experience_level   — explicit seniority field (beyond keyword detection)
#   detail_fetch       — second HTTP call needed to get full JD (vs. in list API)
#   pagination         — platform supports multi-page / cursor pagination
#   source_reliability — "api" | "semi" | "scrape"
#     api    = public documented REST/GraphQL endpoint, stable
#     semi   = unofficial/undocumented JSON but consistent
#     scrape = HTML-only, fragile

BOARD_CAPABILITIES: dict[str, dict] = {
    "workday": {
        "jd": True,
        "requirements": False,      # JD is one HTML blob; no separate sections
        "responsibilities": False,
        "department": True,
        "geo": True,
        "salary": False,            # Workday rarely exposes salary in API
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": True,       # list page → detail endpoint for full JD
        "pagination": True,
        "source_reliability": "semi",
    },
    "greenhouse": {
        "jd": True,
        "requirements": False,      # JD is one HTML blob
        "responsibilities": False,
        "department": True,
        "geo": True,
        "salary": False,            # rarely in public API
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": False,      # full content in list API
        "pagination": True,
        "source_reliability": "api",
    },
    "lever": {
        "jd": True,
        "requirements": True,       # `lists[]` array with labeled sections
        "responsibilities": True,
        "department": True,         # categories.department
        "geo": True,
        "salary": False,            # rarely populated; best-effort from text
        "employment_type": True,    # categories.commitment
        "education": False,
        "experience_level": False,
        "detail_fetch": False,
        "pagination": True,
        "source_reliability": "api",
    },
    "ashby": {
        "jd": True,
        "requirements": False,      # GraphQL: description is one HTML block
        "responsibilities": False,
        "department": True,
        "geo": True,
        "salary": True,             # compensation{min,max,currency} in GraphQL
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": False,
        "pagination": True,
        "source_reliability": "api",
    },
    "icims": {
        "jd": True,
        "requirements": True,       # separate `qualifications` field in API
        "responsibilities": True,   # separate `responsibilities` field
        "department": True,
        "geo": True,
        "salary": False,
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": False,
        "pagination": True,
        "source_reliability": "semi",
    },
    "taleo": {
        "jd": True,
        "requirements": False,      # JD is HTML blob
        "responsibilities": False,
        "department": True,
        "geo": True,
        "salary": False,
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": True,       # detail fetch for full JD
        "pagination": True,
        "source_reliability": "semi",
    },
    "jobvite": {
        "jd": True,
        "requirements": False,
        "responsibilities": False,
        "department": True,
        "geo": True,
        "salary": False,
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": True,
        "pagination": True,
        "source_reliability": "semi",
    },
    "oracle": {
        "jd": True,
        "requirements": False,
        "responsibilities": False,
        "department": True,
        "geo": True,
        "salary": False,
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": False,
        "pagination": True,
        "source_reliability": "semi",
    },
    "smartrecruiters": {
        "jd": True,
        "requirements": True,       # sections.qualifications
        "responsibilities": True,   # sections.jobSummary / tasks
        "department": True,
        "geo": True,
        "salary": True,             # compensation{min,max,currency,payPeriod}
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": True,       # list has stub; detail API has sections
        "pagination": True,
        "source_reliability": "api",
    },
    "workable": {
        "jd": True,
        "requirements": True,       # requirements field in job detail
        "responsibilities": True,   # summary / responsibilities
        "department": True,
        "geo": True,
        "salary": False,
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": True,
        "pagination": True,
        "source_reliability": "semi",
    },
    "bamboohr": {
        "jd": True,
        "requirements": True,       # qualifications / requirements in detail
        "responsibilities": True,   # responsibilities / summary
        "department": True,
        "geo": True,
        "salary": True,             # minSalary/maxSalary in detail API
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": True,
        "pagination": False,        # all jobs in one list call
        "source_reliability": "semi",
    },
    "recruitee": {
        "jd": True,
        "requirements": False,      # description is one HTML blob
        "responsibilities": False,
        "department": True,
        "geo": True,
        "salary": False,
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": False,
        "pagination": True,
        "source_reliability": "api",
    },
    "ultipro": {
        "jd": True,
        "requirements": True,       # Qualifications in detail API
        "responsibilities": True,   # Responsibilities in detail API
        "department": True,
        "geo": True,
        "salary": False,
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": True,
        "pagination": True,
        "source_reliability": "semi",
    },
    "dayforce": {
        "jd": False,                # harvester not operational
        "requirements": False,
        "responsibilities": False,
        "department": False,
        "geo": False,
        "salary": False,
        "employment_type": False,
        "education": False,
        "experience_level": False,
        "detail_fetch": False,
        "pagination": False,
        "source_reliability": "scrape",
        "unsupported": True,
    },
    "breezy": {
        "jd": True,
        "requirements": False,      # HTML scraper — section detection only
        "responsibilities": False,
        "department": True,         # parsed from group-header h2
        "geo": True,
        "salary": False,
        "employment_type": False,   # not available in HTML listing
        "education": False,
        "experience_level": False,
        "detail_fetch": True,       # fetches detail page for JD
        "pagination": False,
        "source_reliability": "scrape",
    },
    "zoho": {
        "jd": False,                # hidden JSON → detail page needed
        "requirements": False,
        "responsibilities": False,
        "department": True,         # Industry field
        "geo": True,
        "salary": False,
        "employment_type": False,
        "education": False,
        "experience_level": False,
        "detail_fetch": True,
        "pagination": False,
        "source_reliability": "scrape",
    },
    "teamtailor": {
        "jd": True,
        "requirements": False,      # HTML scraper
        "responsibilities": False,
        "department": True,         # meta_parts[0]
        "geo": True,
        "salary": False,
        "employment_type": False,
        "education": False,
        "experience_level": False,
        "detail_fetch": True,
        "pagination": False,
        "source_reliability": "scrape",
    },
    "applytojob": {
        "jd": False,
        "requirements": False,
        "responsibilities": False,
        "department": False,
        "geo": False,
        "salary": False,
        "employment_type": False,
        "education": False,
        "experience_level": False,
        "detail_fetch": False,
        "pagination": False,
        "source_reliability": "scrape",
        "unsupported": True,
    },
    "adp": {
        "jd": False,
        "requirements": False,
        "responsibilities": False,
        "department": False,
        "geo": False,
        "salary": False,
        "employment_type": False,
        "education": False,
        "experience_level": False,
        "detail_fetch": False,
        "pagination": False,
        "source_reliability": "scrape",
        "unsupported": True,
    },
    "applicantpro": {
        "jd": False,
        "requirements": False,
        "responsibilities": False,
        "department": False,
        "geo": False,
        "salary": False,
        "employment_type": False,
        "education": False,
        "experience_level": False,
        "detail_fetch": False,
        "pagination": False,
        "source_reliability": "scrape",
        "unsupported": True,
    },
}

_COVERAGE_FIELDS = [
    "jd", "requirements", "responsibilities", "department",
    "geo", "salary", "employment_type", "education",
]


def get_capabilities(slug: str) -> dict:
    """Return capability dict for a platform slug, with defaults for unknown slugs."""
    caps = BOARD_CAPABILITIES.get(slug)
    if caps:
        return caps
    # Sensible unknown defaults
    return {
        "jd": True,
        "requirements": False,
        "responsibilities": False,
        "department": True,
        "geo": True,
        "salary": False,
        "employment_type": True,
        "education": False,
        "experience_level": False,
        "detail_fetch": False,
        "pagination": True,
        "source_reliability": "unknown",
    }


def capability_gap(slug: str, coverage: dict) -> dict[str, str]:
    """
    Given a coverage dict (field→pct|None), return per-field gap assessment:
      'n/a'       — source doesn't provide this field (expected 0%)
      'good'      — coverage ≥ 50%
      'parser'    — source provides it but coverage < 50% (parser gap)
      'unknown'   — no data
    """
    caps = get_capabilities(slug)
    result = {}
    for field in _COVERAGE_FIELDS:
        pct = coverage.get(field)
        if not caps.get(field):
            result[field] = "n/a"
        elif pct is None:
            result[field] = "unknown"
        elif pct >= 50:
            result[field] = "good"
        else:
            result[field] = "parser"
    return result
