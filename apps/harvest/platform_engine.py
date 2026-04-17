"""
GoCareers harvest — platform engine registry.

Maps every ATS listed in the [OpenPostings](https://github.com/Masterjx9/OpenPostings)
aggregator README to our implementation status. OpenPostings implements fetch/sync in a
single Node `server/index.js` (HTML + API parsers per vendor). We implement the same
idea in Python: dedicated `*Harvester` classes where possible, `HTMLScrapeHarvester`
fallback for long-tail vendors, and explicit `planned` rows for parity gaps.

Use this module for admin dashboards, audits, and docs — not as a second dispatch path;
`get_harvester()` in `harvesters/__init__.py` remains the runtime entry point.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterator

OPENPOSTINGS_REPO = "https://github.com/Masterjx9/OpenPostings"


class ImplementationKind(str, Enum):
    """How we fetch jobs for this slug today."""

    DEDICATED = "dedicated"       # Explicit class in HARVESTER_MAP
    GENERIC_HTML = "generic_html"  # Falls back to HTMLScrapeHarvester
    PLANNED = "planned"          # Listed in OpenPostings; no first-class harvester yet


@dataclass(frozen=True)
class PlatformRecord:
    """One row in the unified ATS matrix."""

    slug: str
    label: str
    openpostings_label: str | None
    kind: ImplementationKind
    notes: str = ""


# Slugs we explicitly register in HARVESTER_MAP (dedicated parsers / APIs).
_DEDICATED_SLUGS: frozenset[str] = frozenset({
    "workday", "greenhouse", "lever", "ashby", "workable", "smartrecruiters",
    "bamboohr", "recruitee", "oracle", "icims", "jobvite", "taleo", "ultipro",
    "dayforce", "breezy", "zoho", "teamtailor",
})

# ATS names from OpenPostings README "Supported ATS" — slug is our internal key when we
# have one; otherwise we use a placeholder slug for roadmap tracking.
_OPENPOSTINGS_MATRIX: tuple[PlatformRecord, ...] = (
    # Implemented with dedicated or strong parsers in this repo
    PlatformRecord("workday", "Workday", "Workday", ImplementationKind.DEDICATED, ""),
    PlatformRecord("ashby", "Ashby", "Ashby / ashbyhq", ImplementationKind.DEDICATED, ""),
    PlatformRecord("greenhouse", "Greenhouse", "Greenhouse / greenhouse.io", ImplementationKind.DEDICATED, ""),
    PlatformRecord("lever", "Lever", "Lever / lever.co", ImplementationKind.DEDICATED, ""),
    PlatformRecord("jobvite", "Jobvite", "Jobvite / jobvite.com", ImplementationKind.DEDICATED, ""),
    PlatformRecord("applicantpro", "ApplicantPro", "Applicantpro / applicantpro.com", ImplementationKind.GENERIC_HTML, "HTML fallback"),
    PlatformRecord("applytojob", "ApplyToJob", "Applytojob / applytojob.com", ImplementationKind.GENERIC_HTML, "HTML fallback"),
    PlatformRecord("theapplicantmanager", "The Applicant Manager", "Theapplicantmanager / theapplicantmanager.com", ImplementationKind.GENERIC_HTML, "HTML fallback"),
    PlatformRecord("icims", "iCIMS", "Icims / icims.com", ImplementationKind.DEDICATED, ""),
    PlatformRecord("recruitee", "Recruitee", "Recruitee / recruitee.com", ImplementationKind.DEDICATED, ""),
    PlatformRecord("ultipro", "UltiPro / UKG", "Ultipro / ukg", ImplementationKind.DEDICATED, "Board API + fallbacks"),
    PlatformRecord("taleo", "Taleo", "Taleo / taleo.net", ImplementationKind.DEDICATED, ""),
    PlatformRecord("breezy", "Breezy HR", "BreezyHR", ImplementationKind.DEDICATED, "Ported HTML list parser (OpenPostings-aligned)"),
    PlatformRecord("zoho", "Zoho Recruit", "Zoho", ImplementationKind.DEDICATED, "Hidden jobs/meta inputs JSON (OpenPostings-aligned)"),
    PlatformRecord("bamboohr", "BambooHR", "BambooHR", ImplementationKind.DEDICATED, ""),
    PlatformRecord("workable", "Workable", "—", ImplementationKind.DEDICATED, "Not in OP README table; added from real URLs"),
    PlatformRecord("smartrecruiters", "SmartRecruiters", "—", ImplementationKind.DEDICATED, ""),
    PlatformRecord("dayforce", "Dayforce HCM", "—", ImplementationKind.DEDICATED, "Ceridian — API parity ongoing"),
    PlatformRecord("adp", "ADP", "—", ImplementationKind.GENERIC_HTML, "SPA-heavy"),
    PlatformRecord("oracle", "Oracle HCM", "—", ImplementationKind.DEDICATED, "REST requisitions"),
    # OpenPostings-only (no first-class harvester here yet)
    PlatformRecord("applicantai", "ApplicantAI", "ApplicantAI", ImplementationKind.PLANNED, "OP: HTML blocks parser"),
    PlatformRecord("career_plug", "CareerPlug", "Career Plug", ImplementationKind.PLANNED, ""),
    PlatformRecord("career_puck", "CareerPuck", "Career Puck", ImplementationKind.PLANNED, ""),
    PlatformRecord("fountain", "Fountain", "Fountain", ImplementationKind.PLANNED, ""),
    PlatformRecord("getro", "Getro", "Getro", ImplementationKind.PLANNED, ""),
    PlatformRecord("hrm_direct", "HRM Direct", "HRM Direct", ImplementationKind.PLANNED, ""),
    PlatformRecord("talent_lyft", "Talent Lyft", "Talent Lyft", ImplementationKind.PLANNED, ""),
    PlatformRecord("talexio", "Talexio", "Talexio", ImplementationKind.PLANNED, ""),
    PlatformRecord("teamtailor", "Teamtailor", "Team Tailor", ImplementationKind.DEDICATED, "block-grid-item HTML parser (OpenPostings-aligned)"),
    PlatformRecord("talent_reef", "Talent Reef", "Talent Reef", ImplementationKind.PLANNED, ""),
    PlatformRecord("manatal", "Manatal", "Manatal", ImplementationKind.PLANNED, ""),
    PlatformRecord("gem", "Gem", "Gem", ImplementationKind.PLANNED, ""),
    PlatformRecord("jobaps", "Jobaps", "Jobaps", ImplementationKind.PLANNED, ""),
    PlatformRecord("join", "Join.com", "Join", ImplementationKind.PLANNED, ""),
    PlatformRecord("saphrcloud", "SAP HR Cloud", "Saphrcloud", ImplementationKind.PLANNED, ""),
)


def iter_openpostings_matrix() -> Iterator[PlatformRecord]:
    """Yield the full OpenPostings-oriented capability matrix."""
    yield from _OPENPOSTINGS_MATRIX


def dedicated_slugs() -> frozenset[str]:
    """Slugs with explicit harvester classes."""
    return _DEDICATED_SLUGS


def kind_for_slug(slug: str) -> ImplementationKind:
    """Resolve implementation kind for a platform slug."""
    for row in _OPENPOSTINGS_MATRIX:
        if row.slug == slug:
            return row.kind
    if slug in _DEDICATED_SLUGS:
        return ImplementationKind.DEDICATED
    return ImplementationKind.GENERIC_HTML


def harvester_class_name_for_slug(slug: str) -> str:
    """Best-effort harvester class name for logging/UI (no import side effects)."""
    mapping = {
        "workday": "WorkdayHarvester",
        "greenhouse": "GreenhouseHarvester",
        "lever": "LeverHarvester",
        "ashby": "AshbyHarvester",
        "workable": "WorkableHarvester",
        "smartrecruiters": "SmartRecruitersHarvester",
        "bamboohr": "BambooHRHarvester",
        "recruitee": "RecruiteeHarvester",
        "oracle": "OracleHCMHarvester",
        "icims": "IcimsHarvester",
        "jobvite": "JobviteHarvester",
        "taleo": "TaleoHarvester",
        "ultipro": "UltiProHarvester",
        "dayforce": "DayforceHarvester",
        "breezy": "BreezyHarvester",
        "zoho": "ZohoHarvester",
        "teamtailor": "TeamtailorHarvester",
    }
    return mapping.get(slug, "HTMLScrapeHarvester")
