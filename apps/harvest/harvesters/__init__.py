from .workday import WorkdayHarvester
from .greenhouse import GreenhouseHarvester
from .lever import LeverHarvester
from .ashby import AshbyHarvester
from .icims import IcimsHarvester
from .jobvite import JobviteHarvester
from .taleo import TaleoHarvester
from .workable import WorkableHarvester
from .smartrecruiters import SmartRecruitersHarvester
from .bamboohr import BambooHRHarvester
from .recruitee import RecruiteeHarvester
from .oracle import OracleHCMHarvester
from .ultipro import UltiProHarvester
from .dayforce import DayforceHarvester
from .breezy import BreezyHarvester
from .zoho import ZohoHarvester
from .teamtailor import TeamtailorHarvester
from .html_scraper import HTMLScrapeHarvester

HARVESTER_MAP: dict[str, type] = {
    # ── Full dedicated API harvesters ─────────────────────────────────────────
    "workday":         WorkdayHarvester,
    "greenhouse":      GreenhouseHarvester,
    "lever":           LeverHarvester,
    "ashby":           AshbyHarvester,
    "workable":        WorkableHarvester,
    "smartrecruiters": SmartRecruitersHarvester,
    "bamboohr":        BambooHRHarvester,
    "recruitee":       RecruiteeHarvester,
    "oracle":          OracleHCMHarvester,
    # ── Dedicated HTML/AJAX scrapers ──────────────────────────────────────────
    "icims":           IcimsHarvester,
    "jobvite":         JobviteHarvester,
    "taleo":           TaleoHarvester,
    "ultipro":         UltiProHarvester,
    "dayforce":        DayforceHarvester,
    "breezy":          BreezyHarvester,
    "zoho":            ZohoHarvester,
    "teamtailor":      TeamtailorHarvester,
    # ── Generic HTML fallback (covers adp, applytojob, applicantpro, etc.) ────
    # These use HTMLScrapeHarvester which follows job-looking links on the
    # career page. Works OK for simple static pages; may miss SPA-rendered jobs.
}


def get_harvester(platform_slug: str):
    """Return the appropriate harvester instance for a platform slug."""
    cls = HARVESTER_MAP.get(platform_slug, HTMLScrapeHarvester)
    return cls()


__all__ = [
    "WorkdayHarvester", "GreenhouseHarvester", "LeverHarvester",
    "AshbyHarvester", "IcimsHarvester", "JobviteHarvester", "TaleoHarvester",
    "WorkableHarvester", "SmartRecruitersHarvester", "BambooHRHarvester",
    "RecruiteeHarvester", "OracleHCMHarvester", "UltiProHarvester",
    "DayforceHarvester", "BreezyHarvester", "ZohoHarvester", "TeamtailorHarvester",
    "HTMLScrapeHarvester", "get_harvester", "HARVESTER_MAP",
]
