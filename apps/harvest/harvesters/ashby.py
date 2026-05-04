"""
AshbyHarvester — Public Ashby GraphQL API

Ashby exposes a publicly accessible GraphQL endpoint used by their own
job board widgets. It returns only published, public-facing postings.

Endpoint: https://jobs.ashbyhq.com/api/non-user-graphql

Two-step approach (confirmed working 2025):

  Step 1 — List all jobs
    Query: jobBoardWithTeams
    Returns: JobPostingBriefsWithIdsAndTeamId for each job
    Fields: id, title, locationName, workplaceType, employmentType,
            compensationTierSummary, secondaryLocations, teamId

  Step 2 — Fetch full details per job
    Query: jobPosting(jobPostingId, organizationHostedJobsPageName)
    Returns: JobPostingDetails
    Fields: id, title, descriptionHtml, departmentName, teamNames,
            locationName, locationAddress (String), workplaceType,
            employmentType, compensationTierSummary, publishedDate

NOTE: The `jobBoard` query and the `descriptionHtml` field on
`JobPostingBriefsWithIdsAndTeamId` do NOT exist in the live schema
(introspected 2025-04-19). Per-job detail calls are required for JD.

Compliance:
  - Honest User-Agent (inherited from BaseHarvester)
  - 1-second minimum delay (BaseHarvester rate limit)
  - Retry + backoff on server errors (BaseHarvester)
"""
import re
import time
from typing import Any

from .base import BaseHarvester, MIN_DELAY_API

GQL_URL = "https://jobs.ashbyhq.com/api/non-user-graphql"

# Step 1: list query — returns brief job info (no description)
ASHBY_LIST_QUERY = """
query jobBoardWithTeams($organizationHostedJobsPageName: String!) {
  jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
    jobPostings {
      id
      title
      locationName
      workplaceType
      employmentType
      compensationTierSummary
      teamId
      secondaryLocations {
        locationName
      }
    }
    teams {
      id
      name
      parentTeamId
    }
  }
}
"""

# Step 2: per-job detail query — returns descriptionHtml + department + location details
ASHBY_DETAIL_QUERY = """
query jobPosting($jobPostingId: String!, $organizationHostedJobsPageName: String!) {
  jobPosting(
    jobPostingId: $jobPostingId
    organizationHostedJobsPageName: $organizationHostedJobsPageName
  ) {
    id
    title
    descriptionHtml
    departmentName
    teamNames
    locationName
    locationAddress
    workplaceType
    employmentType
    compensationTierSummary
    publishedDate
  }
}
"""

ETYPE_MAP = {
    "FullTime":    "FULL_TIME",
    "PartTime":    "PART_TIME",
    "Contract":    "CONTRACT",
    "Contractor":  "CONTRACT",
    "Internship":  "INTERNSHIP",
    "Temporary":   "TEMPORARY",
}


def _parse_comp_summary(summary: str) -> tuple:
    """Parse Ashby compensationTierSummary string.

    Examples:
      "$144K – $220K • Offers Equity • Offers Commission • Multiple Ranges"
      "$90K – $120K • Offers Equity"
      "$50/hr – $75/hr"
    Returns: (sal_min, sal_max, currency, period, salary_raw)
    """
    if not summary:
        return None, None, "USD", "", ""

    salary_raw = summary
    currency = "USD"
    period = "YEAR"  # default for salaried roles

    # Detect hourly
    if "/hr" in summary.lower() or "hour" in summary.lower():
        period = "HOUR"

    # Extract numeric values — handles K suffix
    nums = []
    for m in re.finditer(r'\$([0-9]+(?:\.[0-9]+)?)\s*([Kk])?', summary):
        val = float(m.group(1))
        if m.group(2):
            val *= 1000
        if val > 0:
            nums.append(val)

    sal_min = min(nums) if nums else None
    sal_max = max(nums) if len(nums) > 1 else sal_min

    return sal_min, sal_max, currency, period, salary_raw


def _detect_location_type(location_raw: str, workplace_type: str) -> tuple[str, bool]:
    """Map Ashby workplaceType (Remote|Hybrid|OnSite) to our location_type/is_remote."""
    wt = (workplace_type or "").lower()
    if wt == "remote":
        return "REMOTE", True
    if wt == "hybrid":
        return "HYBRID", False
    if wt == "onsite":
        return "ONSITE", False
    # Fallback: check location name text
    loc_lower = (location_raw or "").lower()
    if "remote" in loc_lower:
        return "REMOTE", True
    if "hybrid" in loc_lower:
        return "HYBRID", False
    if location_raw and location_raw.strip():
        return "ONSITE", False
    return "UNKNOWN", False


def _detect_experience_level(title: str, description: str) -> str:
    combined = (title + " " + description).lower()
    if any(k in combined for k in ("intern", "internship", "co-op", "coop")):
        return "ENTRY"
    if any(k in combined for k in ("chief ", "cto", "ceo", "coo", "cfo", "svp", "evp", "vp ", "vice president")):
        return "EXECUTIVE"
    if any(k in combined for k in ("director", "head of")):
        return "DIRECTOR"
    if any(k in combined for k in ("manager", "mgr")):
        return "MANAGER"
    if any(k in combined for k in ("lead ", "principal", "staff ")):
        return "LEAD"
    if any(k in combined for k in ("senior", "sr.", "sr ")):
        return "SENIOR"
    if any(k in combined for k in ("junior", "jr.", "jr ", "entry", "associate")):
        return "ENTRY"
    return "MID"


def _parse_location_string(location_address: str) -> tuple[str, str, str]:
    """
    Ashby `locationAddress` is a plain string like 'San Francisco, CA, US'.
    Parse it into (city, state, country).
    """
    if not location_address:
        return "", "", ""
    parts = [p.strip() for p in location_address.split(",")]
    city    = parts[0] if len(parts) >= 1 else ""
    state   = parts[1] if len(parts) >= 2 else ""
    country = parts[2] if len(parts) >= 3 else ""
    return city, state, country


class AshbyHarvester(BaseHarvester):
    """Harvests jobs from Ashby public GraphQL API."""

    platform_slug = "ashby"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        if not tenant_id:
            return []

        # ── Step 1: fetch job list ────────────────────────────────────────────
        list_payload = {
            "operationName": "jobBoardWithTeams",
            "query": ASHBY_LIST_QUERY,
            "variables": {"organizationHostedJobsPageName": tenant_id},
        }

        data = self._post(GQL_URL, json_data=list_payload)
        if isinstance(data, dict) and "error" in data:
            return []

        board = (data.get("data") or {}).get("jobBoardWithTeams") or {}
        brief_postings = board.get("jobPostings") or []
        teams_list = board.get("teams") or []
        self.last_total_available = len(brief_postings)

        # Build team_id → team_name lookup
        team_lookup: dict[str, str] = {
            t["id"]: t.get("name", "") for t in teams_list if t.get("id")
        }

        results = []
        for brief in brief_postings:
            job_id   = brief.get("id", "")
            title    = brief.get("title", "")
            loc_name = brief.get("locationName", "") or ""
            workplace_type = brief.get("workplaceType", "") or ""
            employment_type = ETYPE_MAP.get(brief.get("employmentType", ""), "UNKNOWN")
            comp_summary = brief.get("compensationTierSummary") or ""
            team_id = brief.get("teamId", "") or ""
            team_name = team_lookup.get(team_id, "")

            job_url = f"https://jobs.ashbyhq.com/{tenant_id}/{job_id}"

            location_type, is_remote = _detect_location_type(loc_name, workplace_type)

            sal_min, sal_max, salary_currency, salary_period, salary_raw = (
                _parse_comp_summary(comp_summary)
            )

            # ── Step 2: fetch full details (description, department, location) ─
            description  = ""
            dept         = ""
            city = state = country = ""
            posted_date  = ""

            if job_id:
                try:
                    detail_payload = {
                        "operationName": "jobPosting",
                        "query": ASHBY_DETAIL_QUERY,
                        "variables": {
                            "jobPostingId": job_id,
                            "organizationHostedJobsPageName": tenant_id,
                        },
                    }
                    detail_data = self._post(GQL_URL, json_data=detail_payload)
                    if isinstance(detail_data, dict) and "error" not in detail_data:
                        p = (detail_data.get("data") or {}).get("jobPosting") or {}
                        description  = p.get("descriptionHtml") or ""
                        dept         = p.get("departmentName") or ""
                        if not team_name:
                            team_names = p.get("teamNames") or []
                            team_name  = team_names[0] if team_names else ""
                        # locationAddress is a plain string "City, State, Country"
                        loc_addr = p.get("locationAddress") or ""
                        city, state, country = _parse_location_string(loc_addr)
                        if not loc_name:
                            loc_name = p.get("locationName") or ""
                        posted_date = p.get("publishedDate") or ""
                        # Update comp if list had none
                        if not comp_summary and p.get("compensationTierSummary"):
                            comp_summary = p["compensationTierSummary"]
                            sal_min, sal_max, salary_currency, salary_period, salary_raw = (
                                _parse_comp_summary(comp_summary)
                            )
                except Exception:
                    pass  # description stays empty — backfill will handle it
                time.sleep(MIN_DELAY_API)

            experience_level = _detect_experience_level(title, description[:500])

            results.append({
                "external_id":      job_id,
                "original_url":     job_url,
                "apply_url":        job_url,
                "title":            title,
                "company_name":     company.name,
                "department":       dept,
                "team":             team_name,
                "location_raw":     loc_name,
                "city":             city,
                "state":            state,
                "country":          country,
                "is_remote":        is_remote,
                "location_type":    location_type,
                "employment_type":  employment_type,
                "experience_level": experience_level,
                "salary_min":       sal_min,
                "salary_max":       sal_max,
                "salary_currency":  salary_currency,
                "salary_period":    salary_period,
                "salary_raw":       salary_raw,
                "description":      description,
                "requirements":     "",
                "responsibilities": "",
                "benefits":         "",
                "posted_date_raw":  posted_date,
                "closing_date":     "",
                "raw_payload":      brief,
            })

        return results
