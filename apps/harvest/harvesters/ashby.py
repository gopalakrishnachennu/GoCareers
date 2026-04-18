"""
AshbyHarvester — Public Ashby GraphQL API

Ashby exposes a publicly accessible GraphQL endpoint used by their own
job board widgets. It returns only published, public-facing postings.

Endpoint: https://jobs.ashbyhq.com/api/non-user-graphql

Query: jobBoardWithTeams — returns all public job postings for an org.
The old `jobPostingsForOrganization` field was removed from the schema;
`jobBoardWithTeams` is the current public API (confirmed 2024+).

JobPostingBriefsWithIdsAndTeamId fields available:
  id, title, locationName, locationAddress, locationId, teamId,
  workplaceType (Remote|Hybrid|OnSite), employmentType, secondaryLocations,
  compensationTierSummary

Compliance:
  - Honest User-Agent (inherited from BaseHarvester)
  - 1-second minimum delay (BaseHarvester rate limit)
  - Retry + backoff on server errors (BaseHarvester)
"""
import re
from typing import Any

from .base import BaseHarvester

GQL_URL = "https://jobs.ashbyhq.com/api/non-user-graphql"

# jobBoardWithTeams: returns brief job listings + team structure.
# Uses only fields confirmed via GQL introspection (JobPostingBriefsWithIdsAndTeamId).
ASHBY_QUERY = """
query jobBoardWithTeams($organizationHostedJobsPageName: String!) {
  jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
    teams {
      id
      name
      parentTeamId
    }
    jobPostings {
      id
      title
      locationName
      workplaceType
      employmentType
      teamId
      compensationTierSummary
    }
  }
}
"""

ETYPE_MAP = {
    "FullTime":   "FULL_TIME",
    "PartTime":   "PART_TIME",
    "Contract":   "CONTRACT",
    "Contractor": "CONTRACT",
    "Internship": "INTERNSHIP",
    "Temporary":  "TEMPORARY",
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


class AshbyHarvester(BaseHarvester):
    """Harvests jobs from Ashby public GraphQL API."""

    platform_slug = "ashby"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        if not tenant_id:
            return []

        payload = {
            "operationName": "jobBoardWithTeams",
            "query": ASHBY_QUERY,
            "variables": {"organizationHostedJobsPageName": tenant_id},
        }

        data = self._post(GQL_URL, json_data=payload)
        if isinstance(data, dict) and "error" in data:
            return []

        board = (data.get("data") or {}).get("jobBoardWithTeams") or {}

        # Build team-id → team-name lookup
        teams_list = board.get("teams") or []
        team_map: dict[str, str] = {t["id"]: t.get("name", "") for t in teams_list if t.get("id")}

        # Top-level jobPostings array (brief records)
        postings = board.get("jobPostings") or []

        self.last_total_available = len(postings)
        results = []
        for job in postings:
            job_id = job.get("id", "")
            job_url = f"https://jobs.ashbyhq.com/{tenant_id}/{job_id}"

            team_id = job.get("teamId", "")
            team_name = team_map.get(team_id, "")

            location_raw = job.get("locationName", "") or ""
            workplace_type = job.get("workplaceType", "") or ""
            location_type, is_remote = _detect_location_type(location_raw, workplace_type)

            employment_type = ETYPE_MAP.get(job.get("employmentType", ""), "UNKNOWN")

            # Salary: compensationTierSummary is a human-readable string e.g.
            # "$144K – $220K • Offers Equity • Offers Commission"
            comp_summary = job.get("compensationTierSummary") or ""
            sal_min, sal_max, salary_currency, salary_period, salary_raw = (
                _parse_comp_summary(comp_summary)
            )

            experience_level = _detect_experience_level(job.get("title", ""), "")

            results.append({
                "external_id": job_id,
                "original_url": job_url,
                "apply_url": job_url,
                "title": job.get("title", ""),
                "company_name": company.name,
                "department": "",
                "team": team_name,
                "location_raw": location_raw,
                "city": "",
                "state": "",
                "country": "",
                "is_remote": is_remote,
                "location_type": location_type,
                "employment_type": employment_type,
                "experience_level": experience_level,
                "salary_min": sal_min,
                "salary_max": sal_max,
                "salary_currency": salary_currency,
                "salary_period": salary_period,
                "salary_raw": salary_raw,
                "description": "",
                "requirements": "",
                "benefits": "",
                "posted_date_raw": "",
                "closing_date": "",
                "raw_payload": job,
            })

        return results
