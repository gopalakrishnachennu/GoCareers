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

# jobBoard: returns full job postings including descriptionHtml, team, salary.
# This replaces the old jobBoardWithTeams query which only returned brief fields
# and had no description. jobBoard uses the JobPosting type which has all fields.
ASHBY_QUERY = """
query jobBoard($organizationHostedJobsPageName: String!) {
  jobBoard(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
    jobPostings {
      id
      title
      descriptionHtml
      locationName
      workplaceType
      employmentType
      compensationTierSummary
      publishedDate
      team {
        id
        name
        parentTeamId
      }
      department {
        id
        name
      }
      location {
        id
        name
        city
        region
        regionCode
        countryCode
        isRemote
      }
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
            "operationName": "jobBoard",
            "query": ASHBY_QUERY,
            "variables": {"organizationHostedJobsPageName": tenant_id},
        }

        data = self._post(GQL_URL, json_data=payload)
        if isinstance(data, dict) and "error" in data:
            return []

        board = (data.get("data") or {}).get("jobBoard") or {}
        postings = board.get("jobPostings") or []
        self.last_total_available = len(postings)

        results = []
        for job in postings:
            job_id = job.get("id", "")
            job_url = f"https://jobs.ashbyhq.com/{tenant_id}/{job_id}"

            # Team from nested object
            team_obj = job.get("team") or {}
            team_name = team_obj.get("name", "") if isinstance(team_obj, dict) else ""

            # Department from nested object
            dept_obj = job.get("department") or {}
            dept = dept_obj.get("name", "") if isinstance(dept_obj, dict) else ""

            # Location — use nested location object when available, fall back to locationName
            loc_obj = job.get("location") or {}
            if isinstance(loc_obj, dict) and loc_obj:
                location_raw = loc_obj.get("name") or job.get("locationName", "") or ""
                city    = loc_obj.get("city", "") or ""
                state   = loc_obj.get("region", "") or ""
                country = loc_obj.get("countryCode", "") or ""
                remote_flag = bool(loc_obj.get("isRemote", False))
            else:
                location_raw = job.get("locationName", "") or ""
                city = state = country = ""
                remote_flag = False

            workplace_type = job.get("workplaceType", "") or ""
            location_type, is_remote = _detect_location_type(location_raw, workplace_type)
            is_remote = is_remote or remote_flag

            employment_type = ETYPE_MAP.get(job.get("employmentType", ""), "UNKNOWN")

            comp_summary = job.get("compensationTierSummary") or ""
            sal_min, sal_max, salary_currency, salary_period, salary_raw = (
                _parse_comp_summary(comp_summary)
            )

            # ── Description — now included in jobBoard query ──────────────
            description = job.get("descriptionHtml") or ""
            experience_level = _detect_experience_level(job.get("title", ""), description[:500])

            results.append({
                "external_id": job_id,
                "original_url": job_url,
                "apply_url": job_url,
                "title": job.get("title", ""),
                "company_name": company.name,
                "department": dept,
                "team": team_name,
                "location_raw": location_raw,
                "city": city,
                "state": state,
                "country": country,
                "is_remote": is_remote,
                "location_type": location_type,
                "employment_type": employment_type,
                "experience_level": experience_level,
                "salary_min": sal_min,
                "salary_max": sal_max,
                "salary_currency": salary_currency,
                "salary_period": salary_period,
                "salary_raw": salary_raw,
                "description": description,
                "requirements": "",
                "benefits": "",
                "posted_date_raw": job.get("publishedDate") or "",
                "closing_date": "",
                "raw_payload": job,
            })

        return results
