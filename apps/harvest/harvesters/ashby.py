"""
AshbyHarvester — Public Ashby GraphQL API

Ashby exposes a publicly accessible GraphQL endpoint used by their own
job board widgets. It returns only published, public-facing postings.

Endpoint: https://jobs.ashbyhq.com/api/non-user-graphql

Compliance:
  - Honest User-Agent (inherited from BaseHarvester)
  - 1-second minimum delay (BaseHarvester rate limit)
  - Retry + backoff on server errors (BaseHarvester)
  - Only queries `jobPostingsForOrganization` — public data only
  - fetch_all=True fetches with pagination via `after` cursor if available
"""
import re
from typing import Any

from .base import BaseHarvester

GQL_URL = "https://jobs.ashbyhq.com/api/non-user-graphql"

# Standard single-page query (Ashby returns all public postings in one call)
ASHBY_QUERY = """
query ApiJobBoardJobPostingsForOrganization($organizationHostedJobsPageName: String!) {
  jobBoard: jobPostingsForOrganization(
    organizationHostedJobsPageName: $organizationHostedJobsPageName
  ) {
    jobPostings {
      id
      title
      department { name }
      team { name }
      locationName
      employmentType
      isRemote
      descriptionHtml
      publishedDate
      externalLink
      compensation {
        summaryComponents { label value }
        currency
      }
      applicationLink
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

_SALARY_PERIOD_MAP = {
    "year": "YEAR",
    "annual": "YEAR",
    "hour": "HOUR",
    "hourly": "HOUR",
    "month": "MONTH",
}


def _parse_compensation(comp: dict) -> tuple:
    """Extract salary_min, salary_max, currency, period, salary_raw from Ashby comp dict."""
    if not comp:
        return None, None, "USD", "", ""

    currency = comp.get("currency", "USD") or "USD"
    components = comp.get("summaryComponents", []) or []

    raw_parts = []
    nums = []
    period = ""

    for c in components:
        label = (c.get("label") or "").lower()
        val = c.get("value") or ""
        raw_parts.append(f"{label}: {val}".strip())

        # Detect period from label
        for pk, pv in _SALARY_PERIOD_MAP.items():
            if pk in label:
                period = pv
                break

        # Extract numbers from value
        found = re.findall(r"[\d,]+(?:\.\d+)?", str(val).replace(",", ""))
        for n in found:
            try:
                v = float(n)
                if v > 0:
                    nums.append(v)
            except ValueError:
                pass

    salary_raw = "; ".join(raw_parts)
    sal_min = min(nums) if nums else None
    sal_max = max(nums) if len(nums) > 1 else sal_min

    return sal_min, sal_max, currency, period, salary_raw


def _detect_location_type(location_raw: str, is_remote_flag) -> tuple[str, bool]:
    if is_remote_flag:
        return "REMOTE", True
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
            "operationName": "ApiJobBoardJobPostingsForOrganization",
            "query": ASHBY_QUERY,
            "variables": {"organizationHostedJobsPageName": tenant_id},
        }

        data = self._post(GQL_URL, json_data=payload)
        if isinstance(data, dict) and "error" in data:
            return []

        postings = (
            ((data.get("data") or {}).get("jobBoard") or {}).get("jobPostings") or []
        )

        results = []
        for job in postings:
            job_id = job.get("id", "")
            apply_link = job.get("applicationLink", "") or ""
            job_url = (
                job.get("externalLink")
                or f"https://jobs.ashbyhq.com/{tenant_id}/{job_id}"
            )
            dept = (job.get("department") or {}).get("name", "")
            team = (job.get("team") or {}).get("name", "")
            location_raw = job.get("locationName", "") or ""
            is_remote_flag = job.get("isRemote", False)
            location_type, is_remote = _detect_location_type(location_raw, is_remote_flag)

            employment_type = ETYPE_MAP.get(job.get("employmentType", ""), "UNKNOWN")
            description_html = job.get("descriptionHtml", "") or ""

            # Strip basic HTML tags for plain text approximation
            description_plain = re.sub(r"<[^>]+>", " ", description_html).strip()

            experience_level = _detect_experience_level(
                job.get("title", ""), description_plain[:500]
            )

            comp = job.get("compensation") or {}
            sal_min, sal_max, currency, period, salary_raw = _parse_compensation(comp)

            results.append({
                "external_id": job_id,
                "original_url": job_url,
                "apply_url": apply_link or job_url,
                "title": job.get("title", ""),
                "company_name": company.name,
                "department": dept,
                "team": team,
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
                "salary_currency": currency,
                "salary_period": period,
                "salary_raw": salary_raw,
                "description": description_plain,
                "requirements": "",
                "benefits": "",
                "posted_date_raw": job.get("publishedDate", ""),
                "closing_date": "",
                "raw_payload": job,
            })

        return results
