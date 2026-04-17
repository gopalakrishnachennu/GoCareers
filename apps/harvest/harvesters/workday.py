"""
WorkdayHarvester — Public Workday REST API

Workday provides a PUBLICLY documented job board API at:
  https://{tenant}.myworkdayjobs.com/wday/cxs/{tenant}/{path}/jobs

This is their intended public interface for job boards. No authentication
is required. We identify ourselves honestly as GoCareers-Bot.

Compliance:
  - Honest User-Agent (inherited from BaseHarvester)
  - 1-second minimum delay between path attempts (rate_limit)
  - Max 20 results per request (their recommended page size)
  - Stops as soon as a valid path returns results (no unnecessary calls)
  - Retries with backoff on 5xx / timeouts (BaseHarvester)
  - fetch_all=True paginates through ALL results with polite delays
"""
import re as _re
import time
from typing import Any

from .base import BaseHarvester, MIN_DELAY_API

# Generic Workday job-board path fallbacks (used only when no specific path
# is stored in tenant_id). Real paths are highly company-specific.
WORKDAY_PATHS_FALLBACK = [
    "External",
    "EXT",
    "External_Career_Site",
    "Careers",
    "Search",
    "US",
    "All",
    "US-External",
    "Jobs",
    "Global",
]

PAGE_SIZE = 20


def _normalize_workday_job(job: dict, job_domain: str, company_name: str) -> dict:
    """Normalize a single Workday job posting dict to the canonical RawJob schema."""
    ext_path = job.get("externalPath", "")
    job_url = (
        f"https://{job_domain}.myworkdayjobs.com{ext_path}"
        if ext_path
        else ""
    )

    location_raw = job.get("locationsText", "")
    loc_lower = location_raw.lower()
    if "remote" in loc_lower:
        is_remote = True
        location_type = "REMOTE"
    elif "hybrid" in loc_lower:
        is_remote = False
        location_type = "HYBRID"
    elif location_raw:
        is_remote = False
        location_type = "ONSITE"
    else:
        is_remote = False
        location_type = "UNKNOWN"

    title = job.get("title", "")
    exp_level = _detect_experience_level(title, "")

    # Workday bullet fields sometimes contain the req ID
    ext_id = ""
    bullet = job.get("bulletFields", [])
    if bullet:
        ext_id = bullet[0] if bullet else ""

    return {
        "external_id": ext_id,
        "original_url": job_url,
        "apply_url": job_url,
        "title": title,
        "company_name": company_name,
        "department": "",
        "team": "",
        "location_raw": location_raw,
        "city": "",
        "state": "",
        "country": "",
        "is_remote": is_remote,
        "location_type": location_type,
        "employment_type": "UNKNOWN",
        "experience_level": exp_level,
        "salary_min": None,
        "salary_max": None,
        "salary_currency": "USD",
        "salary_period": "",
        "salary_raw": "",
        "description": "",
        "requirements": "",
        "benefits": "",
        "posted_date_raw": job.get("postedOn", ""),
        "closing_date": "",
        "raw_payload": job,
    }


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
    if any(k in combined for k in ("junior", "jr.", "jr ", "entry", "associate", "i ", "level 1")):
        return "ENTRY"
    return "MID"


class WorkdayHarvester(BaseHarvester):
    """Harvests jobs from Workday public REST API."""

    platform_slug = "workday"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        if not tenant_id:
            return []

        # tenant_id stored as "{full_subdomain}|{jobboard}"
        # e.g. "inotivco.wd5|EXT" or legacy "inotivco|EXT"
        if "|" in tenant_id:
            full_subdomain, jobboard = tenant_id.split("|", 1)
            tenant = _re.sub(r"\.wd\d+$", "", full_subdomain, flags=_re.I)
            paths_to_try = [jobboard] + [
                p for p in WORKDAY_PATHS_FALLBACK if p.lower() != jobboard.lower()
            ]
        else:
            full_subdomain = tenant_id
            tenant = _re.sub(r"\.wd\d+$", "", tenant_id, flags=_re.I)
            paths_to_try = [tenant] + WORKDAY_PATHS_FALLBACK

        job_domain = full_subdomain

        for path in paths_to_try:
            url = (
                f"https://{tenant}.myworkdayjobs.com"
                f"/wday/cxs/{tenant}/{path}/jobs"
            )

            # ── First page ────────────────────────────────────────────────────
            payload = {
                "appliedFacets": {},
                "limit": PAGE_SIZE,
                "offset": 0,
                "searchText": "",
            }
            data = self._post(url, json_data=payload)

            if not isinstance(data, dict) or "error" in data:
                time.sleep(MIN_DELAY_API)
                continue

            postings = data.get("jobPostings") or []
            if not postings:
                time.sleep(MIN_DELAY_API)
                continue

            # Found a valid path — collect results
            results = [_normalize_workday_job(j, job_domain, company.name) for j in postings]

            if fetch_all:
                total = data.get("total", len(postings))
                offset = PAGE_SIZE
                while offset < total:
                    time.sleep(MIN_DELAY_API)
                    next_payload = {
                        "appliedFacets": {},
                        "limit": PAGE_SIZE,
                        "offset": offset,
                        "searchText": "",
                    }
                    next_data = self._post(url, json_data=next_payload)
                    if not isinstance(next_data, dict) or "error" in next_data:
                        break
                    page_postings = next_data.get("jobPostings") or []
                    if not page_postings:
                        break
                    results.extend(
                        _normalize_workday_job(j, job_domain, company.name)
                        for j in page_postings
                    )
                    offset += PAGE_SIZE

            return results

        return []
