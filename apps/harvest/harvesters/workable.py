"""
WorkableHarvester — Public Workable Jobs API

Workable exposes a public undocumented but stable REST API:
  POST https://apply.workable.com/api/v3/accounts/{company}/jobs
  Body: {"limit": 100, "details": false}

Pagination via paging.next token field.
No authentication required for published postings.
"""
import time
from typing import Any

from .base import BaseHarvester, MIN_DELAY_API

PAGE_SIZE = 100


class WorkableHarvester(BaseHarvester):
    platform_slug = "workable"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        if not tenant_id:
            return []

        slug = tenant_id.strip().strip("/")
        url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"

        results: list[dict] = []
        token: str | None = None

        while True:
            # Workable v3 rejects legacy "limit/details" body keys with 400.
            # Empty body works and returns {"total","results","paging"}.
            payload: dict = {}
            if token:
                payload["token"] = token

            data = self._post(url, json_data=payload)
            if not isinstance(data, dict) or "error" in data:
                break

            page_jobs = data.get("results") or []
            for job in page_jobs:
                results.append(self._normalize(job, slug, company.name))

            paging = data.get("paging") or {}
            total = data.get("total") or paging.get("count") or 0
            if total:
                self.last_total_available = int(total)
            token = paging.get("next")

            if not fetch_all or not token:
                break
            time.sleep(MIN_DELAY_API)

        return results

    # ── Normalization ─────────────────────────────────────────────────────────

    def _normalize(self, job: dict, slug: str, company_name: str) -> dict:
        loc = job.get("location") or {}
        location_raw = loc.get("location_str") or ""
        is_remote = bool(loc.get("telecommuting", False))
        if is_remote:
            location_type = "REMOTE"
        elif location_raw:
            location_type = "ONSITE"
        else:
            location_type = "UNKNOWN"

        dept_raw = job.get("department") or []
        if isinstance(dept_raw, list):
            dept = dept_raw[0] if dept_raw else ""
        else:
            dept = str(dept_raw) if dept_raw else ""

        emp_raw = (job.get("employment_type") or "").upper().replace("-", "_")
        emp_map = {
            "FULL_TIME": "FULL_TIME",
            "PART_TIME": "PART_TIME",
            "CONTRACT": "CONTRACT",
            "TEMPORARY": "TEMPORARY",
            "INTERN": "INTERNSHIP",
            "INTERNSHIP": "INTERNSHIP",
        }
        employment_type = emp_map.get(emp_raw, "UNKNOWN")

        shortcode = job.get("shortcode") or ""
        app_url = (
            job.get("application_url")
            or f"https://apply.workable.com/{slug}/j/{shortcode}"
        )

        return {
            "external_id": str(job.get("id") or shortcode),
            "original_url": app_url,
            "apply_url": app_url,
            "title": job.get("title") or "",
            "company_name": company_name,
            "department": dept,
            "team": "",
            "location_raw": location_raw,
            "city": "",
            "state": "",
            "country": "",
            "is_remote": is_remote,
            "location_type": location_type,
            "employment_type": employment_type,
            "experience_level": "UNKNOWN",
            "salary_min": None,
            "salary_max": None,
            "salary_currency": "USD",
            "salary_period": "",
            "salary_raw": "",
            "description": "",
            "requirements": "",
            "benefits": "",
            "posted_date_raw": job.get("created_at") or "",
            "closing_date": "",
            "raw_payload": job,
        }
