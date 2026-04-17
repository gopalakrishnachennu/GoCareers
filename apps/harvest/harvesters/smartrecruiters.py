"""
SmartRecruitersHarvester — Public SmartRecruiters Postings API

SmartRecruiters provides a documented public REST API for job postings:
  GET https://api.smartrecruiters.com/v1/companies/{company}/postings
      ?limit=100&offset=0

No authentication required for published postings.
"""
import time
from typing import Any

from .base import BaseHarvester, MIN_DELAY_API

PAGE_SIZE = 100


class SmartRecruitersHarvester(BaseHarvester):
    platform_slug = "smartrecruiters"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        if not tenant_id:
            return []

        slug = tenant_id.strip()
        base_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"

        results: list[dict] = []
        offset = 0

        while True:
            data = self._get(base_url, params={"limit": PAGE_SIZE, "offset": offset})
            if not isinstance(data, dict) or "error" in data:
                break

            postings = data.get("content") or []
            for p in postings:
                results.append(self._normalize(p, slug, company.name))

            total = int(data.get("totalFound") or 0)
            offset += len(postings)

            if not fetch_all or not postings or offset >= total:
                break
            time.sleep(MIN_DELAY_API)

        return results

    # ── Normalization ─────────────────────────────────────────────────────────

    def _normalize(self, p: dict, slug: str, company_name: str) -> dict:
        loc = p.get("location") or {}
        city = loc.get("city") or ""
        state = loc.get("region") or ""
        country = loc.get("country") or ""
        is_remote = bool(loc.get("remote", False))
        location_raw = ", ".join(x for x in [city, state, country] if x)
        if is_remote:
            location_type = "REMOTE"
        elif location_raw:
            location_type = "ONSITE"
        else:
            location_type = "UNKNOWN"

        dept = (p.get("department") or {}).get("label") or ""

        emp_raw = ((p.get("typeOfEmployment") or {}).get("label") or "").lower()
        emp_map = {
            "full-time": "FULL_TIME",
            "permanent": "FULL_TIME",
            "part-time": "PART_TIME",
            "contract": "CONTRACT",
            "temporary": "TEMPORARY",
            "internship": "INTERN",
            "intern": "INTERN",
            "freelance": "CONTRACT",
        }
        employment_type = emp_map.get(emp_raw, "UNKNOWN")

        exp_raw = ((p.get("experienceLevel") or {}).get("label") or "").lower()
        exp_map = {
            "entry level": "ENTRY",
            "mid level": "MID",
            "senior level": "SENIOR",
            "director": "DIRECTOR",
            "executive": "EXECUTIVE",
            "manager": "MANAGER",
        }
        experience_level = exp_map.get(exp_raw, "UNKNOWN")

        job_id = p.get("id") or ""
        job_url = (
            p.get("ref")
            or f"https://jobs.smartrecruiters.com/{slug}/{job_id}"
        )

        return {
            "external_id": job_id,
            "original_url": job_url,
            "apply_url": job_url,
            "title": p.get("name") or "",
            "company_name": company_name,
            "department": dept,
            "team": "",
            "location_raw": location_raw,
            "city": city,
            "state": state,
            "country": country,
            "is_remote": is_remote,
            "location_type": location_type,
            "employment_type": employment_type,
            "experience_level": experience_level,
            "salary_min": None,
            "salary_max": None,
            "salary_currency": "USD",
            "salary_period": "",
            "salary_raw": "",
            "description": "",
            "requirements": "",
            "benefits": "",
            "posted_date_raw": p.get("releasedDate") or "",
            "closing_date": "",
            "raw_payload": p,
        }
