"""
RecruiteeHarvester — Public Recruitee JSON API

Recruitee provides a public REST API for job offers:
  GET https://{company}.recruitee.com/api/offers/

Returns all published offers in a single response — no pagination needed.
No authentication required for public postings.
"""
from typing import Any

from .base import BaseHarvester


class RecruiteeHarvester(BaseHarvester):
    platform_slug = "recruitee"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        if not tenant_id:
            return []

        slug = tenant_id.strip()
        url = f"https://{slug}.recruitee.com/api/offers/"

        data = self._get(url)
        if not isinstance(data, dict) or "error" in data:
            return []

        offers = data.get("offers") or []
        self.last_total_available = len(offers)
        return [self._normalize(o, slug, company.name) for o in offers]

    # ── Normalization ─────────────────────────────────────────────────────────

    def _normalize(self, o: dict, slug: str, company_name: str) -> dict:
        city = o.get("city") or ""
        country = o.get("country") or ""
        is_remote = bool(o.get("remote", False))
        location_raw = ", ".join(x for x in [city, country] if x)
        if is_remote:
            location_type = "REMOTE"
        elif location_raw:
            location_type = "ONSITE"
        else:
            location_type = "UNKNOWN"

        kind = (o.get("kind") or "").lower()
        emp_map = {
            "full_time": "FULL_TIME",
            "part_time": "PART_TIME",
            "contract": "CONTRACT",
            "freelance": "CONTRACT",
            "internship": "INTERN",
            "temporary": "TEMPORARY",
        }
        employment_type = emp_map.get(kind, "UNKNOWN")

        job_slug = o.get("slug") or str(o.get("id") or "")
        careers_url = (
            o.get("careers_url")
            or f"https://{slug}.recruitee.com/o/{job_slug}"
        )

        return {
            "external_id": str(o.get("id") or ""),
            "original_url": careers_url,
            "apply_url": careers_url,
            "title": o.get("title") or "",
            "company_name": company_name,
            "department": o.get("department") or "",
            "team": "",
            "location_raw": location_raw,
            "city": city,
            "state": "",
            "country": country,
            "is_remote": is_remote,
            "location_type": location_type,
            "employment_type": employment_type,
            "experience_level": "UNKNOWN",
            "salary_min": None,
            "salary_max": None,
            "salary_currency": "USD",
            "salary_period": "",
            "salary_raw": "",
            # Recruitee list API returns full description HTML — use it directly
            "description": o.get("description") or "",
            "requirements": o.get("requirements") or "",
            "benefits": "",
            "posted_date_raw": o.get("created_at") or "",
            "closing_date": o.get("ends_at") or "",
            "raw_payload": o,
        }
