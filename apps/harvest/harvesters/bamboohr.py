"""
BambooHRHarvester — Public BambooHR Jobs API

BambooHR exposes a public JSON API for career listings:
  GET https://{company}.bamboohr.com/careers/list

The endpoint returns a JSON array (no auth required for published jobs).
Each object has: id, title, jobOpeningName, locationCity, locationState,
locationCountry, department, employmentStatusLabel, isRemote, link.

tenant_id = subdomain slug e.g. "netflix", "acme"
"""
import time
import re
from typing import Any
from urllib.parse import urljoin

from .base import BaseHarvester, MIN_DELAY_SCRAPE, DEFAULT_TIMEOUT, BOT_USER_AGENT


class BambooHRHarvester(BaseHarvester):
    platform_slug = "bamboohr"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        if not tenant_id:
            return []

        slug = tenant_id.strip()

        # Path 1: JSON list endpoint (preferred — returns clean structured data)
        jobs = self._fetch_json_list(slug, company.name)
        if jobs:
            return jobs

        # Path 2: Embed JS script fallback (some tenants redirect list→HTML)
        return self._fetch_embed(slug, company.name)

    # ── Path 1: JSON list ─────────────────────────────────────────────────────

    def _fetch_json_list(self, slug: str, company_name: str) -> list[dict]:
        import time as _time
        url = f"https://{slug}.bamboohr.com/careers/list"

        self._enforce_rate_limit()
        try:
            resp = self._session.get(
                url,
                timeout=DEFAULT_TIMEOUT,
                headers={
                    "User-Agent": BOT_USER_AGENT,
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
            self._last_request_at = _time.monotonic()

            if not resp.ok:
                return []

            ct = resp.headers.get("Content-Type", "")
            if "json" not in ct:
                # Try to parse anyway — BambooHR sometimes serves JSON with wrong CT
                try:
                    data = resp.json()
                except Exception:
                    return []
            else:
                data = resp.json()

            if not isinstance(data, list):
                return []

            return [self._normalize_list(j, slug, company_name) for j in data]

        except Exception:
            return []

    def _normalize_list(self, j: dict, slug: str, company_name: str) -> dict:
        city = j.get("locationCity") or j.get("city") or ""
        state = j.get("locationState") or j.get("state") or ""
        country = j.get("locationCountry") or j.get("country") or ""
        is_remote = bool(j.get("isRemote", False))
        location_raw = ", ".join(x for x in [city, state, country] if x)
        if is_remote:
            location_type = "REMOTE"
        elif location_raw:
            location_type = "ONSITE"
        else:
            location_type = "UNKNOWN"

        emp_raw = (j.get("employmentStatusLabel") or "").lower()
        emp_map = {
            "full-time": "FULL_TIME",
            "full time": "FULL_TIME",
            "part-time": "PART_TIME",
            "part time": "PART_TIME",
            "contractor": "CONTRACT",
            "contract": "CONTRACT",
            "temporary": "TEMPORARY",
            "intern": "INTERN",
            "internship": "INTERN",
        }
        employment_type = emp_map.get(emp_raw, "UNKNOWN")

        job_id = j.get("id") or ""
        job_url = (
            j.get("link")
            or j.get("url")
            or f"https://{slug}.bamboohr.com/careers/{job_id}"
        )

        return {
            "external_id": str(job_id),
            "original_url": job_url,
            "apply_url": job_url,
            "title": j.get("jobOpeningName") or j.get("title") or "",
            "company_name": company_name,
            "department": j.get("department") or j.get("departmentLabel") or "",
            "team": "",
            "location_raw": location_raw,
            "city": city,
            "state": state,
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
            "description": "",
            "requirements": "",
            "benefits": "",
            "posted_date_raw": j.get("datePosted") or j.get("created_at") or "",
            "closing_date": "",
            "raw_payload": j,
        }

    # ── Path 2: HTML embed scraper fallback ───────────────────────────────────

    def _fetch_embed(self, slug: str, company_name: str) -> list[dict]:
        import time as _time
        url = f"https://{slug}.bamboohr.com/careers"

        self._enforce_rate_limit()
        try:
            resp = self._session.get(
                url,
                timeout=DEFAULT_TIMEOUT,
                headers={
                    "User-Agent": BOT_USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
            self._last_request_at = _time.monotonic()
            if not resp.ok:
                return []
            return self._parse_careers_html(resp.text, slug, company_name)
        except Exception:
            return []

    def _parse_careers_html(self, html: str, slug: str, company_name: str) -> list[dict]:
        results: list[dict] = []
        seen: set[str] = set()
        base = f"https://{slug}.bamboohr.com"

        # BambooHR career page: links like /careers/{id}-{title}
        for m in re.finditer(
            r'<a[^>]*href=["\'](/careers/(\d+)[^"\']*)["\'][^>]*>([\s\S]*?)</a>',
            html, re.I,
        ):
            path = m.group(1)
            job_id = m.group(2)
            link_html = m.group(3)
            abs_url = urljoin(base, path)
            if abs_url in seen:
                continue
            seen.add(abs_url)
            title = re.sub(r"<[^>]+>", " ", link_html).strip()
            if not title or len(title) < 3 or len(title) > 300:
                continue
            results.append({
                "external_id": job_id,
                "original_url": abs_url,
                "apply_url": abs_url,
                "title": title,
                "company_name": company_name,
                "department": "",
                "team": "",
                "location_raw": "",
                "city": "",
                "state": "",
                "country": "",
                "is_remote": False,
                "location_type": "UNKNOWN",
                "employment_type": "UNKNOWN",
                "experience_level": "UNKNOWN",
                "salary_min": None,
                "salary_max": None,
                "salary_currency": "USD",
                "salary_period": "",
                "salary_raw": "",
                "description": "",
                "requirements": "",
                "benefits": "",
                "posted_date_raw": "",
                "closing_date": "",
                "raw_payload": {"source": "html_fallback"},
            })

        return results
