"""
DayforceHarvester — Dayforce HCM (Ceridian) Career Portal

Dayforce career portals live at:
  https://jobs.dayforcehcm.com/en-US/{company}/CANDIDATEPORTAL/jobs

Dayforce has an undocumented but publicly accessible REST API that powers
their React SPA:
  GET https://jobs.dayforcehcm.com/CandidatePortal/en-US/{company}/api/jobs
      ?pagesize=50&page=1

tenant_id = company code  e.g. "theverve", "globaltransportation"
"""
import re
import time
from typing import Any

from .base import BaseHarvester, MIN_DELAY_API, DEFAULT_TIMEOUT, BOT_USER_AGENT

PAGE_SIZE = 50
MAX_PAGES = 40


class DayforceHarvester(BaseHarvester):
    platform_slug = "dayforce"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        self.last_total_available = 0
        if not tenant_id:
            return []

        raw = tenant_id.strip()
        if "|" in raw:
            slug, board_code = raw.split("|", 1)
            board_code = board_code.strip() or "CANDIDATEPORTAL"
        else:
            slug = raw
            board_code = "CANDIDATEPORTAL"
        results: list[dict] = []

        # Path 1: JSON API
        api_results = self._fetch_api(slug, board_code, company.name, fetch_all)
        if api_results:
            return api_results

        # Path 2: HTML scrape
        return self._scrape_html(slug, board_code, company.name)

    # ── Path 1: JSON API ──────────────────────────────────────────────────────

    def _fetch_api(self, slug: str, board_code: str, company_name: str, fetch_all: bool) -> list[dict]:
        import time as _t
        # Two possible API URL patterns
        api_urls = [
            f"https://jobs.dayforcehcm.com/CandidatePortal/en-US/{slug}/api/jobs",
            f"https://jobs.dayforcehcm.com/en-US/{slug}/{board_code}/api/jobs",
        ]

        for base_url in api_urls:
            results: list[dict] = []
            page = 1
            while True:
                data = self._get(base_url, params={"pagesize": PAGE_SIZE, "page": page})
                if not isinstance(data, dict) or "error" in data:
                    break

                jobs = (
                    data.get("JobPostings")
                    or data.get("jobs")
                    or data.get("Items")
                    or data.get("items")
                    or []
                )
                if not jobs:
                    break

                for j in jobs:
                    results.append(self._normalize(j, slug, company_name))

                total = int(
                    data.get("TotalCount")
                    or data.get("totalCount")
                    or data.get("total")
                    or 0
                )
                if total:
                    self.last_total_available = total
                if not fetch_all or (total and page * PAGE_SIZE >= total) or page >= MAX_PAGES:
                    break
                page += 1
                _t.sleep(MIN_DELAY_API)

            if results:
                return results

        return []

    def _normalize(self, j: dict, slug: str, company_name: str) -> dict:
        job_id = (
            j.get("JobRequisitionId")
            or j.get("id")
            or j.get("Id")
            or j.get("jobId")
            or ""
        )
        title = j.get("JobTitle") or j.get("title") or j.get("Title") or ""

        city = j.get("JobLocation") or j.get("city") or j.get("City") or ""
        state = j.get("State") or j.get("state") or ""
        country = j.get("Country") or j.get("country") or ""
        location_raw = ", ".join(x for x in [city, state, country] if x)

        is_remote = bool(
            j.get("WorkFromHome")
            or j.get("isRemote")
            or "remote" in (location_raw + title).lower()
        )
        location_type = "REMOTE" if is_remote else ("ONSITE" if location_raw else "UNKNOWN")

        job_url = (
            j.get("ApplyUrl")
            or j.get("applyUrl")
            or f"https://jobs.dayforcehcm.com/en-US/{slug}/CANDIDATEPORTAL/jobs/{job_id}"
        )

        return {
            "external_id": str(job_id),
            "original_url": job_url,
            "apply_url": job_url,
            "title": title,
            "company_name": company_name,
            "department": j.get("Department") or j.get("department") or "",
            "team": "",
            "location_raw": location_raw,
            "city": city,
            "state": state,
            "country": country,
            "is_remote": is_remote,
            "location_type": location_type,
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
            "posted_date_raw": j.get("PostedDate") or j.get("postedDate") or "",
            "closing_date": "",
            "raw_payload": j,
        }

    # ── Path 2: HTML scrape ───────────────────────────────────────────────────

    def _scrape_html(self, slug: str, board_code: str, company_name: str) -> list[dict]:
        import time as _t
        url = f"https://jobs.dayforcehcm.com/en-US/{slug}/{board_code}/jobs"
        self._enforce_rate_limit()
        try:
            resp = self._session.get(
                url, timeout=DEFAULT_TIMEOUT,
                headers={"User-Agent": BOT_USER_AGENT, "Accept": "text/html"},
            )
            self._last_request_at = _t.monotonic()
            if not resp.ok:
                return []
            html = resp.text
        except Exception:
            return []

        results: list[dict] = []
        seen: set[str] = set()
        base = f"https://jobs.dayforcehcm.com/en-US/{slug}/{board_code}/jobs"

        # Look for job links in the SPA HTML
        for m in re.finditer(
            rf'href=["\']([^"\']*{re.escape(board_code)}/jobs/\d+[^"\']*)["\']',
            html, re.I,
        ):
            job_url = m.group(1)
            if not job_url.startswith("http"):
                job_url = f"https://jobs.dayforcehcm.com{job_url}"
            if job_url in seen:
                continue
            seen.add(job_url)
            results.append({
                "external_id": "",
                "original_url": job_url,
                "apply_url": job_url,
                "title": "Untitled Position",
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
                "raw_payload": {"source": "html_scrape", "board_code": board_code},
            })
        return results
