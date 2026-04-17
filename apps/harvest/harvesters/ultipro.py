"""
UltiProHarvester — UltiPro / UKG Pro Recruiting HTML Scraper

UltiPro (now UKG Pro) career portals live at:
  https://recruiting.ultipro.com/{company}/JobBoard/{jobboard_id}

The page renders a React SPA. Jobs are loaded from an internal API:
  POST https://recruiting.ultipro.com/api/recruiting/search/v1/job-board-jobs
  Body: {"companyIdentifier": "{company}", "page": 1, "pageSize": 20}

No auth required for public postings.

tenant_id stored as "{company_code}" e.g. "MCPHP" or "{company_code}|{jobboard_id}"
"""
import time
from typing import Any

from .base import BaseHarvester, MIN_DELAY_API

PAGE_SIZE = 20
MAX_PAGES = 50


class UltiProHarvester(BaseHarvester):
    platform_slug = "ultipro"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        self.last_total_available = 0
        if not tenant_id:
            return []

        # tenant_id may be "COMPANY_CODE" or "COMPANY_CODE|JobBoardId"
        if "|" in tenant_id:
            company_code, jobboard_id = tenant_id.split("|", 1)
        else:
            company_code = tenant_id
            jobboard_id = ""
        company_code = company_code.strip()

        results: list[dict] = []

        # Path 1: Board-scoped JSON API (preferred)
        api_results = self._fetch_board_api(company_code, jobboard_id, company.name, fetch_all)
        if api_results:
            return api_results

        # Path 2: Legacy JSON API (kept as backup)
        api_results = self._fetch_api(company_code, company.name, fetch_all)
        if api_results:
            return api_results

        # Path 3: HTML scrape (fallback)
        if jobboard_id:
            url = f"https://recruiting.ultipro.com/{company_code}/JobBoard/{jobboard_id}"
        else:
            url = f"https://recruiting.ultipro.com/{company_code}/JobBoard"
        return self._scrape_html(url, company.name)

    # ── Path 1: JSON API ──────────────────────────────────────────────────────

    def _fetch_board_api(
        self,
        company_code: str,
        jobboard_id: str,
        company_name: str,
        fetch_all: bool,
    ) -> list[dict]:
        import time as _t
        if not jobboard_id:
            return []

        url = (
            f"https://recruiting.ultipro.com/{company_code}/JobBoard/{jobboard_id}"
            "/JobBoardView/LoadSearchResults"
        )
        results: list[dict] = []
        skip = 0

        while True:
            payload = {
                "opportunitySearch": {
                    "Top": PAGE_SIZE,
                    "Skip": skip,
                    "Query": "",
                    "SortBy": "Relevance",
                    "Filters": [],
                }
            }
            data = self._post(url, json_data=payload)
            if not isinstance(data, dict) or "error" in data:
                break

            jobs = data.get("opportunities") or []
            if not jobs:
                break

            for j in jobs:
                results.append(self._normalize_api(j, company_code, company_name))

            total = int(data.get("totalCount") or 0)
            if total:
                self.last_total_available = total
            skip += len(jobs)
            if not fetch_all or not total or skip >= total or skip >= (MAX_PAGES * PAGE_SIZE):
                break
            _t.sleep(MIN_DELAY_API)

        return results

    def _fetch_api(self, company_code: str, company_name: str, fetch_all: bool) -> list[dict]:
        import time as _t
        url = "https://recruiting.ultipro.com/api/recruiting/search/v1/job-board-jobs"

        results: list[dict] = []
        page = 1
        while True:
            payload = {
                "companyIdentifier": company_code,
                "page": page,
                "pageSize": PAGE_SIZE,
            }
            data = self._post(url, json_data=payload)
            if not isinstance(data, dict) or "error" in data:
                break

            jobs = data.get("jobs") or data.get("jobPostings") or []
            if not jobs:
                # Some tenants return data in different structure
                jobs = data.get("value") or data.get("results") or []
            if not jobs:
                break

            for j in jobs:
                results.append(self._normalize_api(j, company_code, company_name))

            total = int(data.get("total") or data.get("totalCount") or 0)
            if total:
                self.last_total_available = total
            if not fetch_all or (total and page * PAGE_SIZE >= total) or page >= MAX_PAGES:
                break
            page += 1
            _t.sleep(MIN_DELAY_API)

        return results

    def _normalize_api(self, j: dict, company_code: str, company_name: str) -> dict:
        job_id = j.get("requisitionId") or j.get("id") or j.get("jobId") or j.get("Id") or ""
        title = j.get("jobTitle") or j.get("title") or j.get("Title") or ""
        city = j.get("city") or ""
        state = j.get("state") or j.get("stateCode") or ""
        country = j.get("country") or j.get("countryCode") or ""
        locs = j.get("Locations") or []
        if not city and locs and isinstance(locs[0], dict):
            city = locs[0].get("AddressCity") or ""
            state = state or locs[0].get("AddressState") or ""
            country = country or locs[0].get("AddressCountry") or ""
        location_raw = ", ".join(x for x in [city, state, country] if x)

        is_remote = bool(
            j.get("workFromHome")
            or j.get("isRemote")
            or "remote" in (location_raw + title).lower()
        )
        if is_remote:
            location_type = "REMOTE"
        elif location_raw:
            location_type = "ONSITE"
        else:
            location_type = "UNKNOWN"

        emp_raw = (j.get("employmentType") or j.get("jobType") or "").lower()
        emp_map = {
            "full time": "FULL_TIME",
            "full-time": "FULL_TIME",
            "part time": "PART_TIME",
            "part-time": "PART_TIME",
            "contract": "CONTRACT",
            "temporary": "TEMPORARY",
            "intern": "INTERN",
        }
        employment_type = emp_map.get(emp_raw, "UNKNOWN")

        links = j.get("Links") or {}
        url = (
            links.get("OpportunityDetail")
            or j.get("applyUrl")
            or j.get("url")
            or f"https://recruiting.ultipro.com/{company_code}/JobBoard/job/{job_id}"
        )

        return {
            "external_id": str(job_id),
            "original_url": url,
            "apply_url": url,
            "title": title,
            "company_name": company_name,
            "department": j.get("department") or j.get("businessUnit") or "",
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
            "posted_date_raw": j.get("postedDate") or j.get("datePosted") or "",
            "closing_date": "",
            "raw_payload": j,
        }

    # ── Path 2: HTML scrape ───────────────────────────────────────────────────

    def _scrape_html(self, url: str, company_name: str) -> list[dict]:
        import re
        import time as _t
        self._enforce_rate_limit()
        try:
            resp = self._session.get(
                url, timeout=15,
                headers={"User-Agent": "GoCareers-Bot/1.0 (+https://gocareers.io/bot)"},
            )
            self._last_request_at = _t.monotonic()
            if not resp.ok:
                return []
            html = resp.text
        except Exception:
            return []

        results: list[dict] = []
        seen: set[str] = set()

        # UltiPro/UKG job links pattern (OpportunityDetail pages)
        for m in re.finditer(
            r'href=["\']([^"\']*(?:recruiting\.ultipro\.com)?/[^"\']+/JobBoard/[^"\']+/OpportunityDetail\?opportunityId=[^"\']+)["\']',
            html, re.I,
        ):
            job_url = m.group(1)
            if job_url.startswith("/"):
                job_url = f"https://recruiting.ultipro.com{job_url}"
            if job_url in seen:
                continue
            seen.add(job_url)
            # Try to get title from nearby text
            start = max(0, m.start() - 500)
            ctx = html[start:m.end() + 200]
            title_m = re.search(r'<[^>]*class=["\'][^"\']*job[Tt]itle[^"\']*["\'][^>]*>([\s\S]*?)</[^>]+>', ctx, re.I)
            title = re.sub(r"<[^>]+>", " ", title_m.group(1)).strip() if title_m else "Untitled Position"
            results.append({
                "external_id": "",
                "original_url": job_url,
                "apply_url": job_url,
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
                "raw_payload": {"source": "html_scrape"},
            })
        return results
