"""
DayforceHarvester — Dayforce HCM (Ceridian) Career Portal

Dayforce portals are Next.js SPAs hosted at:
  https://jobs.dayforcehcm.com/en-US/{company}/CANDIDATEPORTAL

The job-search API powers the SPA:
  POST https://jobs.dayforcehcm.com/api/geo/{company}/jobposting/search
  Body: {"cultureCode": "en-US", "pageNum": 1, "pageSize": 50}

The API requires a Cloudflare-managed session cookie (__cf_bm / _cfuvid).
We obtain the cookie by visiting the portal page first, then POST to the API.

tenant_id = company code  e.g. "corpay", "atricure", "hightower"
"""
import re
import time
from typing import Any

from .base import BaseHarvester, MIN_DELAY_API, DEFAULT_TIMEOUT, BOT_USER_AGENT

PAGE_SIZE = 50
MAX_PAGES = 40

# Browser-like headers required to pass Cloudflare bot check
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}


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

        # Warm the session: Cloudflare needs a page visit before allowing API calls
        self._warm_session(slug, board_code)

        # Path 1: GEO JSON API (Next.js SPA backend)
        api_results = self._fetch_api(slug, company.name, fetch_all)
        if api_results:
            return api_results

        # Path 2: HTML scrape (last resort)
        return self._scrape_html(slug, board_code, company.name)

    def _warm_session(self, slug: str, board_code: str) -> None:
        """Visit the portal page to obtain CF cookies before calling the API."""
        portal_url = f"https://jobs.dayforcehcm.com/en-US/{slug}/{board_code}"
        self._enforce_rate_limit()
        try:
            self._session.get(
                portal_url,
                timeout=DEFAULT_TIMEOUT,
                headers={
                    "User-Agent": BROWSER_HEADERS["User-Agent"],
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
            self._last_request_at = time.monotonic()
        except Exception:
            pass

    # ── Path 1: GEO JSON API ─────────────────────────────────────────────────

    def _fetch_api(self, slug: str, company_name: str, fetch_all: bool) -> list[dict]:
        search_url = f"https://jobs.dayforcehcm.com/api/geo/{slug}/jobposting/search"

        results: list[dict] = []
        page = 1
        while True:
            payload = {
                "cultureCode": "en-US",
                "pageNum": page,
                "pageSize": PAGE_SIZE,
            }
            headers = dict(BROWSER_HEADERS)
            headers["Content-Type"] = "application/json"
            headers["Origin"] = "https://jobs.dayforcehcm.com"
            headers["Referer"] = f"https://jobs.dayforcehcm.com/en-US/{slug}/CANDIDATEPORTAL"

            self._enforce_rate_limit()
            try:
                resp = self._session.post(
                    search_url,
                    json=payload,
                    headers=headers,
                    timeout=DEFAULT_TIMEOUT,
                )
                self._last_request_at = time.monotonic()
            except Exception:
                break

            if not resp.ok:
                break

            try:
                data = resp.json()
            except Exception:
                break

            jobs = (
                data.get("data")
                or data.get("items")
                or data.get("JobPostings")
                or data.get("jobs")
                or []
            )
            if not isinstance(jobs, list) or not jobs:
                break

            for j in jobs:
                results.append(self._normalize(j, slug, company_name))

            total = int(
                data.get("totalCount")
                or data.get("TotalCount")
                or data.get("total")
                or 0
            )
            if total:
                self.last_total_available = total
            if not fetch_all or (total and page * PAGE_SIZE >= total) or page >= MAX_PAGES:
                break
            page += 1
            time.sleep(MIN_DELAY_API)

        return results

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
