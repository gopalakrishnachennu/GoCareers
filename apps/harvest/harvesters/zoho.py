"""
Zoho Recruit — hidden-field JSON harvester.

Parses `<input id="jobs" value="[...]">` JSON and optional `<input id="meta">`
for `list_url`, matching OpenPostings `parseZohoPostingsFromHtml` /
`extractZohoHiddenInputValue` in `server/index.js`.

Reference: https://github.com/Masterjx9/OpenPostings

tenant_id:
  - Portal slug: `mycompany` → https://jobs.zoho.com/portal/mycompany/careers
  - Or subdomain: `mycompany` also tries https://mycompany.zohorecruit.com/jobs/Careers
"""
from __future__ import annotations

import html as html_lib
import json
import logging
import re
import time
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from .base import (
    BOT_USER_AGENT,
    DEFAULT_TIMEOUT,
    MAX_RETRIES,
    BACKOFF_FACTOR,
    BaseHarvester,
    MIN_DELAY_SCRAPE,
    _check_robots_allowed,
)

logger = logging.getLogger(__name__)

MAX_JOBS = 150


def _clean_text(value: str) -> str:
    s = re.sub(r"<[^>]+>", " ", html_lib.unescape(str(value or "")))
    return re.sub(r"\s+", " ", s).strip()


class ZohoHarvester(BaseHarvester):
    platform_slug = "zoho"
    is_scraper = True

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        self.last_total_available = 0
        t = (tenant_id or "").strip()
        if not t:
            return []

        for page_url in self._candidate_urls(t):
            raw = self._fetch_html(page_url)
            if not raw:
                continue
            results = self._parse_page(raw, page_url, company.name)
            if results:
                self.last_total_available = len(results)
                if not fetch_all and len(results) > MAX_JOBS:
                    return results[:MAX_JOBS]
                return results
        return []

    def _candidate_urls(self, tenant: str) -> list[str]:
        """Try portal and zohorecruit career pages (OpenPostings uses zohorecruit)."""
        raw = (tenant or "").strip()
        slug = raw.split("|")[0].strip()
        slug = re.sub(r"^https?://", "", slug)

        if "zohorecruit.com" in slug.lower():
            base_url = slug if slug.startswith("http") else f"https://{slug}"
            p = urlparse(base_url)
            if p.netloc:
                return [f"{p.scheme or 'https'}://{p.netloc}/jobs/Careers"]
            return []

        return [
            f"https://jobs.zoho.com/portal/{slug}/careers",
            f"https://{slug}.zohorecruit.com/jobs/Careers",
        ]

    def _fetch_html(self, url: str) -> str:
        if not _check_robots_allowed(url):
            logger.warning("[HARVEST] Zoho: robots.txt blocked %s", url)
            return ""

        elapsed = time.monotonic() - self._last_request_at
        if elapsed < MIN_DELAY_SCRAPE:
            time.sleep(MIN_DELAY_SCRAPE - elapsed)

        headers = {
            "User-Agent": BOT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self._session.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
                self._last_request_at = time.monotonic()
                if resp.status_code == 429:
                    time.sleep(min(int(resp.headers.get("Retry-After", BACKOFF_FACTOR ** attempt)), 120))
                    continue
                if resp.status_code >= 500:
                    time.sleep(BACKOFF_FACTOR ** attempt)
                    continue
                if resp.status_code >= 400:
                    return ""
                return resp.text
            except requests.exceptions.Timeout:
                time.sleep(BACKOFF_FACTOR ** attempt)
            except Exception as exc:
                logger.warning("[HARVEST] Zoho fetch error %s: %s", url, exc)
                return ""
        return ""

    def _extract_input_value(self, page_html: str, input_id: str) -> str:
        soup = BeautifulSoup(page_html or "", "html.parser")
        inp = soup.find("input", id=input_id)
        if inp and inp.get("value") is not None:
            return str(inp.get("value", "")).strip()
        return ""

    def _extract_list_url(self, page_html: str, fallback_url: str) -> str:
        meta_raw = self._extract_input_value(page_html, "meta")
        if meta_raw:
            try:
                meta_data = json.loads(html_lib.unescape(meta_raw))
                list_url = str(meta_data.get("list_url") or "").strip()
                if list_url:
                    return list_url
            except (json.JSONDecodeError, TypeError):
                pass

        og = re.search(
            r'<meta[^>]*property=["\']og:url["\'][^>]*content=["\']([^"\']+)["\']',
            page_html or "",
            re.I,
        )
        if og:
            return html_lib.unescape(og.group(1).strip())

        p = urlparse(fallback_url)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}/jobs/Careers"
        return fallback_url.strip()

    def _build_job_url(self, list_url: str, job_id: str) -> str:
        job_id = str(job_id or "").strip()
        p = urlparse(list_url)
        if not p.scheme or not p.netloc:
            return list_url.strip()

        path = (p.path or "").rstrip("/") or "/jobs/Careers"
        if "/jobs/careers" not in path.lower():
            path = "/jobs/Careers"
        return urlunparse((p.scheme, p.netloc, f"{path}/{job_id}", "", "", ""))

    def _parse_page(self, page_html: str, page_url: str, company_name: str) -> list[dict[str, Any]]:
        raw_jobs = self._extract_input_value(page_html, "jobs")
        if not raw_jobs:
            return []

        try:
            payload = html_lib.unescape(raw_jobs)
            jobs = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return []

        if not isinstance(jobs, list):
            return []

        list_url = self._extract_list_url(page_html, page_url)
        results: list[dict[str, Any]] = []
        seen: set[str] = set()

        for job in jobs:
            if not isinstance(job, dict):
                continue
            if job.get("Publish") is False:
                continue

            job_id = str(job.get("id") or "").strip()
            if not job_id or job_id in seen:
                continue

            title = (
                _clean_text(str(job.get("Posting_Title") or ""))
                or _clean_text(str(job.get("Job_Opening_Name") or ""))
                or "Untitled Position"
            )
            city = _clean_text(str(job.get("City") or ""))
            state = _clean_text(str(job.get("State") or ""))
            country = _clean_text(str(job.get("Country") or ""))
            loc_parts = [x for x in [city, state, country] if x]
            location_raw = ", ".join(loc_parts)

            posted = _clean_text(str(job.get("Date_Opened") or ""))
            department = _clean_text(str(job.get("Industry") or ""))

            original_url = self._build_job_url(list_url, job_id)
            seen.add(job_id)

            results.append({
                "external_id": job_id,
                "original_url": original_url,
                "apply_url": original_url,
                "title": title,
                "company_name": company_name,
                "department": department,
                "team": "",
                "location_raw": location_raw,
                "city": city,
                "state": state,
                "country": country,
                "is_remote": "remote" in (title + location_raw).lower(),
                "location_type": "REMOTE" if "remote" in location_raw.lower() else (
                    "ONSITE" if location_raw else "UNKNOWN"
                ),
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
                "posted_date_raw": posted,
                "closing_date": "",
                "raw_payload": job,
            })

        return results
