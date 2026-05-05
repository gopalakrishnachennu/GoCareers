"""
Breezy HR — public career portal HTML harvester.

List-page parsing follows the same structure as OpenPostings
`parseBreezyPostingsFromHtml` in `server/index.js` (link + title + location blocks).
Reference: https://github.com/Masterjx9/OpenPostings

tenant_id: subdomain only, e.g. "acme" for https://acme.breezy.hr/
"""
from __future__ import annotations

import html as html_lib
import logging
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

import requests

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

MAX_JOBS = 100
DETAIL_FETCH_CAP = 30


def _clean_text(value: str) -> str:
    s = re.sub(r"<[^>]+>", " ", html_lib.unescape(str(value or "")))
    s = re.sub(r"\s+", " ", s).replace(" ,", ",").strip()
    return s


class BreezyHarvester(BaseHarvester):
    """Harvest job links from a Breezy HR subdomain portal."""

    platform_slug = "breezy"
    is_scraper = True

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        self.last_total_available = 0
        sub = (tenant_id or "").strip().lower()
        if not sub or "|" in sub:
            return []

        portal_url = f"https://{sub}.breezy.hr/"
        origin = f"https://{sub}.breezy.hr"

        raw = self._fetch_html(portal_url)
        if not raw:
            return []

        results = self._parse_html(raw, origin, company.name)
        self.last_total_available = len(results)
        if not fetch_all and len(results) > MAX_JOBS:
            results = results[:MAX_JOBS]
        for i, posting in enumerate(results):
            if i >= DETAIL_FETCH_CAP:
                break
            if posting.get("description"):
                continue
            url = posting.get("original_url", "")
            if url:
                desc = self._fetch_detail_description(url)
                if desc:
                    posting["description"] = desc
        return results

    def _fetch_html(self, url: str) -> str:
        if not _check_robots_allowed(url):
            logger.warning("[HARVEST] Breezy: robots.txt blocked %s", url)
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
                host = (urlparse(resp.url).hostname or "").lower()
                if host in ("breezy.hr", "www.breezy.hr"):
                    return ""
                return resp.text
            except requests.exceptions.Timeout:
                time.sleep(BACKOFF_FACTOR ** attempt)
            except Exception as exc:
                logger.warning("[HARVEST] Breezy fetch error %s: %s", url, exc)
                return ""
        return ""

    def _fetch_detail_description(self, url: str) -> str:
        import json as _json
        html = self._fetch_html(url)
        if not html:
            return ""
        for block in re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html, re.S | re.I,
        ):
            try:
                schema = _json.loads(block)
                if isinstance(schema, list):
                    schema = schema[0]
                if isinstance(schema, dict) and schema.get("@type") == "JobPosting":
                    desc = schema.get("description") or ""
                    if desc and len(str(desc)) > 80:
                        return str(desc).strip()
            except Exception:
                continue
        for pat in [
            r'<div[^>]+class=["\'][^"\']*\bdescription\b[^"\']*["\'][^>]*>([\s\S]{100,}?)</div>',
            r'<div[^>]+id=["\']description["\'][^>]*>([\s\S]{100,}?)</div>',
            r'<section[^>]+class=["\'][^"\']*\bcontent\b[^"\']*["\'][^>]*>([\s\S]{100,}?)</section>',
        ]:
            m = re.search(pat, html, re.I)
            if m:
                text = re.sub(r"<[^>]+>", " ", m.group(1))
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) > 100:
                    return text
        return ""

    def _parse_html(self, page_html: str, origin: str, company_name: str) -> list[dict[str, Any]]:
        source = page_html or ""
        postings: list[dict[str, Any]] = []
        seen: set[str] = set()

        link_re = re.compile(
            r'<a[^>]*href=["\']((?:https?://[^"\'<>]+)?/p/[^"\'<>]+)["\'][^>]*>([\s\S]*?)</a>',
            re.I,
        )
        title_re = re.compile(r"<h2[^>]*>([\s\S]*?)</h2>", re.I)
        loc_re = re.compile(
            r'<li[^>]*class=["\'][^"\']*\blocation\b[^"\']*["\'][^>]*>[\s\S]*?<span>([\s\S]*?)</span>',
            re.I,
        )
        posted_re = re.compile(
            r'<li[^>]*class=["\'][^"\']*(?:posted|created|date)[^"\']*["\'][^>]*>[\s\S]*?<span>([\s\S]*?)</span>',
            re.I,
        )
        dept_re = re.compile(
            r'<h2[^>]*class=["\'][^"\']*\bgroup-header\b[^"\']*["\'][^>]*>[\s\S]*?<span>([\s\S]*?)</span>',
            re.I,
        )

        for m in link_re.finditer(source):
            href = (m.group(1) or "").strip()
            abs_url = urljoin(f"{origin}/", href)
            if not abs_url or abs_url in seen:
                continue

            link_body = m.group(2) or ""
            tm = title_re.search(link_body)
            title = _clean_text(tm.group(1)) if tm else ""
            if not title:
                continue

            lm = loc_re.search(link_body)
            pm = posted_re.search(link_body)
            ctx_start = max(0, m.start() - 3000)
            context = source[ctx_start : m.start()]
            dept_matches = list(dept_re.finditer(context))
            department = ""
            if dept_matches:
                department = _clean_text(dept_matches[-1].group(1))

            seen.add(abs_url)
            postings.append({
                "external_id": "",
                "original_url": abs_url,
                "apply_url": abs_url,
                "title": title,
                "company_name": company_name,
                "department": department,
                "team": "",
                "location_raw": _clean_text(lm.group(1)) if lm else "",
                "city": "",
                "state": "",
                "country": "",
                "is_remote": "remote" in (title + (lm.group(1) if lm else "")).lower(),
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
                "responsibilities": "",
                "benefits": "",
                "posted_date_raw": _clean_text(pm.group(1)) if pm else "",
                "closing_date": "",
                "raw_payload": {"source": "breezy_html"},
            })
            if len(postings) >= MAX_JOBS:
                break

        return postings
