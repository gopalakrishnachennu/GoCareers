"""
Teamtailor — public /jobs HTML list harvester.

Parses `<li class="...block-grid-item...">` cards like OpenPostings
`parseTeamtailorPostingsFromHtml` in `server/index.js`.

Reference: https://github.com/Masterjx9/OpenPostings

tenant_id: subdomain only, e.g. "acme" for https://acme.teamtailor.com/jobs
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
    return re.sub(r"\s+", " ", s).strip()


class TeamtailorHarvester(BaseHarvester):
    platform_slug = "teamtailor"
    is_scraper = True

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        self.last_total_available = 0
        sub = (tenant_id or "").strip().lower().split("|")[0].strip()
        if not sub:
            return []

        jobs_url = f"https://{sub}.teamtailor.com/jobs"
        base_origin = f"https://{sub}.teamtailor.com"

        raw = self._fetch_html(jobs_url)
        if not raw:
            return []

        results = self._parse_html(raw, base_origin, company.name)
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
            logger.warning("[HARVEST] Teamtailor: robots.txt blocked %s", url)
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
                if not host.endswith(".teamtailor.com"):
                    return ""
                return resp.text
            except requests.exceptions.Timeout:
                time.sleep(BACKOFF_FACTOR ** attempt)
            except Exception as exc:
                logger.warning("[HARVEST] Teamtailor fetch error %s: %s", url, exc)
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
            r'<div[^>]+data-ui=["\']job-description["\'][^>]*>([\s\S]{100,}?)</div>',
            r'<div[^>]+class=["\'][^"\']*\bjob__description\b[^"\']*["\'][^>]*>([\s\S]{100,}?)</div>',
            r'<div[^>]+class=["\'][^"\']*\bdescription\b[^"\']*["\'][^>]*>([\s\S]{100,}?)</div>',
        ]:
            m = re.search(pat, html, re.I)
            if m:
                text = re.sub(r"<[^>]+>", " ", m.group(1))
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) > 100:
                    return text
        return ""

    def _extract_meta_parts(self, meta_raw: str) -> list[str]:
        parts: list[str] = []
        seen: set[str] = set()
        for m in re.finditer(r"<span[^>]*>([\s\S]*?)</span>", meta_raw, re.I):
            cleaned = _clean_text(m.group(1) or "")
            norm = cleaned.lower()
            if not cleaned or cleaned in ("·", "&middot;") or norm in seen:
                continue
            parts.append(cleaned)
            seen.add(norm)
        return parts

    def _parse_html(self, page_html: str, base_origin: str, company_name: str) -> list[dict[str, Any]]:
        source = page_html or ""
        postings: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        item_re = re.compile(
            r'<li[^>]*class=["\'][^"\']*\bblock-grid-item\b[^"\']*["\'][^>]*>([\s\S]*?)</li>',
            re.I,
        )
        href_re = re.compile(r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>', re.I)
        title_attr_re = re.compile(
            r'<span[^>]*class=["\'][^"\']*\btext-block-base-link\b[^"\']*["\'][^>]*\btitle=["\']([^"\']+)["\']',
            re.I,
        )
        title_body_re = re.compile(
            r'<span[^>]*class=["\'][^"\']*\btext-block-base-link\b[^"\']*["\'][^>]*>([\s\S]*?)</span>',
            re.I,
        )
        meta_re = re.compile(
            r'<div[^>]*class=["\'][^"\']*\bmt-1\b[^"\']*\btext-md\b[^"\']*["\'][^>]*>([\s\S]*?)</div>',
            re.I,
        )

        for m in item_re.finditer(source):
            item_html = m.group(1) or ""
            hm = href_re.search(item_html)
            href = (hm.group(1) or "").strip() if hm else ""
            job_url = urljoin(f"{base_origin}/", href) if href else ""
            if not job_url or job_url in seen_urls:
                continue

            title_from_attr = _clean_text(title_attr_re.search(item_html).group(1) if title_attr_re.search(item_html) else "")
            title_from_body = _clean_text(title_body_re.search(item_html).group(1) if title_body_re.search(item_html) else "")
            title = title_from_attr or title_from_body or "Untitled Position"

            meta_raw = meta_re.search(item_html)
            meta_raw = meta_raw.group(1) if meta_raw else ""
            meta_parts = self._extract_meta_parts(meta_raw)
            department = meta_parts[0] if len(meta_parts) > 1 else ""
            location_raw = " / ".join(meta_parts[1:]) if len(meta_parts) > 1 else (meta_parts[0] if meta_parts else "")

            seen_urls.add(job_url)
            postings.append({
                "external_id": "",
                "original_url": job_url,
                "apply_url": job_url,
                "title": title,
                "company_name": company_name,
                "department": department,
                "team": "",
                "location_raw": location_raw,
                "city": "",
                "state": "",
                "country": "",
                "is_remote": "remote" in (title + location_raw).lower(),
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
                "raw_payload": {"source": "teamtailor_html"},
            })
            if len(postings) >= MAX_JOBS:
                break

        return postings
