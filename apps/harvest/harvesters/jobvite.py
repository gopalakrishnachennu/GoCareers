"""
JobviteHarvester — Python port of OpenPostings Jobvite scraper.

Jobvite job boards live at: jobs.jobvite.com/{company}/jobs
Jobs are grouped by department in <table class="jv-job-list"> blocks,
each preceded by an <h3> department heading.
Each row has:
  <td class="jv-job-list-name">  → title + job link
  <td class="jv-job-list-location"> → location

Single-page — no pagination needed.

Ported 1-to-1 from OpenPostings (MIT) JavaScript implementation.
"""
import re
import time
from typing import Any
from urllib.parse import urljoin

from .base import BaseHarvester, DEFAULT_TIMEOUT, BOT_USER_AGENT

DETAIL_FETCH_CAP = 30


def _detect_location_type(location_raw: str) -> tuple[str, bool]:
    loc_lower = (location_raw or "").lower()
    if "remote" in loc_lower:
        return "REMOTE", True
    if "hybrid" in loc_lower:
        return "HYBRID", False
    if location_raw and location_raw.strip():
        return "ONSITE", False
    return "UNKNOWN", False


def _split_location(location_raw: str) -> tuple[str, str, str]:
    """Return (city, state, country) best-effort from a raw location string."""
    parts = [p.strip() for p in (location_raw or "").split(",")]
    city = parts[0] if parts else ""
    state = parts[1] if len(parts) > 1 else ""
    country = parts[2] if len(parts) > 2 else ""
    return city, state, country


class JobviteHarvester(BaseHarvester):
    platform_slug = "jobvite"
    is_scraper = True

    BASE_ORIGIN = "https://jobs.jobvite.com"

    def fetch_jobs(self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False) -> list[dict[str, Any]]:
        """
        tenant_id = company slug e.g. "loandepot", "varonis", "leovegas"
        Also handles /careers/{slug} variant automatically.
        """
        if not tenant_id:
            return []

        # Strip "careers/" prefix if stored that way
        slug = tenant_id.lstrip("/")
        if slug.startswith("careers/"):
            slug = slug[len("careers/"):]

        jobs_url = f"{self.BASE_ORIGIN}/{slug}/jobs"
        html = self._fetch_html(jobs_url)
        if not html:
            # Try alternate careers path
            html = self._fetch_html(f"{self.BASE_ORIGIN}/careers/{slug}/jobs")
        if not html:
            return []

        postings = self._parse_postings(company.name, slug, html)
        for i, posting in enumerate(postings):
            if i >= DETAIL_FETCH_CAP:
                break
            if posting.get("description"):
                continue
            url = posting.get("original_url", "")
            if url:
                desc = self._fetch_detail_description(url)
                if desc:
                    posting["description"] = desc
        return postings

    # ── HTML helpers ──────────────────────────────────────────────────────────

    def _fetch_html(self, url: str) -> str:
        self._enforce_rate_limit()
        try:
            resp = self._session.get(
                url, timeout=DEFAULT_TIMEOUT,
                headers={"Accept": "text/html,application/xhtml+xml", "User-Agent": BOT_USER_AGENT},
            )
            self._last_request_at = time.monotonic()
            if resp.ok:
                return resp.text
        except Exception:
            pass
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
        m = re.search(
            r'<div[^>]+class=["\'][^"\']*jv-job-detail-description[^"\']*["\'][^>]*>([\s\S]{100,}?)</div>',
            html, re.I,
        )
        if m:
            text = re.sub(r"<[^>]+>", " ", m.group(1))
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) > 100:
                return text
        return ""

    # ── Parsing ───────────────────────────────────────────────────────────────

    def _extract_id_from_url(self, url: str) -> str:
        """Extract job ID from Jobvite URL like /jobs/oLui0fwk or ?jvi=oLui0fwk."""
        m = re.search(r'[?&]jvi=([^&]+)', url)
        if m:
            return m.group(1)
        m = re.search(r'/jobs/([^/?#]+)', url)
        return m.group(1) if m else ""

    def _parse_postings(self, company_name: str, slug: str, html: str) -> list[dict]:
        postings: list[dict] = []
        seen: set[str] = set()

        # Department-grouped tables: <h3>Department</h3> <table class="jv-job-list">...</table>
        table_pat = re.compile(
            r"<h3[^>]*>([\s\S]*?)</h3>\s*"
            r'<table[^>]*class=["\'][^"\']*\bjv-job-list\b[^"\']*["\'][^>]*>([\s\S]*?)</table>',
            re.I,
        )
        row_pat = re.compile(
            r"<tr[^>]*>[\s\S]*?"
            r'<td[^>]*class=["\'][^"\']*\bjv-job-list-name\b[^"\']*["\'][^>]*>[\s\S]*?'
            r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>([\s\S]*?)</a>[\s\S]*?</td>[\s\S]*?'
            r'<td[^>]*class=["\'][^"\']*\bjv-job-list-location\b[^"\']*["\'][^>]*>([\s\S]*?)</td>'
            r'[\s\S]*?</tr>',
            re.I,
        )

        def push_rows(rows_html: str, department: str = "") -> None:
            for m in row_pat.finditer(rows_html):
                href = m.group(1).strip()
                abs_url = urljoin(self.BASE_ORIGIN + "/", href) if href else ""
                if not abs_url or abs_url in seen:
                    continue
                location_raw = self._clean(m.group(3)) or ""
                location_type, is_remote = _detect_location_type(location_raw)
                city, state, country = _split_location(location_raw)
                external_id = self._extract_id_from_url(abs_url)
                postings.append({
                    "external_id": external_id,
                    "original_url": abs_url,
                    "apply_url": abs_url,
                    "title": self._clean(m.group(2)) or "Untitled Position",
                    "company_name": company_name,
                    "department": self._clean(department) or "",
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
                    "posted_date_raw": "",
                    "closing_date": "",
                    "raw_payload": {},
                })
                seen.add(abs_url)

        matched = False
        for table_m in table_pat.finditer(html):
            push_rows(table_m.group(2), table_m.group(1))
            matched = True

        # Fallback: try without department grouping (some Jobvite pages differ)
        if not matched:
            push_rows(html)

        self.last_total_available = len(postings)
        return postings

    def _clean(self, value: str) -> str:
        text = re.sub(r"<[^>]+>", " ", value or "")
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"\s*,\s*", ", ", text)
        return text.strip()
