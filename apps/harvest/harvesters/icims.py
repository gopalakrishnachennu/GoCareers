"""
iCIMSHarvester — Python port of OpenPostings iCIMS scraper.

iCIMS job boards use an iframe pattern:
  1. Fetch {tenant}.icims.com/jobs/search?ss=1
  2. Extract iframe src from the wrapper page
  3. Parse <li class="iCIMS_JobCardItem"> elements inside the iframe
  4. Follow <link rel="next"> for pagination (up to 25 pages)

Ported 1-to-1 from OpenPostings (MIT) JavaScript implementation.
"""
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse, urlencode, parse_qs

from .base import BaseHarvester, MIN_DELAY_SCRAPE, DEFAULT_TIMEOUT, BOT_USER_AGENT

MAX_PAGES = 25
# Fetch full description for the first N jobs per company during harvest.
# Remaining jobs get their JDs filled by the background backfill engine.
DETAIL_FETCH_CAP = 50


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


class IcimsHarvester(BaseHarvester):
    platform_slug = "icims"
    is_scraper = True

    # ── Public interface ──────────────────────────────────────────────────────

    def fetch_jobs(self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False) -> list[dict[str, Any]]:
        """
        tenant_id = full subdomain e.g. "careers-audacy", "careers-samaritanvillage"
        Also accepts "uscareeropenings-alliancelaundry" style slugs.
        """
        if not tenant_id:
            return []

        base_origin = f"https://{tenant_id}.icims.com"
        search_url = f"{base_origin}/jobs/search?ss=1"

        wrapper_html = self._fetch_html(search_url)
        if not wrapper_html:
            return []

        page_url = self._extract_iframe_url(wrapper_html, search_url)
        page_url = self._ensure_iframe_url(page_url)

        collected: list[dict] = []
        seen_urls: set[str] = set()
        seen_pages: set[str] = set()

        for _ in range(MAX_PAGES):
            if not page_url or page_url in seen_pages:
                break
            seen_pages.add(page_url)

            page_html = self._fetch_html(page_url)
            if not page_html:
                break

            for posting in self._parse_postings(company.name, base_origin, page_html):
                url = posting.get("original_url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    collected.append(posting)

        # Inline detail fetch for the first DETAIL_FETCH_CAP jobs.
        # Remaining jobs will be filled by the background backfill engine.
        for i, posting in enumerate(collected):
            if i >= DETAIL_FETCH_CAP:
                break
            url = posting.get("original_url", "")
            if url and not posting.get("description"):
                detail = self._fetch_detail_data(url)
                if detail.get("description"):
                    posting["description"] = detail["description"]
                if detail.get("department") and not posting.get("department"):
                    posting["department"] = detail["department"]
                if detail.get("salary_min") and not posting.get("salary_min"):
                    posting["salary_min"] = detail["salary_min"]
                    posting["salary_max"] = detail.get("salary_max")
                    posting["salary_period"] = detail.get("salary_period", "YEAR")
                    posting["salary_raw"] = detail.get("salary_raw", "")

            next_url = self._extract_next_page(page_html, page_url)
            if not next_url:
                break
            page_url = next_url
            time.sleep(MIN_DELAY_SCRAPE)

        self.last_total_available = len(collected)
        return collected

    # ── Detail JD fetch ───────────────────────────────────────────────────────

    def _fetch_detail_data(self, url: str) -> dict:
        """Fetch the job detail page and extract description + department + salary."""
        import json as _json

        detail_url = re.sub(r"\?.*$", "", url)
        if not detail_url.endswith("/job"):
            detail_url = re.sub(r"/+(search|intro).*$", "", detail_url)
            if re.search(r"/jobs/\d+$", detail_url):
                detail_url = detail_url + "/job"

        html = self._fetch_html(detail_url)
        if not html:
            return {}

        result: dict = {}

        # JSON-LD JobPosting schema (most reliable)
        for block in re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.S | re.I
        ):
            try:
                schema = _json.loads(block)
                if isinstance(schema, list):
                    schema = schema[0]
                if isinstance(schema, dict) and schema.get("@type") == "JobPosting":
                    desc = schema.get("description") or ""
                    if desc and len(str(desc)) > 80:
                        result["description"] = str(desc).strip()
                    # Department from JSON-LD occupationalCategory or hiringOrganization
                    dept = (
                        schema.get("occupationalCategory")
                        or schema.get("jobCategory")
                        or ""
                    )
                    if dept:
                        result["department"] = str(dept).strip()
                    # Salary
                    sal = schema.get("baseSalary")
                    if isinstance(sal, dict):
                        val = sal.get("value", {})
                        if isinstance(val, dict):
                            result["salary_min"] = val.get("minValue")
                            result["salary_max"] = val.get("maxValue")
                            result["salary_period"] = val.get("unitText", "YEAR").upper()
                            if result.get("salary_min"):
                                result["salary_raw"] = (
                                    f"{result['salary_min']:,.0f}–{result['salary_max']:,.0f}"
                                    if result.get("salary_max") else f"{result['salary_min']:,.0f}"
                                )
                    if "description" in result:
                        break
            except Exception:
                continue

        # iCIMS-specific div containers for description fallback
        if "description" not in result:
            for pat in [
                r'<div[^>]+class=["\'][^"\']*iCIMS_JobDescription[^"\']*["\'][^>]*>([\s\S]{100,}?)</div>',
                r'<div[^>]+id=["\']requisitionDescriptionInterface[^"\']*["\'][^>]*>([\s\S]{100,}?)</div>',
            ]:
                m = re.search(pat, html, re.I)
                if m:
                    text = re.sub(r"<[^>]+>", " ", m.group(1))
                    text = re.sub(r"\s+", " ", text).strip()
                    if len(text) > 100:
                        result["description"] = text
                        break

        # Department from iCIMS header fields (JobHeaderData)
        if "department" not in result:
            for dept_pat in [
                r'field-label">\s*(?:Job\s+)?(?:Category|Department|Function|Group)\s*</span>\s*</dt>\s*<dd[^>]*>\s*<span[^>]*>([\s\S]*?)</span>',
                r'iCIMS_InfoMsg_Job[^>]*>\s*(?:Category|Department|Function):\s*([\w\s,&/-]+)',
            ]:
                dm = re.search(dept_pat, html, re.I)
                if dm:
                    dept_val = re.sub(r"<[^>]+>", "", dm.group(1)).strip()
                    if dept_val:
                        result["department"] = dept_val
                        break

        return result

    def _fetch_detail_description(self, url: str) -> str:
        """Legacy wrapper — returns description string only."""
        return self._fetch_detail_data(url).get("description", "")

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

    def _ensure_iframe_url(self, url: str) -> str:
        if not url:
            return url
        parsed = urlparse(url)
        params = {k: v[0] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}
        params["in_iframe"] = "1"
        return parsed._replace(query=urlencode(params)).geturl()

    def _extract_iframe_url(self, html: str, base_url: str) -> str:
        for pattern in [
            r"icimsFrame\.src\s*=\s*'([^']+)'",
            r'icimsFrame\.src\s*=\s*"([^"]+)"',
            r'<iframe[^>]*id=["\']icims_content_iframe["\'][^>]*src=["\']([^"\']+)["\']',
        ]:
            m = re.search(pattern, html, re.I)
            if m:
                candidate = m.group(1).strip()
                if candidate.startswith("//"):
                    candidate = "https:" + candidate
                elif not re.match(r"https?://", candidate, re.I):
                    candidate = urljoin(base_url, candidate)
                return self._ensure_iframe_url(candidate)
        return self._ensure_iframe_url(base_url)

    # ── Parsing ───────────────────────────────────────────────────────────────

    def _extract_job_id(self, url: str) -> str:
        """Extract numeric job ID from iCIMS URL like /jobs/12345/..."""
        m = re.search(r'/jobs/(\d+)', url)
        return m.group(1) if m else ""

    def _parse_postings(self, company_name: str, origin: str, html: str) -> list[dict]:
        postings: list[dict] = []
        seen: set[str] = set()

        def make_posting(abs_url: str, title: str, location_raw: str, posted_date_raw: str) -> dict:
            location_type, is_remote = _detect_location_type(location_raw)
            city, state, country = _split_location(location_raw)
            external_id = self._extract_job_id(abs_url)
            return {
                "external_id": external_id,
                "original_url": abs_url,
                "apply_url": abs_url,
                "title": title or "Untitled",
                "company_name": company_name,
                "department": "",
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
                "responsibilities": "",
                "benefits": "",
                "posted_date_raw": posted_date_raw,
                "closing_date": "",
                "raw_payload": {},
            }

        card_pat = re.compile(
            r'<li[^>]*class=["\'][^"\']*iCIMS_JobCardItem[^"\']*["\'][^>]*>([\s\S]*?)</li>', re.I
        )
        for card_m in card_pat.finditer(html):
            card_html = card_m.group(1)
            link_m = re.search(r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>([\s\S]*?)</a>', card_html, re.I)
            if not link_m:
                continue
            href = link_m.group(1).strip()
            if not re.search(r"/jobs/\d+", href, re.I):
                continue
            abs_url = urljoin(origin + "/", href)
            if abs_url in seen or "/jobs/intro" in abs_url.lower():
                continue

            title_m = re.search(r"<h[1-6][^>]*>([\s\S]*?)</h[1-6]>", link_m.group(2), re.I)
            title = self._clean(title_m.group(1) if title_m else link_m.group(2))
            location_raw = self._extract_location(card_html)
            posted_date_raw = self._extract_date(card_html)
            postings.append(make_posting(abs_url, title, location_raw, posted_date_raw))
            seen.add(abs_url)

        if postings:
            return postings

        # Fallback: any /jobs/\d+ links in the page
        for m in re.finditer(
            r'<a[^>]*href=["\']([^"\']*\/jobs\/\d+[^"\']*)["\'][^>]*>([\s\S]*?)</a>', html, re.I
        ):
            href = m.group(1).strip()
            abs_url = urljoin(origin + "/", href)
            if abs_url in seen or "/jobs/intro" in abs_url.lower():
                continue
            title_m = re.search(r"<h[1-6][^>]*>([\s\S]*?)</h[1-6]>", m.group(2), re.I)
            ctx = html[max(0, m.start() - 800): m.end() + 2200]
            title = self._clean(title_m.group(1) if title_m else m.group(2))
            location_raw = self._extract_location(ctx)
            posted_date_raw = self._extract_date(ctx)
            postings.append(make_posting(abs_url, title, location_raw, posted_date_raw))
            seen.add(abs_url)

        return postings

    def _extract_location(self, html: str) -> str:
        for pat in [
            r'field-label">Location\s*</span>\s*</dt>\s*<dd[^>]*class=["\'][^"\']*iCIMS_JobHeaderData[^"\']*["\'][^>]*>\s*<span[^>]*>([\s\S]*?)</span>',
            r'glyphicons-map-marker[^>]*>[\s\S]*?</dt>\s*<dd[^>]*class=["\'][^"\']*iCIMS_JobHeaderData[^"\']*["\'][^>]*>\s*<span[^>]*>([\s\S]*?)</span>',
        ]:
            m = re.search(pat, html, re.I)
            if m:
                loc = self._clean(m.group(1))
                if loc:
                    return loc
        return ""

    def _extract_date(self, html: str) -> str:
        m = re.search(
            r'field-label">Date Posted\s*</span>\s*<span[^>]*?(?:title=["\']([^"\']+)["\'])?[^>]*>\s*([^<]*)',
            html, re.I,
        )
        if m:
            return (m.group(1) or m.group(2) or "").strip()
        return ""

    def _extract_next_page(self, html: str, current_url: str) -> str:
        for pat in [
            r'<link[^>]*rel=["\']next["\'][^>]*href=["\']([^"\']+)["\']',
            r'<link[^>]*href=["\']([^"\']+)["\'][^>]*rel=["\']next["\']',
        ]:
            m = re.search(pat, html, re.I)
            if m:
                candidate = m.group(1).strip()
                if candidate.startswith("//"):
                    candidate = "https:" + candidate
                elif not re.match(r"https?://", candidate, re.I):
                    candidate = urljoin(current_url, candidate)
                candidate = self._ensure_iframe_url(candidate)
                if candidate != current_url:
                    return candidate
        return ""

    def _clean(self, value: str) -> str:
        text = re.sub(r"<[^>]+>", " ", value or "")
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"\s*,\s*", ", ", text)
        return text.strip()
