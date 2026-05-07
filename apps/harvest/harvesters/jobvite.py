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
JSON_FEED_URL = "https://jobs.jobvite.com/{slug}/job?i=Json"


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


def _detail_location_line(html: str, title: str = "") -> str:
    """Extract Jobvite detail header location text before the Description section."""
    try:
        from bs4 import BeautifulSoup
        from harvest.location_resolver import split_multi_location_text
    except Exception:
        return ""

    soup = BeautifulSoup(html or "", "html.parser")
    lines = [line.strip() for line in soup.get_text("\n", strip=True).splitlines() if line.strip()]
    if not lines:
        return ""

    meta = soup.select_one(".jv-job-detail-meta")
    if meta:
        candidates: list[str] = []
        # Jobvite often renders multi-location headers as:
        # Hybrid Remote<span>,</span> Seattle, Washington
        # <span class="jv-inline-separator"></span> Los Angeles, California
        # Splitting on the separator before stripping text preserves city/state
        # pairs that BeautifulSoup otherwise breaks into separate lines.
        for segment in re.split(r"<span[^>]*jv-inline-separator[^>]*></span>", str(meta), flags=re.I):
            text = BeautifulSoup(segment, "html.parser").get_text(" ", strip=True)
            text = re.sub(r"\s*,\s*", ", ", text)
            text = re.sub(r"\s+", " ", text).strip(" ,")
            for candidate in split_multi_location_text(text):
                if candidate not in candidates:
                    candidates.append(candidate)
        if candidates:
            return " | ".join(candidates)[:512]

    start = 0
    if title:
        title_key = title.strip().lower()
        for idx, line in enumerate(lines):
            if line.lower() == title_key:
                start = idx + 1
                break

    collected: list[str] = []
    for line in lines[start:start + 8]:
        low = line.lower()
        if low in {"description", "job description", "job summary"}:
            break
        if "apply" == low:
            continue
        if split_multi_location_text(line):
            collected.append(line)

    if not collected:
        return ""

    # Drop a leading department/category segment when a bullet-delimited meta
    # line looks like "Professional Staff • Seattle, Washington • Los Angeles..."
    text = " • ".join(collected)
    parts = [p.strip(" ,") for p in re.split(r"\s*(?:\u2022|·|\|)\s*", text) if p.strip(" ,")]
    if len(parts) > 1 and not split_multi_location_text(parts[0]):
        parts = parts[1:]
    return " | ".join(parts)[:512]


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

        # ── Try JSON feed first (faster, richer data) ─────────────────────────
        postings = self._fetch_json_feed(company.name, slug)

        # ── Fall back to HTML scraping ────────────────────────────────────────
        if not postings:
            jobs_url = f"{self.BASE_ORIGIN}/{slug}/jobs"
            html = self._fetch_html(jobs_url)
            if not html:
                html = self._fetch_html(f"{self.BASE_ORIGIN}/careers/{slug}/jobs")
            if html:
                postings = self._parse_postings(company.name, slug, html)

        if not postings:
            return []

        detail_fetched = 0
        for posting in postings:
            needs_location_detail = "locations" in (posting.get("location_raw") or "").lower()
            if posting.get("description") and not needs_location_detail:
                continue
            if detail_fetched >= DETAIL_FETCH_CAP:
                break
            url = posting.get("original_url", "")
            if url:
                detail = self._fetch_detail_data(url)
                detail_fetched += 1
                if detail.get("description"):
                    posting["description"] = detail["description"]
                if detail.get("requirements") and not posting.get("requirements"):
                    posting["requirements"] = detail["requirements"]
                if detail.get("responsibilities") and not posting.get("responsibilities"):
                    posting["responsibilities"] = detail["responsibilities"]
                if detail.get("location_raw"):
                    posting["location_raw"] = detail["location_raw"]
                    posting["location_candidates"] = detail.get("location_candidates") or []
                    location_type, is_remote = _detect_location_type(posting["location_raw"])
                    posting["location_type"] = location_type
                    posting["is_remote"] = is_remote
                    first_location = (posting["location_candidates"] or [posting["location_raw"]])[0]
                    city, state, country = _split_location(first_location)
                    posting["city"] = city
                    posting["state"] = state
                    posting["country"] = country
        return postings

    # ── JSON feed ─────────────────────────────────────────────────────────────

    def _fetch_json_feed(self, company_name: str, slug: str) -> list[dict]:
        """Try the Jobvite hidden JSON feed endpoint. Returns [] if unavailable."""
        url = JSON_FEED_URL.format(slug=slug)
        self._enforce_rate_limit()
        try:
            resp = self._session.get(
                url, timeout=DEFAULT_TIMEOUT,
                headers={"Accept": "application/json", "User-Agent": BOT_USER_AGENT},
            )
            self._last_request_at = __import__("time").monotonic()
            if not resp.ok:
                return []
            data = resp.json()
        except Exception:
            return []

        jobs_data = data if isinstance(data, list) else (data.get("jobs") or data.get("requisitions") or [])
        if not jobs_data:
            return []

        postings = []
        seen: set[str] = set()
        for j in jobs_data:
            jid = str(j.get("id") or j.get("jobId") or j.get("requisitionId") or "").strip()
            title = str(j.get("title") or j.get("jobTitle") or "").strip()
            if not title:
                continue
            href = j.get("url") or j.get("applyUrl") or j.get("link") or ""
            abs_url = urljoin(self.BASE_ORIGIN + "/", href) if href else ""
            if not abs_url:
                abs_url = f"{self.BASE_ORIGIN}/{slug}/job/{jid}" if jid else ""
            if abs_url in seen:
                continue
            seen.add(abs_url)

            location_raw = str(j.get("location") or j.get("locationText") or "").strip()
            location_type, is_remote = _detect_location_type(location_raw)
            city, state, country = _split_location(location_raw)
            department = str(j.get("department") or j.get("category") or "").strip()
            description = str(j.get("description") or j.get("descriptionHtml") or "").strip()
            if description:
                description = re.sub(r"<[^>]+>", " ", description)
                description = re.sub(r"\s+", " ", description).strip()
            postings.append({
                "external_id": jid or self._extract_id_from_url(abs_url),
                "original_url": abs_url,
                "apply_url": abs_url,
                "title": title,
                "company_name": company_name,
                "department": department,
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
                "description": description,
                "requirements": "",
                "responsibilities": "",
                "benefits": "",
                "posted_date_raw": str(j.get("datePosted") or j.get("postedDate") or ""),
                "closing_date": "",
                "raw_payload": j,
            })
        self.last_total_available = len(postings)
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

    def _fetch_detail_data(self, url: str) -> dict:
        """Fetch job detail page, return dict with description/requirements/responsibilities."""
        import json as _json
        html = self._fetch_html(url)
        if not html:
            return {}
        result: dict = {}

        # JSON-LD JobPosting
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
                        result["description"] = str(desc).strip()
                    break
            except Exception:
                continue

        # Jobvite HTML containers
        if "description" not in result:
            m = re.search(
                r'<div[^>]+class=["\'][^"\']*jv-job-detail-description[^"\']*["\'][^>]*>([\s\S]{100,}?)</div>',
                html, re.I,
            )
            if m:
                text = re.sub(r"<[^>]+>", " ", m.group(1))
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) > 100:
                    result["description"] = text

        # Extract requirements / responsibilities from section headers in description HTML
        for section_pat, key in [
            (r'(?:Requirements?|Qualifications?|Must\s+Have|Required\s+Skills?)', "requirements"),
            (r'(?:Responsibilities?|What\s+You.ll\s+Do|The\s+Role)', "responsibilities"),
        ]:
            header_re = re.compile(
                rf'<(?:h[1-6]|strong|b)[^>]*>\s*{section_pat}[^<]*</(?:h[1-6]|strong|b)>'
                r'([\s\S]{50,2000}?)(?=<(?:h[1-6]|strong|b)|$)',
                re.I,
            )
            sm = header_re.search(html)
            if sm:
                plain = re.sub(r"<[^>]+>", " ", sm.group(1))
                plain = re.sub(r"\s+", " ", plain).strip()
                if plain:
                    result[key] = plain

        location_raw = _detail_location_line(html, "")
        if location_raw:
            from harvest.location_resolver import split_multi_location_text
            result["location_raw"] = location_raw
            result["location_candidates"] = split_multi_location_text(location_raw)

        return result

    def _fetch_detail_description(self, url: str) -> str:
        """Legacy wrapper."""
        return self._fetch_detail_data(url).get("description", "")

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
                    "responsibilities": "",
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
