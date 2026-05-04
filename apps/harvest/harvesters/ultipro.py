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
DETAIL_FETCH_CAP = 50  # inline JD fetches per company during harvest


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

        # If no jobboard_id, try to discover it via the JobBoardView redirect.
        # GET /{code}/JobBoardView → 302 → /{code}/JobBoard/{GUID}
        if not jobboard_id:
            jobboard_id = self._discover_guid(company_code)

        # Path 1: Board-scoped LoadSearchResults API (requires GUID)
        if jobboard_id:
            api_results = self._fetch_board_api(company_code, jobboard_id, company.name, fetch_all)
            if api_results:
                return api_results

        # Path 2: HTML scrape (fallback for companies whose GUID can't be discovered)
        board_url = (
            f"https://recruiting.ultipro.com/{company_code}/JobBoard/{jobboard_id}"
            if jobboard_id
            else f"https://recruiting.ultipro.com/{company_code}/JobBoard"
        )
        return self._scrape_html(board_url, company.name)

    # ── GUID discovery ────────────────────────────────────────────────────────

    def _discover_guid(self, company_code: str) -> str:
        """Hit /{code}/JobBoardView and extract the GUID from the redirect URL.

        UltiPro normalises company-code → canonical JobBoard GUID on first load.
        The final URL is /{code}/JobBoard/{GUID}.
        Returns empty string if discovery fails (company not on UltiPro).
        """
        import re as _re
        url = f"https://recruiting.ultipro.com/{company_code}/JobBoardView"
        self._enforce_rate_limit()
        try:
            resp = self._session.get(
                url,
                timeout=15,
                allow_redirects=True,
                headers={"User-Agent": "GoCareers-Bot/1.0 (+https://gocareers.io/bot)"},
            )
            self._last_request_at = __import__("time").monotonic()
            m = _re.search(
                r"/JobBoard/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
                resp.url,
                _re.I,
            )
            return m.group(1) if m else ""
        except Exception:
            return ""

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
                results.append(self._normalize_api(j, company_code, jobboard_id, company_name))

            total = int(data.get("totalCount") or 0)
            if total:
                self.last_total_available = total
            skip += len(jobs)
            if not fetch_all or not total or skip >= total or skip >= (MAX_PAGES * PAGE_SIZE):
                break
            _t.sleep(MIN_DELAY_API)

        # Inline JD fetch for the first DETAIL_FETCH_CAP jobs via GetJob endpoint.
        for i, posting in enumerate(results):
            if i >= DETAIL_FETCH_CAP:
                break
            if posting.get("description"):
                continue
            job_url = posting.get("original_url", "")
            detail = self._fetch_job_detail(company_code, jobboard_id, job_url)
            if detail.get("description"):
                posting["description"] = detail["description"]
            if detail.get("requirements") and not posting.get("requirements"):
                posting["requirements"] = detail["requirements"]
            if detail.get("responsibilities") and not posting.get("responsibilities"):
                posting["responsibilities"] = detail["responsibilities"]

        return results

    def _normalize_api(self, j: dict, company_code: str, jobboard_id: str, company_name: str) -> dict:
        # LoadSearchResults response: PascalCase keys
        # {"Id": uuid, "Title": str, "FullTime": bool, "Locations": [{"LocalizedName": ...}], ...}
        job_id = j.get("Id") or j.get("id") or j.get("requisitionId") or j.get("jobId") or ""
        title = j.get("Title") or j.get("jobTitle") or j.get("title") or ""

        # Location comes from Locations[0].LocalizedName — e.g. "PA - Duquesne"
        locs = j.get("Locations") or []
        location_parts = []
        if locs and isinstance(locs[0], dict):
            loc_name = (locs[0].get("LocalizedName") or locs[0].get("LocalizedLocation") or "").strip()
            if loc_name:
                location_parts.append(loc_name)
        # Fallback: flat city/state fields
        if not location_parts:
            city = j.get("city") or j.get("AddressCity") or ""
            state = j.get("state") or j.get("stateCode") or j.get("AddressState") or ""
            country = j.get("country") or j.get("countryCode") or j.get("AddressCountry") or ""
            location_parts = [x for x in [city, state, country] if x]

        location_raw = ", ".join(location_parts)

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

        # FullTime boolean flag
        is_full_time = j.get("FullTime")
        if is_full_time is True:
            employment_type = "FULL_TIME"
        elif is_full_time is False:
            employment_type = "PART_TIME"
        else:
            emp_raw = (j.get("employmentType") or j.get("jobType") or j.get("JobCategoryName") or "").lower()
            emp_map = {
                "full time": "FULL_TIME",
                "full-time": "FULL_TIME",
                "part time": "PART_TIME",
                "part-time": "PART_TIME",
                "contract": "CONTRACT",
                "temporary": "TEMPORARY",
                "internship": "INTERNSHIP",
                "intern": "INTERNSHIP",
            }
            employment_type = emp_map.get(emp_raw, "UNKNOWN")

        # Build OpportunityDetail URL using job Id + board GUID.
        # Format: /{company_code}/JobBoard/{board_guid}/OpportunityDetail?opportunityId={job_id}
        links = j.get("Links") or {}
        opp_detail = links.get("OpportunityDetail") or ""
        if opp_detail and not opp_detail.startswith("http"):
            opp_detail = f"https://recruiting.ultipro.com{opp_detail}"
        if not opp_detail and jobboard_id and job_id:
            opp_detail = (
                f"https://recruiting.ultipro.com/{company_code}/JobBoard/{jobboard_id}"
                f"/OpportunityDetail?opportunityId={job_id}"
            )
        job_url = (
            opp_detail
            or j.get("applyUrl")
            or j.get("url")
            or f"https://recruiting.ultipro.com/{company_code}/JobBoard"
        )

        return {
            "external_id": j.get("RequisitionNumber") or str(job_id),
            "original_url": job_url,
            "apply_url": job_url,
            "title": title,
            "company_name": company_name,
            "department": j.get("JobCategoryName") or j.get("department") or j.get("businessUnit") or "",
            "team": "",
            "location_raw": location_raw,
            "city": "",
            "state": "",
            "country": "",
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
            "posted_date_raw": j.get("PostedDate") or j.get("postedDate") or j.get("datePosted") or "",
            "closing_date": "",
            "raw_payload": j,
        }

    # ── Per-job detail fetch ──────────────────────────────────────────────────

    def _fetch_job_detail(self, company_code: str, jobboard_id: str, job_url: str) -> dict:
        """Fetch full job detail for a single UltiPro job. Returns dict with description/requirements/responsibilities."""
        import re as _re
        import time as _t
        import json as _json

        result: dict = {}

        # Path 1: GetJob JSON endpoint
        opp_m = _re.search(r"opportunityId=([^&\s]+)", job_url, _re.I)
        if opp_m and jobboard_id:
            opp_id = opp_m.group(1)
            get_job_url = (
                f"https://recruiting.ultipro.com/{company_code}/JobBoard/{jobboard_id}"
                f"/JobBoardView/GetJob?opportunityId={opp_id}"
            )
            self._enforce_rate_limit()
            try:
                resp = self._session.get(get_job_url, headers={"Accept": "application/json"}, timeout=15)
                self._last_request_at = _t.monotonic()
                if resp.ok:
                    d = resp.json()
                    desc = (
                        d.get("Description") or d.get("description")
                        or d.get("JobDescription") or d.get("FullDescription") or ""
                    )
                    if desc and len(str(desc)) > 80:
                        result["description"] = str(desc).strip()
                    req = d.get("Qualifications") or d.get("Requirements") or ""
                    if req:
                        result["requirements"] = str(req).strip()
                    resp_text = d.get("Responsibilities") or d.get("EssentialFunctions") or ""
                    if resp_text:
                        result["responsibilities"] = str(resp_text).strip()
            except Exception:
                pass

        if result.get("description"):
            return result

        # Path 2: HTML page + JSON-LD
        self._enforce_rate_limit()
        try:
            resp2 = self._session.get(
                job_url, timeout=15,
                headers={"Accept": "text/html", "User-Agent": "Mozilla/5.0 (compatible; GoCareers-Bot/1.0)"},
            )
            self._last_request_at = _t.monotonic()
            if resp2.ok:
                html = resp2.text
                for block in _re.findall(
                    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, _re.S | _re.I
                ):
                    try:
                        schema = _json.loads(block)
                        if isinstance(schema, list):
                            schema = schema[0]
                        if isinstance(schema, dict) and schema.get("@type") == "JobPosting":
                            d2 = schema.get("description") or ""
                            if d2 and len(str(d2)) > 80:
                                result["description"] = str(d2).strip()
                            break
                    except Exception:
                        continue
                # Section extraction from HTML headers
                for section_pat, key in [
                    (r'(?:Requirements?|Qualifications?|Required\s+Skills?)', "requirements"),
                    (r'(?:Responsibilities?|Essential\s+Functions?)', "responsibilities"),
                ]:
                    if result.get(key):
                        continue
                    hdr = _re.compile(
                        rf'<(?:h[1-6]|strong|b)[^>]*>\s*{section_pat}[^<]*</(?:h[1-6]|strong|b)>'
                        r'([\s\S]{{50,2000}}?)(?=<(?:h[1-6]|strong|b)|$)',
                        _re.I,
                    )
                    sm = hdr.search(html)
                    if sm:
                        plain = _re.sub(r"<[^>]+>", " ", sm.group(1))
                        plain = _re.sub(r"\s+", " ", plain).strip()
                        if plain:
                            result[key] = plain
        except Exception:
            pass
        return result

    def _fetch_job_description(self, company_code: str, jobboard_id: str, job_url: str) -> str:
        """Legacy wrapper."""
        return self._fetch_job_detail(company_code, jobboard_id, job_url).get("description", "")

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
