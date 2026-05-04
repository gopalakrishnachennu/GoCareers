from __future__ import annotations

import html
import re
from dataclasses import dataclass
from urllib.parse import urlparse

import requests


_WS_RE = re.compile(r"\s+")

# Generic signals for job pages that render an error page with HTTP 200.
_DEAD_MARKERS_GENERIC = (
    "page you are looking for doesnt exist",
    "page you are looking for does not exist",
    "job not found",
    "this job is no longer available",
    "position is no longer available",
    "posting is no longer available",
    "we couldnt find the job",
    "we couldn't find the job",
    "requisition is no longer available",
    "position has been filled",
    "position is filled",
    "no longer accepting applications",
    "job posting has expired",
    "this posting has expired",
    "position has been closed",
    "this position has been closed",
    "this job has been closed",
    "this requisition is no longer active",
    "requisition has been removed",
    "opportunity has expired",
)

_DEAD_MARKERS_BY_PLATFORM = {
    "workday": (
        "the page you are looking for doesnt exist",
        "the page you are looking for does not exist",
        "the job you are trying to view is no longer available",
        "we are unable to find the job you are looking for",
    ),
    "icims": (
        "job description no longer available",
        "this opportunity is no longer available",
        "this job is no longer posted",
        "this position is no longer posted",
    ),
    "greenhouse": (
        "this job has been filled",
        "this role is no longer open",
    ),
    "lever": (
        "this posting is no longer available",
        "the position has been filled",
    ),
}

_LIVE_MARKERS_GENERIC = (
    "job description",
    "responsibilities",
    "qualifications",
    "requirements",
    "about the role",
    "about this role",
    "what you'll do",
    "what you will do",
    "apply now",
    "apply for this",
)

_LIVE_MARKERS_BY_PLATFORM = {
    "workday": (
        "job profile summary",
        "posted",
        "locations",
        "apply now",
    ),
    "icims": (
        "apply for this job",
        "job summary",
        "job description",
    ),
}

_UA = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; Harvest-LinkHealth/1.0; +https://chennu.co)"
    )
}


@dataclass(frozen=True)
class LinkHealthResult:
    is_live: bool
    status_code: int
    reason: str
    final_url: str


def _norm_text(raw: str) -> str:
    txt = html.unescape(raw or "").lower()
    txt = txt.replace("’", "'")
    txt = txt.replace("doesn't", "doesnt")
    txt = _WS_RE.sub(" ", txt).strip()
    return txt


def _contains_dead_marker(text: str, platform_slug: str) -> bool:
    if not text:
        return False
    markers = list(_DEAD_MARKERS_GENERIC)
    markers.extend(_DEAD_MARKERS_BY_PLATFORM.get((platform_slug or "").lower(), ()))
    return any(m in text for m in markers)


def _contains_live_marker(text: str, platform_slug: str) -> bool:
    if not text:
        return False
    markers = list(_LIVE_MARKERS_GENERIC)
    markers.extend(_LIVE_MARKERS_BY_PLATFORM.get((platform_slug or "").lower(), ()))
    return any(m in text for m in markers)


def _looks_like_detail_path(path: str, platform_slug: str) -> bool:
    p = (path or "").lower()
    if not p:
        return False
    slug = (platform_slug or "").lower()
    if slug == "workday":
        return "/job/" in p or "/details/" in p
    if slug == "icims":
        return "/jobs/" in p and "/search" not in p
    return any(seg in p for seg in ("/job/", "/jobs/", "/details/", "/positions/"))


def is_definitive_inactive(result: LinkHealthResult) -> bool:
    """
    Decision policy for flipping DB rows inactive.

    Fires on:
    - Hard HTTP errors: 404/410/451
    - Soft-404 markers detected in HTML
    - Redirect-to-search with no live signals
    - Platform API: job not found in API response (Workday, Oracle, Greenhouse,
      Lever, Ashby, SmartRecruiters, BambooHR, iCIMS)
    """
    if result.is_live:
        return False
    reason = (result.reason or "").lower()
    code = int(result.status_code or 0)
    if reason.startswith("http_"):
        return code in {404, 410, 451}
    if reason in {
        "soft_404_marker",
        "redirected_to_search_soft404",
        "redirected_to_non_detail_no_live_signals",
        # Workday
        "workday_cxs_not_found",
        "workday_search_no_match",
        # Oracle HCM
        "oracle_hcm_not_found",
        "oracle_hcm_no_results",
        # Greenhouse
        "greenhouse_api_not_found",
        # Lever
        "lever_api_not_found",
        # Ashby
        "ashby_api_not_found",
        # SmartRecruiters
        "smartrecruiters_api_not_found",
        # BambooHR
        "bamboohr_api_not_found",
        # iCIMS
        "icims_api_not_found",
    }:
        return True
    return False


def _workday_cxs_liveness(url: str) -> LinkHealthResult | None:
    """
    Ask Workday CXS JSON endpoint directly for this detail URL.
    Returns:
      - LinkHealthResult(..., is_live=True/False, reason=workday_cxs_*)
      - None when URL shape is not Workday-detail compatible.
    """
    m = re.match(
        r"https?://([\w-]+(?:\.wd\d+)?)\.myworkdayjobs\.com/(?:[a-zA-Z]{2}-[a-zA-Z]{2}/)?([^/?#]+)(/(?:details|job)/[^?#]+)",
        url,
        re.I,
    )
    if not m:
        return None

    full_subdomain = m.group(1)
    jobboard = m.group(2)
    ext_path = m.group(3).split("?")[0]
    tenant = re.sub(r"\.wd\d+$", "", full_subdomain, flags=re.I)
    cxs_url = f"https://{full_subdomain}.myworkdayjobs.com/wday/cxs/{tenant}/{jobboard}{ext_path}"

    # common req id patterns at the end of slug, e.g. _JR-023060 or _R2115899
    req_id = ""
    m_req = re.search(r"_([A-Za-z]+-?\d{3,})$", ext_path)
    if m_req:
        req_id = m_req.group(1)

    try:
        resp = requests.get(
            cxs_url,
            headers={"Accept": "application/json", **_UA},
            timeout=10,
        )
        status = int(resp.status_code or 0)
        if status >= 400:
            # Some tenants block detail CXS for bots (403). Fallback to searchable CXS jobs endpoint.
            if status in {401, 403}:
                search_url = f"https://{full_subdomain}.myworkdayjobs.com/wday/cxs/{tenant}/{jobboard}/jobs"
                if req_id:
                    try:
                        q = requests.post(
                            search_url,
                            json={"limit": 20, "offset": 0, "searchText": req_id, "appliedFacets": {}},
                            headers={"Accept": "application/json", **_UA},
                            timeout=10,
                        )
                        q_status = int(q.status_code or 0)
                        if q_status < 400:
                            data_q = q.json() if q.content else {}
                            total = int((data_q or {}).get("total") or 0)
                            if total > 0:
                                return LinkHealthResult(True, q_status, "workday_search_match", search_url)
                            return LinkHealthResult(False, q_status, "workday_search_no_match", search_url)
                    except Exception:
                        pass
            return LinkHealthResult(False, status, "workday_cxs_http_error", cxs_url)
        data = resp.json() if resp.content else {}
        if not isinstance(data, dict):
            return LinkHealthResult(False, status, "workday_cxs_non_json", cxs_url)

        info = data.get("jobPostingInfo") or data
        # canonical live signals
        for key in ("title", "jobDescription", "jobPostingDescription", "externalJobDescription", "bulletFields"):
            val = info.get(key)
            if isinstance(val, (str, list, dict)) and str(val).strip():
                return LinkHealthResult(True, status, "workday_cxs_live", cxs_url)

        raw_text = _norm_text(str(data))
        if any(k in raw_text for k in ("not found", "doesnt exist", "does not exist", "no longer available")):
            return LinkHealthResult(False, status, "workday_cxs_not_found", cxs_url)
        return LinkHealthResult(False, status, "workday_cxs_empty", cxs_url)
    except Exception:
        return LinkHealthResult(False, 0, "workday_cxs_error", cxs_url)


def _oracle_hcm_liveness(url: str) -> "LinkHealthResult | None":
    """
    Oracle HCM CX pages are SPAs — the 'This job is no longer available' message
    is rendered by JavaScript and invisible to a plain GET request.

    Instead, query the Oracle HCM REST API directly for the requisition.
    Returns None if URL doesn't match Oracle HCM pattern.
    """
    m = re.search(
        r"([\w.-]+\.oraclecloud\.com)/hcmUI/CandidateExperience/[^/]+/sites/([^/]+)/(?:requisitions?(?:/preview)?|jobs?)/(\d+)",
        url,
        re.I,
    )
    if not m:
        return None

    host, sites_id, req_num = m.group(1), m.group(2), m.group(3)
    # Query Oracle HCM detail endpoint by requisition ID directly.
    # The detail endpoint is more reliable than the list endpoint for individual lookups.
    detail_url = (
        f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
        f"?onlyData=true&expand=all"
        f"&finder=ById;Id={req_num},siteNumber={sites_id}"
    )

    try:
        resp = requests.get(
            detail_url,
            headers={"Accept": "application/json", **_UA},
            timeout=15,
        )
        status = int(resp.status_code or 0)
        if status == 404:
            return LinkHealthResult(False, status, "oracle_hcm_not_found", detail_url)
        if status >= 400:
            # API not accessible — fall through to HTML check
            return None
        data = resp.json() if resp.content else {}
        items = data.get("items") or []
        if items and isinstance(items[0], dict):
            # Check if the posting is still active (ExternalPostedEndDate not set, or in future)
            return LinkHealthResult(True, status, "oracle_hcm_live", detail_url)
        # Empty items = requisition not found or not publicly visible = closed
        return LinkHealthResult(False, status, "oracle_hcm_not_found", detail_url)
    except Exception:
        return None


def _greenhouse_liveness(url: str) -> "LinkHealthResult | None":
    """Greenhouse boards-api: 404 = definitively closed."""
    m = re.search(r"boards\.greenhouse\.io/([^/]+)/jobs/(\d+)", url, re.I)
    if not m:
        return None
    board_token, job_id = m.group(1), m.group(2)
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs/{job_id}"
    try:
        resp = requests.get(api_url, headers={"Accept": "application/json", **_UA}, timeout=10)
        status = int(resp.status_code or 0)
        if status == 404:
            return LinkHealthResult(False, status, "greenhouse_api_not_found", api_url)
        if status >= 400:
            return None  # inconclusive (rate limit, server error)
        d = resp.json() if resp.content else {}
        return LinkHealthResult(True, status, "greenhouse_api_live", api_url) if d.get("id") else None
    except Exception:
        return None


def _lever_liveness(url: str) -> "LinkHealthResult | None":
    """Lever public postings API: 404 = closed."""
    m = re.search(r"jobs\.lever\.co/([^/]+)/([0-9a-f-]{36})", url, re.I)
    if not m:
        return None
    company, posting_id = m.group(1), m.group(2)
    api_url = f"https://api.lever.co/v0/postings/{company}/{posting_id}"
    try:
        resp = requests.get(api_url, headers={"Accept": "application/json", **_UA}, timeout=10)
        status = int(resp.status_code or 0)
        if status == 404:
            return LinkHealthResult(False, status, "lever_api_not_found", api_url)
        if status >= 400:
            return None
        d = resp.json() if resp.content else {}
        return LinkHealthResult(True, status, "lever_api_live", api_url) if d.get("id") else None
    except Exception:
        return None


def _ashby_liveness(url: str) -> "LinkHealthResult | None":
    """Ashby board API: job missing from board = closed."""
    m = re.search(r"jobs\.ashbyhq\.com/([^/]+)/([0-9a-f-]{36})", url, re.I)
    if not m:
        return None
    company, job_id = m.group(1), m.group(2)
    api_url = f"https://api.ashbyhq.com/posting-api/job-board/{company}"
    try:
        resp = requests.get(api_url, headers={"Accept": "application/json", **_UA}, timeout=12)
        status = int(resp.status_code or 0)
        if status >= 400:
            return None
        jobs = (resp.json() if resp.content else {}).get("jobs") or []
        match = next((j for j in jobs if (j.get("id") or "").lower() == job_id.lower()), None)
        if match:
            return LinkHealthResult(True, status, "ashby_api_live", api_url)
        if jobs:
            # Board returned results but this job wasn't in it → gone
            return LinkHealthResult(False, status, "ashby_api_not_found", api_url)
        return None  # empty board = inconclusive
    except Exception:
        return None


def _smartrecruiters_liveness(url: str) -> "LinkHealthResult | None":
    """SmartRecruiters public API: 404 = closed."""
    m = re.search(r"(?:jobs\.smartrecruiters\.com|smartrecruiters\.com)/([^/]+)/(\d+)", url, re.I)
    if not m:
        # Handle legacy records where original_url was accidentally set to the API URL
        # e.g. api.smartrecruiters.com/v1/companies/{company}/postings/{id}
        m = re.search(r"api\.smartrecruiters\.com/v1/companies/([^/]+)/postings/(\d+)", url, re.I)
        if not m:
            return None
    company, job_id = m.group(1), m.group(2)
    api_url = f"https://api.smartrecruiters.com/v1/companies/{company}/postings/{job_id}"
    try:
        resp = requests.get(api_url, headers={"Accept": "application/json", **_UA}, timeout=12)
        status = int(resp.status_code or 0)
        if status == 404:
            return LinkHealthResult(False, status, "smartrecruiters_api_not_found", api_url)
        if status >= 400:
            return None
        d = resp.json() if resp.content else {}
        return LinkHealthResult(True, status, "smartrecruiters_api_live", api_url) if d.get("id") else None
    except Exception:
        return None


def _bamboohr_liveness(url: str) -> "LinkHealthResult | None":
    """BambooHR careers page: 404 on detail URL = closed."""
    m = re.search(r"([\w-]+)\.bamboohr\.com/(?:careers|jobs)/(\d+)", url, re.I)
    if not m:
        return None
    subdomain, job_id = m.group(1), m.group(2)
    api_url = f"https://{subdomain}.bamboohr.com/careers/json/jobs/{job_id}"
    try:
        resp = requests.get(api_url, headers={"Accept": "application/json", **_UA}, timeout=10)
        status = int(resp.status_code or 0)
        if status == 404:
            return LinkHealthResult(False, status, "bamboohr_api_not_found", api_url)
        if status >= 400:
            return None
        d = resp.json() if resp.content else {}
        return LinkHealthResult(True, status, "bamboohr_api_live", api_url) if d.get("id") or d.get("title") else None
    except Exception:
        return None


# Map URL hostname/pattern → liveness function for quick lookup
_PLATFORM_LIVENESS_REGISTRY: list[tuple[str, object]] = [
    ("myworkdayjobs.com",     _workday_cxs_liveness),
    ("oraclecloud.com",       _oracle_hcm_liveness),
    ("greenhouse.io",         _greenhouse_liveness),
    ("lever.co",              _lever_liveness),
    ("ashbyhq.com",           _ashby_liveness),
    ("smartrecruiters.com",   _smartrecruiters_liveness),
    ("bamboohr.com",          _bamboohr_liveness),
]

_INCONCLUSIVE_API_REASONS = {
    "workday_cxs_error",
    "workday_cxs_http_error",
    "workday_cxs_non_json",
    "workday_cxs_empty",
}


def check_job_posting_live(
    url: str,
    *,
    platform_slug: str = "",
    timeout_head: int = 10,
    timeout_get: int = 12,
    max_read_bytes: int = 32768,
) -> LinkHealthResult:
    url = (url or "").strip()
    if not url:
        return LinkHealthResult(False, 0, "missing_url", "")
    if not urlparse(url).scheme:
        url = "https://" + url

    # Platform-specific API checks first — these are definitive and avoid false positives
    # from SPA soft-404 pages that serve HTTP 200 with a "job gone" message in JS.
    url_lower = url.lower()
    for hostname_fragment, liveness_fn in _PLATFORM_LIVENESS_REGISTRY:
        if hostname_fragment in url_lower:
            result = liveness_fn(url)
            if result is not None:
                # Some API probes can fail for tenant/firewall reasons while the
                # actual detail page is still available. Fall back to HTML checks
                # for these inconclusive states instead of short-circuiting.
                if (result.reason or "") in _INCONCLUSIVE_API_REASONS:
                    break
                return result
            break  # matched platform but API was inconclusive — fall through to HTML check

    # HEAD first: fast path
    try:
        r_head = requests.head(
            url,
            timeout=timeout_head,
            allow_redirects=True,
            headers=_UA,
        )
        status = int(r_head.status_code or 0)
        final_url = str(getattr(r_head, "url", "") or url)
    except Exception:
        r_head = None
        status = 0
        final_url = url

    # Hard failures
    if status in {404, 410, 451}:
        return LinkHealthResult(False, status, f"http_{status}", final_url)
    # Rate-limited / temporary server failures are inconclusive. Do not hard-fail yet.

    # GET + body sniff for soft-404 detection (needed for Workday/iCIMS, etc.)
    try:
        r_get = requests.get(
            url,
            timeout=timeout_get,
            allow_redirects=True,
            headers=_UA,
            stream=True,
        )
        status_get = int(r_get.status_code or 0)
        final_url = str(getattr(r_get, "url", "") or final_url or url)
        if status_get in {404, 410, 451}:
            r_get.close()
            return LinkHealthResult(False, status_get, f"http_{status_get}", final_url)
        if status_get in {429, 500, 502, 503, 504}:
            # Temporary throttling/upstream issues: treat as unknown-live to prevent
            # accidental mass deactivation.
            r_get.close()
            return LinkHealthResult(True, status_get, f"transient_http_{status_get}", final_url)

        body_bytes = r_get.raw.read(max_read_bytes, decode_content=True) or b""
        r_get.close()
        text = _norm_text(body_bytes.decode("utf-8", errors="ignore"))

        # If the resulting URL already points to search/home routes, it's likely no longer a detail posting.
        path_l = urlparse(final_url).path.lower()
        detail_path = _looks_like_detail_path(path_l, platform_slug)
        dead_marker = _contains_dead_marker(text, platform_slug)
        live_marker = _contains_live_marker(text, platform_slug)

        if any(seg in path_l for seg in ("/jobs/search", "/search", "/job-search")) and not any(
            seg in path_l for seg in ("/job/", "/details/")
        ):
            if dead_marker:
                return LinkHealthResult(False, status_get, "redirected_to_search_soft404", final_url)
            if not live_marker:
                return LinkHealthResult(False, status_get, "redirected_to_non_detail_no_live_signals", final_url)

        if dead_marker:
            return LinkHealthResult(False, status_get, "soft_404_marker", final_url)

        if detail_path and live_marker:
            return LinkHealthResult(True, status_get, "detail_live_markers", final_url)

        if detail_path and len(text) > 800:
            return LinkHealthResult(True, status_get, "detail_long_content", final_url)

        return LinkHealthResult(True, status_get, "ok", final_url)
    except Exception:
        # If GET fails after a successful HEAD<400, keep live as unknown to reduce false negatives.
        if 0 < status < 400:
            return LinkHealthResult(True, status, "head_ok_get_failed", final_url)
        # Network errors with no hard signal are treated as unknown-live.
        return LinkHealthResult(True, status or 0, "request_error_unknown", final_url)
