import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


_TRACKING_QUERY_KEYS = {
    "src",
    "source",
    "ref",
    "refs",
    "referrer",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "utm_name",
    "utm_reader",
    "gh_src",
    "gh_jid",
    "gh_jid_id",
    "gh_src_id",
    "li_fat_id",
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
}


def canonicalize_job_url(url: str) -> str:
    """
    Canonicalize ATS URLs so the same job hashes identically across trackers.

    Keeps identity-bearing path/query pieces but strips tracking noise.
    """
    raw = (url or "").strip()
    if not raw:
        return ""

    try:
        parsed = urlsplit(raw)
        scheme = (parsed.scheme or "https").lower()
        host = (parsed.hostname or "").lower()
        if not host:
            return raw

        port = parsed.port
        netloc = host
        if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
            netloc = f"{host}:{port}"

        path = re.sub(r"/{2,}", "/", parsed.path or "/")
        if len(path) > 1:
            path = path.rstrip("/")

        kept_pairs = []
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            key_norm = (key or "").strip().lower()
            if not key_norm:
                continue
            if key_norm in _TRACKING_QUERY_KEYS or key_norm.startswith("utm_"):
                continue
            kept_pairs.append((key, value))

        query = urlencode(sorted(kept_pairs), doseq=True)
        return urlunsplit((scheme, netloc, path or "/", query, ""))
    except Exception:
        return raw


def compute_url_hash(url: str) -> str:
    canonical = canonicalize_job_url(url)
    if not canonical:
        return ""
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_content_hash(company_id: int, title: str, location_raw: str) -> str:
    """
    Stable identity hash for catching cross-platform duplicates at ingestion time.
    Same company + same normalized title + same location = same hash.
    NOT unique in DB — intentionally allows re-posts after a job closes.
    """
    def _norm(s: str) -> str:
        s = (s or "").lower().strip()
        s = re.sub(r"\s+", " ", s)
        # Strip trailing remote/hybrid/onsite qualifiers that vary by board
        s = re.sub(r"[\-–—]\s*(remote|hybrid|onsite|on.site)\s*$", "", s)
        s = re.sub(r"\s*\((remote|hybrid|onsite|on.site)\)\s*$", "", s)
        return s.strip()

    key = f"{company_id}|{_norm(title)}|{_norm(location_raw)}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


def strip_html(html: str) -> str:
    if not html:
        return ""
    return re.sub(r"<[^>]+>", " ", html).strip()


def extract_salary(raw: str) -> tuple[Optional[float], Optional[float], str]:
    if not raw:
        return None, None, "USD"

    currency = "USD"
    if "£" in raw:
        currency = "GBP"
    elif "€" in raw:
        currency = "EUR"

    nums = re.findall(r"[\d,]+(?:\.\d+)?[kK]?", raw)
    parsed = []
    for n in nums:
        n = n.replace(",", "")
        try:
            if n.lower().endswith("k"):
                parsed.append(float(n[:-1]) * 1000)
            else:
                v = float(n)
                if v > 0:
                    parsed.append(v)
        except ValueError:
            pass

    if len(parsed) >= 2:
        return min(parsed), max(parsed), currency
    elif len(parsed) == 1:
        return parsed[0], parsed[0], currency
    return None, None, currency


def detect_remote(text: str) -> Optional[bool]:
    if not text:
        return None
    lower = text.lower()
    if any(k in lower for k in ["remote", "work from home", "wfh", "anywhere", "distributed"]):
        return True
    if any(k in lower for k in ["on-site", "onsite", "in-office", "on site"]):
        return False
    return None


def normalize_job_data(
    raw_job: dict[str, Any],
    platform,
    company,
    harvest_run,
) -> dict[str, Any]:
    """Convert raw harvester output dict to normalized field values (Phase 5: no HarvestedJob)."""
    _VALID_JOB_TYPES = {"FULL_TIME", "PART_TIME", "CONTRACT", "INTERNSHIP", "UNKNOWN"}

    original_url = raw_job.get("original_url", "").strip()
    url_hash = compute_url_hash(original_url) if original_url else ""

    title = raw_job.get("title", "").strip()
    company_name = raw_job.get("company_name", company.name if company else "").strip()
    location = raw_job.get("location", "").strip()

    salary_raw = raw_job.get("salary_raw", "")
    sal_min, sal_max, currency = extract_salary(salary_raw)

    is_remote = raw_job.get("is_remote")
    if is_remote is None:
        is_remote = detect_remote(location) or detect_remote(title)

    description_html = raw_job.get("description_html", "")
    description_text = raw_job.get("description_text", "") or strip_html(description_html)

    job_type = raw_job.get("job_type", "UNKNOWN")
    if job_type not in _VALID_JOB_TYPES:
        job_type = "UNKNOWN"

    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=24)

    posted_date = None
    posted_raw = raw_job.get("posted_date_raw", "")
    if posted_raw:
        try:
            if "T" in posted_raw or "+" in posted_raw or "Z" in posted_raw:
                posted_date = datetime.fromisoformat(
                    posted_raw.replace("Z", "+00:00")
                ).date()
        except Exception:
            pass

    return {
        "company": company,
        "platform": platform,
        "external_id": str(raw_job.get("external_id", ""))[:500],
        "url_hash": url_hash,
        "original_url": original_url[:1000],
        "title": title[:300],
        "company_name": company_name[:255],
        "location": location[:255],
        "is_remote": is_remote,
        "job_type": job_type,
        "department": str(raw_job.get("department", ""))[:255],
        "salary_min": sal_min,
        "salary_max": sal_max,
        "salary_currency": currency,
        "salary_raw": salary_raw[:200],
        "description_html": description_html,
        "description_text": description_text[:50000],
        "requirements_text": raw_job.get("requirements_text", ""),
        "benefits_text": raw_job.get("benefits_text", ""),
        "posted_date": posted_date,
        "expires_at": expires_at,
        "is_active": True,
        "sync_status": "PENDING",
        "raw_payload": raw_job.get("raw_payload", {}),
    }
