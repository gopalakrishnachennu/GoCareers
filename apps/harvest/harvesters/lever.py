"""
LeverHarvester — Public Lever Posting API

Lever exposes a publicly documented API at:
  https://api.lever.co/v0/postings/{company}

This is their official public posting endpoint — no auth required.
Documentation: https://hire.lever.co/developer/postings

Compliance:
  - Honest User-Agent (inherited from BaseHarvester)
  - 1-second minimum delay (BaseHarvester rate limit)
  - Retry + backoff on server errors (BaseHarvester)
  - fetch_all=True fetches all pages via offset pagination
  - date filtering — only returns jobs created within since_hours window when fetch_all=False
"""
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from .base import BaseHarvester, MIN_DELAY_API

BASE_URL = "https://api.lever.co/v0/postings/{company}"
PAGE_SIZE = 250

_COMMITMENT_MAP = {
    "full-time": "FULL_TIME",
    "full time": "FULL_TIME",
    "fulltime": "FULL_TIME",
    "part-time": "PART_TIME",
    "part time": "PART_TIME",
    "contract": "CONTRACT",
    "contractor": "CONTRACT",
    "intern": "INTERNSHIP",
    "internship": "INTERNSHIP",
    "temporary": "TEMPORARY",
    "temp": "TEMPORARY",
}


def _map_commitment(commitment: str) -> str:
    cl = commitment.lower().strip()
    if cl in _COMMITMENT_MAP:
        return _COMMITMENT_MAP[cl]
    for key, val in _COMMITMENT_MAP.items():
        if key in cl:
            return val
    return "UNKNOWN"


def _detect_location_type(location_raw: str) -> tuple[str, bool]:
    loc_lower = location_raw.lower()
    if "remote" in loc_lower:
        return "REMOTE", True
    if "hybrid" in loc_lower:
        return "HYBRID", False
    if location_raw.strip():
        return "ONSITE", False
    return "UNKNOWN", False


def _detect_experience_level(title: str, description: str) -> str:
    combined = (title + " " + description).lower()
    if any(k in combined for k in ("intern", "internship", "co-op", "coop")):
        return "ENTRY"
    if any(k in combined for k in ("chief ", "cto", "ceo", "coo", "cfo", "svp", "evp", "vp ", "vice president")):
        return "EXECUTIVE"
    if any(k in combined for k in ("director", "head of")):
        return "DIRECTOR"
    if any(k in combined for k in ("manager", "mgr")):
        return "MANAGER"
    if any(k in combined for k in ("lead ", "principal", "staff ")):
        return "LEAD"
    if any(k in combined for k in ("senior", "sr.", "sr ")):
        return "SENIOR"
    if any(k in combined for k in ("junior", "jr.", "jr ", "entry", "associate")):
        return "ENTRY"
    return "MID"


_SALARY_PERIOD_RE = re.compile(r"\b(hour|hr|hourly|month|monthly|annual|year|yr)\b", re.I)


def _parse_lever_salary(text: str) -> tuple:
    """Extract (sal_min, sal_max, period, raw) from a compensation string."""
    if not text:
        return None, None, "", ""
    nums = []
    for m in re.finditer(r'\$([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*([Kk])?', text):
        v = float(m.group(1).replace(",", ""))
        if m.group(2):
            v *= 1000
        if v > 0:
            nums.append(v)
    if not nums:
        return None, None, "", ""
    sal_min = min(nums)
    sal_max = max(nums) if len(nums) > 1 else sal_min
    pm = _SALARY_PERIOD_RE.search(text)
    period = "HOUR" if pm and "hour" in pm.group(1).lower() or "hr" in pm.group(1).lower() else (
        "MONTH" if pm and "month" in pm.group(1).lower() else "YEAR"
    )
    raw = text[:200]
    return sal_min, sal_max, period, raw


def _extract_lever_sections(lists: list) -> tuple[str, str, str, str]:
    """Parse Lever `lists` array into (requirements, responsibilities, benefits, salary_text)."""
    requirements = responsibilities = benefits = salary_text = ""
    for item in lists or []:
        label = (item.get("text") or "").lower().strip()
        content = item.get("content") or ""
        plain = re.sub(r"<[^>]+>", " ", content)
        plain = re.sub(r"\s+", " ", plain).strip()
        if any(k in label for k in ("requirement", "qualification", "skill", "must have", "you have", "you bring")):
            requirements = plain
        elif any(k in label for k in ("responsib", "what you'll do", "you will", "the role", "your role")):
            responsibilities = plain
        elif any(k in label for k in ("benefit", "perk", "we offer", "offer")):
            benefits = plain
        elif any(k in label for k in ("compensat", "salary", "pay")):
            salary_text = plain
    return requirements, responsibilities, benefits, salary_text


def _normalize_lever_job(job: dict, company_name: str) -> dict:
    cats = job.get("categories", {})
    commitment = cats.get("commitment", "")
    employment_type = _map_commitment(commitment)
    location_raw = cats.get("location", "") or ""
    location_type, is_remote = _detect_location_type(location_raw)

    # Description + section extraction
    description_blocks = job.get("descriptionPlain", "") or ""
    lists = job.get("lists") or []
    requirements, responsibilities, benefits, salary_text = _extract_lever_sections(lists)

    # Build full description including all list sections
    lists_plain = job.get("listsPlain", "") or ""
    description = description_blocks
    if lists_plain:
        description = f"{description}\n\n{lists_plain}".strip()

    title = job.get("text", "")
    experience_level = _detect_experience_level(title, description[:500])

    # Salary — try compensation field, then salary text from lists, then description
    sal_min = sal_max = None
    sal_period = sal_raw = ""
    comp_text = salary_text or job.get("compensation") or job.get("salaryRange") or ""
    if not comp_text:
        # scan first 2000 chars of description for salary pattern
        comp_text = description[:2000]
    sal_min, sal_max, sal_period, sal_raw = _parse_lever_salary(str(comp_text))

    return {
        "external_id": job.get("id", ""),
        "original_url": job.get("hostedUrl", ""),
        "apply_url": job.get("applyUrl", "") or job.get("hostedUrl", ""),
        "title": title,
        "company_name": company_name,
        "department": cats.get("department", "") or "",
        "team": cats.get("team", "") or "",
        "location_raw": location_raw,
        "city": "",
        "state": "",
        "country": "",
        "is_remote": is_remote,
        "location_type": location_type,
        "employment_type": employment_type,
        "experience_level": experience_level,
        "salary_min": sal_min,
        "salary_max": sal_max,
        "salary_currency": "USD",
        "salary_period": sal_period,
        "salary_raw": sal_raw,
        "description": description,
        "requirements": requirements,
        "responsibilities": responsibilities,
        "benefits": benefits,
        "posted_date_raw": str(job.get("createdAt", "")),
        "closing_date": "",
        "raw_payload": job,
    }


class LeverHarvester(BaseHarvester):
    """Harvests jobs from Lever public REST API."""

    platform_slug = "lever"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        if not tenant_id:
            return []

        cutoff_ms = None
        if not fetch_all:
            cutoff_ms = int(
                (datetime.now(tz=timezone.utc) - timedelta(hours=since_hours)).timestamp()
                * 1000
            )

        base_url = BASE_URL.format(company=tenant_id)
        results = []
        offset = 0

        while True:
            params = {
                "mode": "json",
                "limit": PAGE_SIZE,
                "offset": offset,
            }
            data = self._get(base_url, params=params)

            if isinstance(data, dict) and "error" in data:
                break
            if not isinstance(data, list):
                break

            if not data:
                break

            self.last_total_available += len(data)

            for job in data:
                created_ms = job.get("createdAt", 0)
                # When not fetching all, skip jobs older than cutoff
                if cutoff_ms and created_ms and created_ms < cutoff_ms:
                    if not fetch_all:
                        continue

                results.append(_normalize_lever_job(job, company.name))

            # Lever returns up to `limit` items; if fewer, we're on the last page
            if len(data) < PAGE_SIZE:
                break

            if not fetch_all:
                break

            offset += PAGE_SIZE
            time.sleep(MIN_DELAY_API)

        return results
