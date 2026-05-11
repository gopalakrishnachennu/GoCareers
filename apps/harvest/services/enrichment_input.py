"""Canonical input builder for RawJob enrichment.

Different harvest paths naturally carry different shapes: platform dictionaries,
Jarvis/backfill dictionaries, and persisted RawJob objects.  The enrichment
engine should see the same key set from every caller so reprocessing does not
change classification simply because a later task passed fewer hints.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any


def _get(source: Any, key: str, default: Any = "") -> Any:
    if isinstance(source, dict):
        return source.get(key, default)
    return getattr(source, key, default)


def _first(source: Any, *keys: str, default: Any = "") -> Any:
    for key in keys:
        value = _get(source, key, None)
        if value not in (None, "", [], {}):
            return value
    return default


def _as_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _as_date_string(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")


def build_enrichment_input(
    source: Any,
    *,
    overrides: dict[str, Any] | None = None,
    company_name: str = "",
    posted_date: Any = None,
) -> dict[str, Any]:
    """Return the stable enrichment contract consumed by extract_enrichments()."""

    overrides = overrides or {}
    raw_payload = _first(overrides, "raw_payload", default=None)
    if raw_payload is None:
        raw_payload = _first(source, "raw_payload", default={})
    if not isinstance(raw_payload, dict):
        raw_payload = {}

    location_raw = _first(
        overrides,
        "location_raw",
        "location",
        default=_first(source, "location_raw", "location", default=""),
    )
    title = _first(overrides, "title", default=_first(source, "title", default=""))
    description = _first(
        overrides,
        "description",
        "description_text",
        default=_first(source, "description", "description_text", default=""),
    )
    posted = posted_date if posted_date is not None else _first(
        overrides,
        "posted_date",
        "posted_date_raw",
        default=_first(source, "posted_date", "posted_date_raw", default=""),
    )

    return {
        "title": title or "",
        "description": description or "",
        "description_clean": _first(
            overrides,
            "description_clean",
            default=_first(source, "description_clean", default=""),
        ),
        "description_raw_html": _first(
            overrides,
            "description_raw_html",
            default=_first(source, "description_raw_html", default=""),
        ),
        "has_html_content": bool(
            _first(overrides, "has_html_content", default=_first(source, "has_html_content", default=False))
        ),
        "cleaning_version": _first(
            overrides,
            "cleaning_version",
            default=_first(source, "cleaning_version", default=""),
        ),
        "requirements": _first(
            overrides,
            "requirements",
            "requirements_text",
            default=_first(source, "requirements", "requirements_text", default=""),
        ),
        "responsibilities": _first(
            overrides,
            "responsibilities",
            default=_first(source, "responsibilities", default=""),
        ),
        "benefits": _first(
            overrides,
            "benefits",
            "benefits_text",
            default=_first(source, "benefits", "benefits_text", default=""),
        ),
        "department": _first(
            overrides,
            "department",
            default=_first(source, "department", default=""),
        ),
        "location_raw": location_raw or "",
        "city": _first(overrides, "city", default=_first(source, "city", default="")),
        "state": _first(overrides, "state", default=_first(source, "state", default="")),
        "country": _first(overrides, "country", default=_first(source, "country", default="")),
        "country_codes": _as_list(_first(overrides, "country_codes", default=_first(source, "country_codes", default=[]))),
        "location_candidates": _as_list(_first(overrides, "location_candidates", default=_first(source, "location_candidates", default=[]))),
        "vendor_location_block": _first(
            overrides,
            "vendor_location_block",
            default=_first(source, "vendor_location_block", default=""),
        ),
        "employment_type": _first(
            overrides,
            "employment_type",
            "job_type",
            default=_first(source, "employment_type", "job_type", default=""),
        ),
        "experience_level": _first(
            overrides,
            "experience_level",
            default=_first(source, "experience_level", default=""),
        ),
        "salary_raw": _first(
            overrides,
            "salary_raw",
            default=_first(source, "salary_raw", default=""),
        ),
        "company_name": company_name or _first(
            overrides,
            "company_name",
            default=_first(source, "company_name", default=""),
        ),
        "posted_date": _as_date_string(posted),
        "vendor_degree_level": _first(
            overrides,
            "vendor_degree_level",
            default=_first(source, "vendor_degree_level", default=raw_payload.get("vendor_degree_level", "")),
        ),
        "vendor_job_schedule": _first(
            overrides,
            "vendor_job_schedule",
            default=_first(source, "vendor_job_schedule", default=raw_payload.get("vendor_job_schedule", "")),
        ),
        "vendor_job_shift": _first(
            overrides,
            "vendor_job_shift",
            default=_first(source, "vendor_job_shift", default=raw_payload.get("vendor_job_shift", "")),
        ),
        "vendor_job_category": _first(
            overrides,
            "vendor_job_category",
            default=_first(source, "vendor_job_category", default=raw_payload.get("vendor_job_category", "")),
        ),
        "vendor_job_identification": _first(
            overrides,
            "vendor_job_identification",
            default=_first(source, "vendor_job_identification", default=raw_payload.get("vendor_job_identification", "")),
        ),
        "raw_payload": raw_payload,
    }
