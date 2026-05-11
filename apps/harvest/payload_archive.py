"""Immutable source-payload capture for RawJob evidence and future AI review."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from django.db import IntegrityError

from .models import RawJob, RawJobPayloadSnapshot


SENSITIVE_KEY_RE = re.compile(
    r"(api[_-]?key|authorization|bearer|cookie|csrf|password|secret|signature|signed|token|session)",
    re.IGNORECASE,
)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"(?<!\w)(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}(?!\w)")
MAX_STORED_STRING = 100_000
MAX_FAILURE_BODY = 8_000


def _redact_url(value: str) -> str:
    try:
        parts = urlsplit(value)
    except ValueError:
        return value
    if not parts.scheme or not parts.netloc or not parts.query:
        return value
    clean_qs = []
    changed = False
    for key, val in parse_qsl(parts.query, keep_blank_values=True):
        if SENSITIVE_KEY_RE.search(key):
            clean_qs.append((key, "[REDACTED]"))
            changed = True
        else:
            clean_qs.append((key, val))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(clean_qs), parts.fragment)) if changed else value


def _redact_scalar(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = _redact_url(value)
    text = EMAIL_RE.sub("[REDACTED_EMAIL]", text)
    text = PHONE_RE.sub("[REDACTED_PHONE]", text)
    if len(text) > MAX_STORED_STRING:
        return f"{text[:MAX_STORED_STRING]}...[TRUNCATED {len(text) - MAX_STORED_STRING} chars]"
    return text


def sanitize_payload(value: Any, *, _depth: int = 0) -> Any:
    """Return a JSON-safe, redacted copy of a vendor payload."""
    if _depth > 30:
        return "[TRUNCATED_DEPTH]"
    if isinstance(value, Mapping):
        clean = {}
        for key, item in value.items():
            key_text = str(key)
            if SENSITIVE_KEY_RE.search(key_text):
                clean[key_text] = "[REDACTED]"
            else:
                clean[key_text] = sanitize_payload(item, _depth=_depth + 1)
        return clean
    if isinstance(value, (list, tuple)):
        return [sanitize_payload(item, _depth=_depth + 1) for item in value[:2000]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return _redact_scalar(value)
    return _redact_scalar(str(value))


def canonical_payload_bytes(payload: Any, raw_html: str = "") -> bytes:
    envelope = {
        "payload": sanitize_payload(payload or {}),
        "raw_html": _redact_scalar(raw_html or ""),
    }
    return json.dumps(envelope, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def capture_rawjob_payload_snapshot(
    raw_job: RawJob,
    *,
    payload: Any | None = None,
    raw_html: str = "",
    payload_kind: str = RawJobPayloadSnapshot.PayloadKind.API_RESPONSE,
    source_url: str = "",
    fetch_batch=None,
    platform_slug: str = "",
    schema_version: str = "source-v1",
    source_metadata: dict | None = None,
    is_failure: bool = False,
    http_status: int | None = None,
) -> RawJobPayloadSnapshot | None:
    """Persist immutable source evidence if this exact job/kind/content is new."""
    payload = payload or {}
    raw_html = raw_html or ""
    if is_failure and raw_html and len(raw_html) > MAX_FAILURE_BODY:
        raw_html = raw_html[:MAX_FAILURE_BODY] + "...[TRUNCATED_FAILURE_BODY]"
    if not payload and not raw_html:
        return None

    clean_payload = sanitize_payload(payload)
    clean_metadata = sanitize_payload(source_metadata or {})
    clean_html = _redact_scalar(raw_html)
    canonical = canonical_payload_bytes(clean_payload, clean_html)
    content_hash = hashlib.sha256(canonical).hexdigest()
    html_bytes = clean_html.encode("utf-8") if clean_html else b""
    payload_bytes = json.dumps(clean_payload, sort_keys=True, default=str).encode("utf-8")

    try:
        snapshot, _ = RawJobPayloadSnapshot.objects.get_or_create(
            raw_job=raw_job,
            payload_kind=payload_kind,
            content_hash=content_hash,
            defaults={
                "fetch_batch": fetch_batch,
                "platform_slug": (platform_slug or raw_job.platform_slug or "")[:64],
                "source_url": (source_url or raw_job.original_url or "")[:1024],
                "schema_version": schema_version[:24],
                "payload": clean_payload if isinstance(clean_payload, dict) else {"value": clean_payload},
                "raw_html_gzip": gzip.compress(html_bytes) if html_bytes else None,
                "payload_size_bytes": len(payload_bytes),
                "raw_html_size_bytes": len(html_bytes),
                "source_metadata": clean_metadata if isinstance(clean_metadata, dict) else {"value": clean_metadata},
                "is_failure": bool(is_failure),
                "http_status": http_status,
            },
        )
        return snapshot
    except IntegrityError:
        return RawJobPayloadSnapshot.objects.filter(
            raw_job=raw_job,
            payload_kind=payload_kind,
            content_hash=content_hash,
        ).first()
