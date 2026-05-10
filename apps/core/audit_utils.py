"""Audit logging helpers — structured events safe for Settings UI and AI-assisted debugging."""

from __future__ import annotations

import uuid
from typing import Any
from urllib.parse import urlencode

from django.http import HttpRequest
from django.http import QueryDict

from .models import AuditLog

# POST/query keys never copied into details (case-insensitive match + token patterns)
_SENSITIVE_POST_KEYS = frozenset(
    k.lower()
    for k in (
        "password",
        "password1",
        "password2",
        "old_password",
        "new_password1",
        "new_password2",
        "csrfmiddlewaretoken",
        "api_key",
        "token",
        "secret",
        "credit_card",
        "card_number",
        "cvv",
        "email_imap_encrypted_password",
        "encrypted_api_key",
    )
)
_SENSITIVE_KEY_FRAGMENTS = (
    "password",
    "passwd",
    "pwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "access_key",
    "private_key",
    "client_secret",
    "credential",
    "authorization",
    "auth",
    "signature",
    "signed",
    "csrf",
    "cvv",
    "card_number",
)


def is_sensitive_key(key: str | None) -> bool:
    """Return True for exact or compound secret-looking field names."""
    normalized = (key or "").strip().lower().replace("-", "_")
    if not normalized:
        return False
    if normalized in _SENSITIVE_POST_KEYS:
        return True
    return any(fragment in normalized for fragment in _SENSITIVE_KEY_FRAGMENTS)


def get_client_ip(request: HttpRequest) -> str | None:
    x_forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip() or None
    ip = request.META.get("REMOTE_ADDR")
    return ip or None


def get_request_correlation_id(request: HttpRequest | None) -> str:
    if request is None:
        return str(uuid.uuid4())
    cid = getattr(request, "audit_correlation_id", None)
    if cid:
        return str(cid)
    cid = str(uuid.uuid4())
    request.audit_correlation_id = cid
    return cid


def outcome_from_status(status_code: int) -> str:
    if 200 <= status_code < 400:
        return AuditLog.Outcome.SUCCESS
    if status_code in (401, 403, 419, 429):
        return AuditLog.Outcome.DENIED
    if 400 <= status_code < 500:
        return AuditLog.Outcome.FAILURE
    if status_code >= 500:
        return AuditLog.Outcome.FAILURE
    return AuditLog.Outcome.UNKNOWN


def _truncate_audit_value(s: str, max_len: int = 500) -> str:
    s = (s or "").strip().replace("\n", " ").replace("\r", "")
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def truncate_user_agent(ua: str | None, max_len: int = 500) -> str:
    if not ua:
        return ""
    ua = ua.strip()
    if len(ua) <= max_len:
        return ua
    return ua[: max_len - 1] + "…"


def safe_query_params(query_params: QueryDict | dict | None, *, max_keys: int = 40) -> dict[str, Any]:
    """Redacted snapshot of query params suitable for DB/file logs."""
    if not query_params:
        return {}
    out: dict[str, Any] = {}
    if hasattr(query_params, "lists"):
        items = list(query_params.lists())
    else:
        items = list(query_params.items())

    for i, item in enumerate(items):
        if i >= max_keys:
            out["_truncated"] = True
            break
        key, value = item
        if is_sensitive_key(str(key)):
            out[str(key)] = "[redacted]"
            continue
        values = value if isinstance(value, (list, tuple)) else [value]
        clean_values = [_truncate_audit_value(str(v), 200) for v in values]
        out[str(key)] = clean_values if len(clean_values) > 1 else (clean_values[0] if clean_values else "")
    return out


def safe_full_path(request: HttpRequest, *, max_len: int = 1000) -> str:
    """Request path with sensitive query parameter values redacted."""
    path = getattr(request, "path", "") or ""
    query = safe_query_params(getattr(request, "GET", None))
    if not query:
        return path[:max_len]
    parts = []
    for key, value in query.items():
        if key == "_truncated":
            continue
        values = value if isinstance(value, list) else [value]
        for v in values:
            parts.append((key, v))
    if query.get("_truncated"):
        parts.append(("_truncated", "True"))
    full_path = f"{path}?{urlencode(parts)}" if parts else path
    return full_path[:max_len]


def safe_post_summary(request: HttpRequest, *, max_keys: int = 40) -> dict[str, Any]:
    """
    Snapshot of POST + FILES keys (multipart). File fields show name/size only, never content.
    Sensitive keys are redacted; scalar values truncated.
    """
    if not hasattr(request, "POST"):
        return {}
    out: dict[str, Any] = {}
    seen: set[str] = set()
    keys: list[str] = []
    for k in request.POST.keys():
        if k not in seen:
            seen.add(k)
            keys.append(k)
    if hasattr(request, "FILES"):
        for k in request.FILES.keys():
            if k not in seen:
                seen.add(k)
                keys.append(k)

    for i, key in enumerate(keys):
        if i >= max_keys:
            out["_truncated"] = True
            break
        if is_sensitive_key(key):
            out[key] = "[redacted]"
            continue
        upload = request.FILES.get(key) if hasattr(request, "FILES") else None
        if upload is not None:
            try:
                name = (getattr(upload, "name", None) or "")[:200]
                size = getattr(upload, "size", None)
                if size is not None:
                    out[key] = f"[file: {name or 'upload'}, {int(size)} bytes]"
                else:
                    out[key] = f"[file: {name or 'upload'}]"
            except Exception:
                out[key] = "[file]"
            continue
        val = request.POST.get(key, "")
        out[key] = _truncate_audit_value(str(val), 500)
    return out


def log_audit_event(
    *,
    actor,
    action: str,
    event_code: str = "",
    outcome: str | None = None,
    human_summary: str = "",
    target_model: str = "",
    target_id: str = "",
    details: dict | None = None,
    request: HttpRequest | None = None,
    ip_address: str | None = None,
    correlation_id: str = "",
    view_name: str = "",
    url_name: str = "",
    user_agent: str = "",
) -> AuditLog:
    """
    Create one AuditLog row. Pass request to inherit correlation_id and IP when set by middleware.
    """
    cid = correlation_id or (get_request_correlation_id(request) if request else str(uuid.uuid4()))
    ip = ip_address
    if ip is None and request is not None:
        ip = get_client_ip(request)
    ua = user_agent
    if not ua and request is not None:
        ua = truncate_user_agent(request.META.get("HTTP_USER_AGENT", ""))

    if outcome is None:
        outcome = AuditLog.Outcome.UNKNOWN

    return AuditLog.objects.create(
        actor=actor,
        action=action[:255],
        event_code=(event_code or "")[:128],
        outcome=outcome,
        human_summary=human_summary or "",
        correlation_id=cid[:36],
        view_name=(view_name or "")[:255],
        url_name=(url_name or "")[:128],
        user_agent=(ua or "")[:512],
        target_model=(target_model or "")[:100],
        target_id=(target_id or "")[:100],
        details=details or {},
        ip_address=ip,
    )


def log_field_changes(actor, instance, old_values, new_values, ip_address=None, request=None):
    """
    Compare old_values dict to new_values dict and log each changed field.
    """
    model_name = instance.__class__.__name__
    target_id = str(instance.pk)
    changes = []

    for field, old_val in old_values.items():
        new_val = new_values.get(field)
        if str(old_val) != str(new_val):
            changes.append(
                {
                    "field": field,
                    "old": str(old_val),
                    "new": str(new_val),
                }
            )

    if changes:
        # Avoid huge JSON rows (e.g. pasted job descriptions).
        safe_changes = []
        for c in changes:
            if is_sensitive_key(c["field"]):
                old = new = "[redacted]"
            else:
                old = _truncate_audit_value(c["old"], 500)
                new = _truncate_audit_value(c["new"], 500)
            safe_changes.append({"field": c["field"], "old": old, "new": new})
        labels = ", ".join(f"{c['field']}" for c in safe_changes[:5])
        if len(safe_changes) > 5:
            labels += ", …"
        summary = f"Updated {model_name} #{target_id}: {labels}"
        log_audit_event(
            actor=actor,
            action="field_change",
            event_code="model.field_change",
            outcome=AuditLog.Outcome.SUCCESS,
            human_summary=summary,
            target_model=model_name,
            target_id=target_id,
            details={"changes": safe_changes},
            request=request,
            ip_address=ip_address,
        )
    return changes
