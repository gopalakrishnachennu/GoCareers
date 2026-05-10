import uuid

from django.conf import settings

from .audit_utils import (
    get_client_ip,
    log_audit_event,
    outcome_from_status,
    safe_post_summary,
    truncate_user_agent,
)
from .models import AuditLog

# Do not log mutating requests to these path prefixes (noise, static, health).
_AUDIT_SKIP_PATH_PREFIXES = (
    "/static/",
    "/media/",
    "/health/",
    "/favicon.ico",
    "/__reload__/",  # django-browser-reload (dev); avoids noisy duplicate audits
)


def _path_is_skipped(path: str) -> bool:
    if not path:
        return True
    p = path
    for prefix in _AUDIT_SKIP_PATH_PREFIXES:
        if p.startswith(prefix):
            return True
    static_url = getattr(settings, "STATIC_URL", "") or ""
    if static_url and static_url != "/" and p.startswith(static_url):
        return True
    extra = getattr(settings, "AUDIT_MIDDLEWARE_EXTRA_SKIP_PREFIXES", ())
    for prefix in extra:
        if prefix and p.startswith(str(prefix)):
            return True
    return False


def _resolver_audit_meta(request):
    view_name = ""
    url_name = ""
    event_code = "http.mutation"
    rm = getattr(request, "resolver_match", None)
    if not rm:
        return view_name, url_name, event_code
    if rm.func:
        view_name = getattr(rm.func, "__name__", "") or ""
    url_name = (rm.url_name or "")[:128]
    namespaces = list(rm.namespaces or [])
    if namespaces and url_name:
        ns = "_".join(namespaces)
        event_code = f"http.{ns}_{url_name}"[:128]
    elif url_name:
        event_code = f"http.{url_name.replace('-', '_')}"[:128]
    return view_name, url_name, event_code


class AuditMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if not getattr(request, "audit_correlation_id", None):
            request.audit_correlation_id = str(uuid.uuid4())

        response = self.get_response(request)

        if (
            request.user.is_authenticated
            and request.method in ("POST", "PUT", "PATCH", "DELETE")
            and not _path_is_skipped(request.path)
        ):
            self._log_mutation(request, response)

        return response

    def _log_mutation(self, request, response):
        try:
            status = getattr(response, "status_code", 0) or 0
            outcome = outcome_from_status(int(status))
            view_name, url_name, event_code = _resolver_audit_meta(request)

            resolver_match = request.resolver_match
            target_id = ""
            if resolver_match and resolver_match.kwargs:
                target_id = str(
                    resolver_match.kwargs.get("pk", "")
                    or resolver_match.kwargs.get("id", "")
                    or ""
                )

            post_summary = {}
            if request.method == "POST":
                post_summary = safe_post_summary(request)

            details = {
                "path": request.path,
                "method": request.method,
                "status_code": status,
                "query_params": dict(request.GET),
            }
            if post_summary:
                details["post_keys_summary"] = post_summary

            try:
                outcome_label = AuditLog.Outcome(outcome).label
            except ValueError:
                outcome_label = str(outcome)
            human = f"{request.method} {request.path} → HTTP {status} ({outcome_label})"

            log_audit_event(
                actor=request.user,
                action=f"{request.method} {request.path}"[:255],
                event_code=event_code,
                outcome=outcome,
                human_summary=human,
                target_model="",
                target_id=target_id[:100],
                details=details,
                request=request,
                ip_address=get_client_ip(request),
                view_name=view_name,
                url_name=url_name,
                user_agent=truncate_user_agent(request.META.get("HTTP_USER_AGENT", "")),
            )
        except Exception as e:
            # Never break the request because audit failed
            if settings.DEBUG:
                import logging

                logging.getLogger(__name__).warning("Audit middleware error: %s", e)
