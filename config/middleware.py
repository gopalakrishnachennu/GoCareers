"""
==========================================================
ADVANCED REQUEST LOGGING MIDDLEWARE
==========================================================
Logs every HTTP request with timing, user info, and status codes.
Automatically flags slow requests (>2s) and server errors (5xx).

Output → logs/requests.log + console
"""

import time
import logging

logger = logging.getLogger('middleware')


class RequestLoggingMiddleware:
    """Log every request: method, path, user, status, duration."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        start_time = time.time()

        # Capture request info
        user = getattr(request, 'user', None)
        username = user.username if user and user.is_authenticated else 'anonymous'
        method = request.method
        path = request.get_full_path()

        response = self.get_response(request)

        # Calculate duration
        duration_ms = (time.time() - start_time) * 1000
        status = response.status_code

        # Build log message
        msg = f"{method} {path} | user={username} | status={status} | {duration_ms:.0f}ms"

        # Log at appropriate level
        if status >= 500:
            logger.error(f"🔴 SERVER ERROR: {msg}")
        elif status >= 400:
            logger.warning(f"⚠️  CLIENT ERROR: {msg}")
        elif duration_ms > 2000:
            logger.warning(f"🐌 SLOW REQUEST: {msg}")
        else:
            logger.info(f"✅ {msg}")

        return response

    def process_exception(self, request, exception):
        """Log unhandled exceptions with full context."""
        user = getattr(request, 'user', None)
        username = user.username if user and user.is_authenticated else 'anonymous'
        logger.critical(
            f"💥 UNHANDLED EXCEPTION: {request.method} {request.get_full_path()} "
            f"| user={username} | error={type(exception).__name__}: {exception}",
            exc_info=True
        )
        return None  # Let Django handle it


class ImpersonateMiddleware:
    """
    Allow admin/superuser to impersonate another user.
    When active, request.user is swapped to the impersonated user.
    The real admin user is stored in request.real_user.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        impersonated_id = request.session.get('_impersonate_user_id')
        request.is_impersonating = False
        request.real_user = request.user

        if impersonated_id and request.user.is_authenticated:
            from django.contrib.auth import get_user_model
            User = get_user_model()
            try:
                target = User.objects.get(pk=impersonated_id)
                # Only allow admin/superuser to impersonate
                if request.user.is_superuser or request.user.role == 'ADMIN':
                    request.real_user = request.user
                    request.user = target
                    request.is_impersonating = True
            except User.DoesNotExist:
                # Target user was deleted, clear session
                del request.session['_impersonate_user_id']

        response = self.get_response(request)
        return response
