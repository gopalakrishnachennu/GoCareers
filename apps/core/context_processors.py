"""Platform singleton for templates + shared nav/footer context."""

from functools import lru_cache
import os
import subprocess

from django.conf import settings

from core.notification_utils import get_cached_unread_count
from core.feature_flags import feature_enabled_for


def platform_settings(request):
    """Expose singleton PlatformConfig as PLATFORM_CONFIG (base.html, etc.)."""
    from core.models import PlatformConfig

    return {'PLATFORM_CONFIG': PlatformConfig.load()}


@lru_cache(maxsize=1)
def _deployment_metadata() -> dict[str, str]:
    """Return immutable deploy metadata for this running process."""
    base_dir = str(getattr(settings, "BASE_DIR", os.getcwd()))

    def _git(*args: str) -> str:
        try:
            return subprocess.check_output(
                ["git", *args],
                cwd=base_dir,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=1.5,
            ).strip()
        except Exception:
            return ""

    sha = (
        os.environ.get("DEPLOY_SHA")
        or os.environ.get("GITHUB_SHA")
        or os.environ.get("SOURCE_COMMIT")
        or _git("rev-parse", "HEAD")
    )
    short_sha = sha[:7] if sha else ""
    subject = os.environ.get("DEPLOY_COMMIT_MESSAGE") or _git("log", "-1", "--format=%s")
    committed_at = os.environ.get("DEPLOY_COMMITTED_AT") or _git("log", "-1", "--format=%cI")
    branch = os.environ.get("DEPLOY_BRANCH") or _git("rev-parse", "--abbrev-ref", "HEAD")

    commit_url = ""
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if repo and sha:
        commit_url = f"https://github.com/{repo}/commit/{sha}"

    return {
        "sha": sha,
        "short_sha": short_sha,
        "subject": subject,
        "committed_at": committed_at,
        "branch": branch,
        "commit_url": commit_url,
    }


def deployment_info(request):
    """Expose deployment commit only to superusers."""
    user = getattr(request, "user", None)
    if not getattr(user, "is_authenticated", False) or not getattr(user, "is_superuser", False):
        return {"DEPLOYMENT_INFO": None}
    return {"DEPLOYMENT_INFO": _deployment_metadata()}


def unread_notifications_count(request):
    if not request.user.is_authenticated:
        return {'unread_notification_count': 0}
    n = get_cached_unread_count(request.user.pk)
    return {'unread_notification_count': n}


def pending_pool_count(request):
    """
    Inject pending_pool_count for admin/employee nav badge.
    Only runs the DB query for logged-in staff users.
    """
    if not request.user.is_authenticated:
        return {'pending_pool_count': 0}
    role = getattr(request.user, 'role', None)
    if not (request.user.is_superuser or role in ('ADMIN', 'EMPLOYEE')):
        return {'pending_pool_count': 0}
    try:
        from jobs.models import Job
        count = Job.objects.filter(status=Job.Status.POOL, is_archived=False).count()
    except Exception:
        count = 0
    return {'pending_pool_count': count}


def dup_pending_count(request):
    """Inject pending duplicate pair count for the subnav badge."""
    if not request.user.is_authenticated:
        return {'dup_pending_count': 0}
    if not (request.user.is_superuser or getattr(request.user, 'role', None) in ('ADMIN', 'EMPLOYEE')):
        return {'dup_pending_count': 0}
    try:
        from harvest.models import RawJobDuplicatePair, DuplicateResolution
        count = RawJobDuplicatePair.objects.filter(resolution=DuplicateResolution.PENDING).count()
    except Exception:
        count = 0
    return {'dup_pending_count': count}


def user_feature_flags(request):
    """
    Inject USER_FEATURE_FLAGS: dict key -> bool for the current user (for nav / dashboards).
    """
    if not request.user.is_authenticated:
        return {'USER_FEATURE_FLAGS': {}}
    try:
        from core.models import FeatureFlag
    except Exception:
        return {'USER_FEATURE_FLAGS': {}}
    keys = FeatureFlag.objects.values_list('key', flat=True)
    return {
        'USER_FEATURE_FLAGS': {k: feature_enabled_for(request.user, k) for k in keys},
    }
