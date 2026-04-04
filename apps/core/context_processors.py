"""Platform singleton for templates + unread notification count for nav bell."""


def platform_settings(request):
    """Expose singleton PlatformConfig as PLATFORM_CONFIG (base.html, etc.)."""
    from core.models import PlatformConfig

    return {'PLATFORM_CONFIG': PlatformConfig.load()}


def unread_notifications_count(request):
    if not request.user.is_authenticated:
        return {'unread_notification_count': 0}
    from core.models import Notification

    n = Notification.objects.filter(user=request.user, read_at__isnull=True).count()
    return {'unread_notification_count': n}
