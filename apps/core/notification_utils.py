"""Create in-app notifications and optional email (Phase 3)."""

from django.conf import settings
from django.core.mail import send_mail
from django.urls import reverse

from .models import Notification


def create_notification(
    user,
    *,
    kind: str,
    title: str,
    body: str = '',
    link: str = '',
):
    """Persist an in-app notification."""
    return Notification.objects.create(
        user=user,
        kind=kind,
        title=title[:200],
        body=body or '',
        link=link[:500] if link else '',
    )


def notify_submission_pipeline_event(
    submission,
    *,
    actor,
    old_status: str,
    new_status: str,
):
    """
    Notify the consultant when staff moves their application on the pipeline.
    Skips if the actor is the consultant themselves.
    """
    consultant_user = submission.consultant.user
    if actor and consultant_user.pk == actor.pk:
        return

    path = reverse('submission-detail', kwargs={'pk': submission.pk})
    title = f"Application updated: {submission.get_status_display()}"
    body = f"{submission.job.title} · {submission.job.company}"
    create_notification(
        consultant_user,
        kind=Notification.Kind.SUBMISSION,
        title=title,
        body=body,
        link=path,
    )

    from users.models import UserEmailNotificationPreferences

    prefs, _ = UserEmailNotificationPreferences.objects.get_or_create(user=consultant_user)
    if not prefs.email_submissions:
        return
    if not getattr(consultant_user, 'email', None):
        return
    try:
        send_mail(
            subject=title,
            message=f"{body}\n\nView: {settings_allowed_origin()}{path}",
            from_email=getattr(settings, 'DEFAULT_FROM_EMAIL', None) or 'noreply@localhost',
            recipient_list=[consultant_user.email],
            fail_silently=True,
        )
    except Exception:
        pass


def settings_allowed_origin():
    """Best-effort base URL for emails (no request context)."""
    return getattr(settings, 'SITE_URL', '') or ''
