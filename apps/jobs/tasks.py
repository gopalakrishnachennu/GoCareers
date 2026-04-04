from celery import shared_task
from urllib.request import Request, urlopen
from urllib.parse import urlparse
import ssl

from django.utils import timezone

from .models import Job


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not url:
        return ""
    parsed = urlparse(url)
    if not parsed.scheme:
        url = "https://" + url
    return url


def _check_job_url(url: str) -> bool:
    if not url:
        return False
    url = _normalize_url(url)
    try:
        ctx = ssl.create_default_context()
        req = Request(url, headers={"User-Agent": "GoCareers-job-url-checker/1.0"})
        req.get_method = lambda: "HEAD"
        try:
            resp = urlopen(req, context=ctx, timeout=5)
        except Exception:
            # Fallback to GET
            req.get_method = lambda: "GET"
            resp = urlopen(req, context=ctx, timeout=5)
        status = getattr(resp, "status", None) or getattr(resp, "code", None)
        if status is None:
            return True
        status = int(status)
        # Consider 2xx and 3xx as live; 4xx (especially 404/410) and 5xx as not live.
        return 200 <= status < 400
    except Exception:
        return False


@shared_task
def validate_job_urls_task(batch_size: int = 50):
    """
    Re-check original job URLs and flag jobs as 'possibly_filled' when their source goes away.
    Runs daily via Celery beat (see core.signals).
    """
    now = timezone.now()
    cutoff = now - timezone.timedelta(hours=24)

    qs = Job.objects.filter(status=Job.Status.OPEN)
    qs = qs.filter(original_link__isnull=False).exclude(original_link="")
    qs = qs.filter(
        original_link_last_checked_at__lt=cutoff
    ) | qs.filter(
        original_link_last_checked_at__isnull=True
    )

    processed = 0
    for job in qs[:batch_size]:
        is_live = _check_job_url(job.original_link)
        job.original_link_is_live = is_live
        job.original_link_last_checked_at = now
        # If URL is not live and job is still marked OPEN, flag as possibly filled.
        job.possibly_filled = not is_live and job.status == Job.Status.OPEN
        job.save(update_fields=["original_link_is_live", "original_link_last_checked_at", "possibly_filled"])
        processed += 1

    result = {"processed": processed}
    try:
        from core.models import PipelineRunLog
        PipelineRunLog.objects.update_or_create(
            task_name="validate_job_urls",
            defaults={"last_run_at": timezone.now(), "last_run_result": result},
        )
    except Exception:
        pass
    return result


@shared_task
def auto_close_jobs_task():
    """
    Close stale OPEN jobs per PlatformConfig:
    - Optional: age in days (job_auto_close_after_days)
    - Optional: dead original link (job_auto_close_when_link_dead)
    """
    from core.models import PlatformConfig, PipelineRunLog

    config = PlatformConfig.load()
    now = timezone.now()
    closed_age = 0
    closed_dead = 0

    days = getattr(config, "job_auto_close_after_days", None)
    if days and days > 0:
        cutoff = now - timezone.timedelta(days=days)
        qs = Job.objects.filter(status=Job.Status.OPEN, created_at__lt=cutoff)
        for job in qs.iterator():
            job.status = Job.Status.CLOSED
            job.save(update_fields=["status", "updated_at"])
            closed_age += 1

    if getattr(config, "job_auto_close_when_link_dead", False):
        qs = Job.objects.filter(
            status=Job.Status.OPEN,
            original_link_is_live=False,
        )
        for job in qs.iterator():
            job.status = Job.Status.CLOSED
            job.save(update_fields=["status", "updated_at"])
            closed_dead += 1

    result = {"closed_stale_days": closed_age, "closed_dead_link": closed_dead}
    try:
        PipelineRunLog.objects.update_or_create(
            task_name="auto_close_jobs",
            defaults={"last_run_at": timezone.now(), "last_run_result": result},
        )
    except Exception:
        pass
    return result

