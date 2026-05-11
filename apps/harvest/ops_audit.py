"""Structured audit rows + grep-friendly logs for non-batch harvest/jobs ops."""

from __future__ import annotations

import logging
import time

from django.contrib.auth import get_user_model
from django.utils import timezone

from .models import HarvestOpsRun

logger = logging.getLogger(__name__)

# Monotonic timestamps for tick throttling (per ops_run pk) — process-local only.
# Sufficient: tick writes are best-effort; we only need one write per 3 s per run.
_TICK_LAST: dict[int, float] = {}
_TICK_MIN_INTERVAL = 3.0  # seconds between DB writes per run


def tick_ops_run_progress(
    run: HarvestOpsRun | None,
    current: int,
    total: int,
    message: str = "",
    *,
    force: bool = False,
) -> None:
    """
    Write DB-backed progress to ``HarvestOpsRun`` so the Live Ops panel
    can display accurate progress that survives page refresh.

    Throttled to one DB UPDATE per run per ``_TICK_MIN_INTERVAL`` seconds to
    avoid hammering the DB inside tight loops. Pass ``force=True`` at the
    start and end of a task to always write those bookend states.
    """
    if run is None:
        return
    now = time.monotonic()
    last = _TICK_LAST.get(run.pk, 0.0)
    if not force and (now - last) < _TICK_MIN_INTERVAL:
        return
    _TICK_LAST[run.pk] = now
    try:
        HarvestOpsRun.objects.filter(pk=run.pk).update(
            progress_current=max(0, int(current)),
            progress_total=max(0, int(total)),
            progress_message=(message or "")[:256],
        )
        # Keep local copy consistent so callers can read run.progress_* directly.
        run.progress_current = max(0, int(current))
        run.progress_total = max(0, int(total))
        run.progress_message = (message or "")[:256]
    except Exception:
        logger.debug("tick_ops_run_progress: DB write skipped (run=%s)", run.pk)


def begin_ops_run(
    operation: str,
    celery_task_id: str,
    *,
    user_id: int | None = None,
    queue: dict | None = None,
    progress_total: int = 0,
) -> HarvestOpsRun:
    user = None
    if user_id:
        User = get_user_model()
        user = User.objects.filter(pk=user_id).first()
    payload = {"queue": queue or {}}
    run = HarvestOpsRun.objects.create(
        operation=operation,
        celery_task_id=(celery_task_id or "")[:128],
        status=HarvestOpsRun.Status.RUNNING,
        audit_payload=payload,
        triggered_by_user=user,
        progress_total=max(0, int(progress_total)),
        progress_current=0,
        progress_message="Starting…",
    )
    logger.info(
        "[HARVEST_AUDIT ops_queue] operation=%s ops_run_id=%s celery_id=%s queue=%s",
        operation,
        run.pk,
        celery_task_id or "",
        payload["queue"],
    )
    return run


def finish_ops_run(
    run: HarvestOpsRun | None,
    status: str,
    completion: dict | None = None,
) -> None:
    if run is None:
        return
    merged = dict(run.audit_payload or {})
    merged["completion"] = completion or {}
    run.audit_payload = merged
    run.status = status
    run.finished_at = timezone.now()
    # Snap progress to final state so UI shows 100% / 0% correctly on refresh
    total = run.progress_total or 0
    if status == HarvestOpsRun.Status.SUCCESS and total:
        run.progress_current = total
    elif status in (HarvestOpsRun.Status.FAILED, HarvestOpsRun.Status.SKIPPED):
        pass  # leave as-is (partial progress visible)
    run.save(update_fields=["audit_payload", "status", "finished_at", "progress_current"])
    logger.info(
        "[HARVEST_AUDIT ops_done] operation=%s ops_run_id=%s status=%s celery_id=%s completion=%s",
        run.operation,
        run.pk,
        status,
        run.celery_task_id or "",
        merged.get("completion"),
    )
