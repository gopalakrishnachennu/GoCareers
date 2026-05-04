"""Structured audit rows + grep-friendly logs for non-batch harvest/jobs ops."""

from __future__ import annotations

import logging

from django.contrib.auth import get_user_model
from django.utils import timezone

from .models import HarvestOpsRun

logger = logging.getLogger(__name__)


def begin_ops_run(
    operation: str,
    celery_task_id: str,
    *,
    user_id: int | None = None,
    queue: dict | None = None,
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
    run.save(update_fields=["audit_payload", "status", "finished_at"])
    logger.info(
        "[HARVEST_AUDIT ops_done] operation=%s ops_run_id=%s status=%s celery_id=%s completion=%s",
        run.operation,
        run.pk,
        status,
        run.celery_task_id or "",
        merged.get("completion"),
    )
