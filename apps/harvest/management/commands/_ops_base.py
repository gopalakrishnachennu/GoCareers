"""
OpsTrackedCommand — BaseCommand subclass that auto-creates a HarvestOpsRun
row for every execution so progress is visible in the ops center.

Usage:
    class Command(OpsTrackedCommand):
        ops_operation = HarvestOpsRun.Operation.CLASSIFY_DOMAINS

        def handle(self, *args, **options):
            self.ops_start(total=1000)              # call once at the top
            for i, item in enumerate(items):
                ...
                self.ops_progress(i + 1, message="Classifying…")
            self.ops_finish(audit_payload={...})    # optional explicit finish
"""
from __future__ import annotations

import traceback
from typing import Any

from django.core.management.base import BaseCommand
from django.utils import timezone


class OpsTrackedCommand(BaseCommand):
    """
    Wraps execute() to bracket the command in a HarvestOpsRun row.
    Subclasses must set `ops_operation` to a HarvestOpsRun.Operation value.
    """

    ops_operation: str = "classify"  # override in subclass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ops_run = None
        self._ops_finished = False

    # ── Public API for subclass commands ─────────────────────────────────────

    def ops_start(self, total: int = 0, message: str = "Starting…") -> None:
        """Call once at the beginning of handle() after you know total row count."""
        if self._ops_run is None:
            return
        from harvest.models import HarvestOpsRun
        HarvestOpsRun.objects.filter(pk=self._ops_run.pk).update(
            progress_total=total,
            progress_current=0,
            progress_message=message[:256],
        )
        self._ops_run.progress_total = total

    def ops_progress(self, current: int, *, message: str = "") -> None:
        """Call in your processing loop. Writes to DB every ~2% or every 500 rows."""
        if self._ops_run is None:
            return
        total = self._ops_run.progress_total or 1
        # Throttle: only write every 2% increment or every 500 rows
        last = getattr(self._ops_run, "_last_progress_pct", -1)
        pct = int(100 * current / total)
        if pct - last < 2 and current % 500 != 0:
            return
        self._ops_run._last_progress_pct = pct
        from harvest.models import HarvestOpsRun
        HarvestOpsRun.objects.filter(pk=self._ops_run.pk).update(
            progress_current=current,
            progress_message=(message or f"{current:,} / {total:,}")[:256],
        )

    def ops_finish(self, *, audit_payload: dict | None = None, status: str = "SUCCESS") -> None:
        """Call explicitly if you want to set a custom status or audit payload."""
        self._finish_run(status=status, audit_payload=audit_payload or {})

    # ── Internals ─────────────────────────────────────────────────────────────

    def _create_run(self, options: dict) -> None:
        try:
            from harvest.models import HarvestOpsRun
            self._ops_run = HarvestOpsRun.objects.create(
                operation=self.ops_operation,
                status=HarvestOpsRun.Status.RUNNING,
                audit_payload={
                    "options": {
                        k: v for k, v in options.items()
                        if k not in ("verbosity", "settings", "pythonpath", "traceback", "no_color", "force_color", "skip_checks")
                        and isinstance(v, (str, int, float, bool, type(None)))
                    }
                },
            )
        except Exception:
            self._ops_run = None

    def _finish_run(self, *, status: str = "SUCCESS", audit_payload: dict | None = None) -> None:
        if self._ops_run is None or self._ops_finished:
            return
        self._ops_finished = True
        try:
            from harvest.models import HarvestOpsRun
            total = getattr(self._ops_run, "progress_total", 0)
            HarvestOpsRun.objects.filter(pk=self._ops_run.pk).update(
                status=status,
                finished_at=timezone.now(),
                progress_current=total,
                progress_message="Done" if status == "SUCCESS" else status,
                audit_payload={**(self._ops_run.audit_payload or {}), **(audit_payload or {})},
            )
        except Exception:
            pass

    def execute(self, *args, **kwargs) -> Any:
        self._create_run(kwargs)
        try:
            result = super().execute(*args, **kwargs)
            if not self._ops_finished:
                self._finish_run(status="SUCCESS")
            return result
        except SystemExit as exc:
            if not self._ops_finished:
                code = exc.code
                self._finish_run(status="SUCCESS" if code in (None, 0) else "FAILED")
            raise
        except Exception as exc:
            if not self._ops_finished:
                self._finish_run(
                    status="FAILED",
                    audit_payload={"error": str(exc)[:500], "traceback": traceback.format_exc()[-1000:]},
                )
            raise
