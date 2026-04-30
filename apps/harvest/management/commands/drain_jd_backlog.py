"""
drain_jd_backlog — aggressively drain the missing-JD backlog.

Usage:
  python manage.py drain_jd_backlog [--platform greenhouse] [--workers 8] [--batch 500]
  python manage.py drain_jd_backlog --reset-locks   # clear stale future cooldown locks

The command fires the backfill_descriptions task synchronously (in-process)
at maximum parallelism so the backlog drains as fast as possible without waiting
for the hourly Beat trigger.

Use --reset-locks to clear old-style future locks so jobs that were given a
12-hour cooldown by a previous run can be retried immediately.
"""
from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Aggressively drain the missing-JD backlog (runs backfill in-process)."

    def add_arguments(self, parser):
        parser.add_argument("--platform", default=None, help="Limit to a specific platform slug")
        parser.add_argument("--workers", type=int, default=8, help="Parallel fetch workers (default 8)")
        parser.add_argument("--batch", type=int, default=500, help="Rows per chunk (default 500)")
        parser.add_argument(
            "--reset-locks",
            action="store_true",
            default=False,
            help="Clear future jd_backfill_locked_at timestamps so cooled-down jobs become eligible immediately",
        )
        parser.add_argument(
            "--reset-failed",
            action="store_true",
            default=False,
            help="Also reset description=' ' back to '' so previously-failed jobs are retried",
        )

    def handle(self, *args, **options):
        from django.utils import timezone

        from harvest.models import RawJob
        from harvest.tasks import _backfill_eligible_queryset

        platform = options["platform"]
        workers = max(1, min(options["workers"], 8))
        batch = max(10, min(options["batch"], 1000))

        if options["reset_locks"]:
            # Clear all future locks (cooldown locks set by new backfill engine)
            qs = RawJob.objects.filter(jd_backfill_locked_at__gt=timezone.now())
            count = qs.count()
            qs.update(jd_backfill_locked_at=None)
            self.stdout.write(self.style.SUCCESS(f"Cleared {count} future cooldown locks."))

        if options["reset_failed"]:
            # Reset description=' ' back to '' so previously-failed jobs are re-queued
            from django.db.models.functions import Length, Trim
            from django.db.models import F, Value
            from django.db.models.functions import Coalesce
            qs = RawJob.objects.filter(has_description=False).extra(
                where=["TRIM(description) = ''"
                       " OR description = ' '"]
            )
            count = qs.count()
            qs.update(description="")
            self.stdout.write(self.style.SUCCESS(f"Reset {count} failed jobs back to empty description."))

        eligible = _backfill_eligible_queryset(platform).count()
        self.stdout.write(f"Eligible for backfill: {eligible} jobs (platform={platform or 'all'})")

        if eligible == 0:
            self.stdout.write(self.style.SUCCESS("Nothing to do!"))
            return

        self.stdout.write(f"Starting backfill: {workers} workers × {batch} batch...")

        from harvest.tasks import backfill_descriptions_task
        result = backfill_descriptions_task.apply(
            kwargs={
                "batch_size": batch,
                "parallel_workers": workers,
                "platform_slug": platform,
            }
        )
        r = result.result if hasattr(result, "result") else result
        self.stdout.write(self.style.SUCCESS(f"Done: {r}"))
