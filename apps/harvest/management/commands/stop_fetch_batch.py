from celery import current_app
from django.core.management.base import BaseCommand
from django.utils import timezone

from harvest.models import CompanyFetchRun, FetchBatch


class Command(BaseCommand):
    help = "Stop a running/partial fetch batch and revoke active company fetch tasks."

    def add_arguments(self, parser):
        parser.add_argument("--batch-id", type=int, default=0, help="Batch id; 0 stops latest active batch.")

    def handle(self, *args, **options):
        batch_id = int(options["batch_id"] or 0)
        active_qs = FetchBatch.objects.filter(status__in=[
            FetchBatch.Status.RUNNING,
            FetchBatch.Status.PARTIAL,
            FetchBatch.Status.PENDING,
        ])
        batch = (
            FetchBatch.objects.filter(pk=batch_id).first()
            if batch_id
            else active_qs.order_by("-created_at").first()
        )

        if not batch:
            self.stdout.write(self.style.WARNING("No active fetch batch found."))
            return

        self.stdout.write(f"Target batch: #{batch.pk} status={batch.status}")
        now = timezone.now()
        task_ids = []

        active_runs = (
            CompanyFetchRun.objects
            .filter(batch=batch, status=CompanyFetchRun.Status.RUNNING)
            .exclude(task_id="")
            .exclude(task_id=None)
        )
        task_ids = list(active_runs.values_list("task_id", flat=True))

        batch.stop_requested = True
        batch.status = FetchBatch.Status.CANCELLED
        batch.completed_at = batch.completed_at or now
        batch.save(update_fields=["stop_requested", "status", "completed_at"])

        if batch.task_id:
            current_app.control.revoke(batch.task_id, terminate=True, signal="SIGTERM")
        if task_ids:
            current_app.control.revoke(task_ids, terminate=True, signal="SIGTERM")
            active_runs.update(status=CompanyFetchRun.Status.SKIPPED, completed_at=now)

        self.stdout.write(self.style.SUCCESS(f"Stopped batch #{batch.pk}; revoked {len(task_ids)} active task(s)."))
