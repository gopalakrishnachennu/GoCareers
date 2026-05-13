import time
from collections import Counter

from django.core.management.base import BaseCommand, CommandError

from harvest.models import FetchBatch, HarvestEngineConfig, HarvestSkippedTitle, RawJob
from harvest.tasks import fetch_raw_jobs_batch_task


class Command(BaseCommand):
    help = "Run a tightly bounded production smoke test for the selective harvest engine."

    def add_arguments(self, parser):
        parser.add_argument("--platform", required=True, help="Single platform slug to test.")
        parser.add_argument("--companies", type=int, default=1, help="Companies to sample; max 2.")
        parser.add_argument("--max-jobs", type=int, default=10, help="Max list jobs per company; max 25.")
        parser.add_argument("--audit-mode", action="store_true", help="Classify only; do not skip JD fetches.")
        parser.add_argument("--polls", type=int, default=24, help="Polling attempts before auto-stop.")
        parser.add_argument("--poll-interval", type=int, default=30, help="Seconds between polling attempts.")

    def handle(self, *args, **options):
        platform = (options["platform"] or "").strip()
        companies = options["companies"]
        max_jobs = options["max_jobs"]
        polls = options["polls"]
        poll_interval = options["poll_interval"]

        if not platform:
            raise CommandError("Strict smoke guard: --platform is required; smoke cannot run all platforms.")
        if companies < 1 or companies > 2:
            raise CommandError("Strict smoke guard: --companies must be 1 or 2.")
        if max_jobs < 1 or max_jobs > 25:
            raise CommandError("Strict smoke guard: --max-jobs must be between 1 and 25.")
        if polls < 1 or polls > 30:
            raise CommandError("Strict smoke guard: --polls must be between 1 and 30.")
        if poll_interval < 5 or poll_interval > 60:
            raise CommandError("Strict smoke guard: --poll-interval must be between 5 and 60 seconds.")

        cfg = HarvestEngineConfig.get()
        cfg.selective_filter_enabled = True
        cfg.filter_audit_mode = bool(options["audit_mode"])
        cfg.worker_concurrency = 2
        cfg.task_rate_limit = 3
        cfg.api_stagger_ms = 1000
        cfg.scraper_stagger_ms = 5000
        cfg.backfill_jd_workers = 1
        cfg.auto_backfill_jd = False
        cfg.auto_sync_to_pool = False
        cfg.save()

        self.stdout.write(
            "Selective smoke config applied "
            f"selective={cfg.selective_filter_enabled} "
            f"audit={cfg.filter_audit_mode} "
            f"task_rate_limit={cfg.task_rate_limit} "
            f"worker_concurrency={cfg.worker_concurrency}"
        )

        task = fetch_raw_jobs_batch_task.delay(
            platform_slug=platform,
            batch_name="Selective Harvest Smoke",
            test_mode=True,
            test_max_jobs=max_jobs,
            companies_per_platform=companies,
            fetch_all=False,
            run_kind="platform_smoke",
        )
        self.stdout.write(f"Queued selective harvest smoke task: {task.id}")

        batch = None
        status = "MISSING"
        for _ in range(polls):
            batch = (
                FetchBatch.objects.filter(task_id=task.id).order_by("-created_at").first()
                or FetchBatch.objects.filter(platform_filter=platform).order_by("-created_at").first()
            )
            status = batch.status if batch else "MISSING"
            self._print_summary(batch, status)
            if status in {
                FetchBatch.Status.COMPLETED,
                FetchBatch.Status.PARTIAL,
                FetchBatch.Status.CANCELLED,
            }:
                break
            time.sleep(poll_interval)

        if status not in {FetchBatch.Status.COMPLETED, FetchBatch.Status.PARTIAL, FetchBatch.Status.CANCELLED}:
            if batch:
                from django.core.management import call_command

                self.stderr.write(
                    f"Smoke batch #{batch.pk} exceeded the strict time limit; stopping it now."
                )
                call_command("stop_fetch_batch", batch_id=batch.pk)
            raise CommandError("Selective harvest smoke did not finish within the strict time limit.")

        if status != FetchBatch.Status.COMPLETED:
            raise CommandError(f"Selective harvest smoke finished with non-success status: {status}")

        self.stdout.write(self.style.SUCCESS("Selective harvest smoke completed successfully."))

    def _print_summary(self, batch, status):
        decision_counts = dict(Counter(
            RawJob.objects.exclude(filter_decision__isnull=True)
            .values_list("filter_decision", flat=True)
        ))
        self.stdout.write(
            "Batch: "
            + str({
                "id": batch.id if batch else None,
                "status": status,
                "total": batch.total_companies if batch else None,
                "completed": batch.completed_companies if batch else None,
                "failed": batch.failed_companies if batch else None,
            })
        )
        self.stdout.write(f"RawJob total: {RawJob.objects.count()}")
        self.stdout.write(f"Filter decisions: {decision_counts}")
        self.stdout.write(f"JD skipped: {RawJob.objects.filter(jd_fetch_skipped=True).count()}")
        self.stdout.write(f"Skipped title logs: {HarvestSkippedTitle.objects.count()}")
