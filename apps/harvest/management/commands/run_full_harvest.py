"""
run_full_harvest — launch a full-board harvest batch (fetch_all=True).

Every company's ATS board is scraped in full — NO 25-hour time window.
The batch is tracked in FetchBatch + CompanyFetchRun (visible in the GUI).

Usage (local-ops, connects to prod DB):
    docker compose -f docker-compose.local-harvester.yml run --rm harvester \\
      python manage.py run_full_harvest [options]

Options:
  --platform SLUG      Limit to one ATS platform (e.g. greenhouse, lever).
  --dry-run            Print what would be launched without actually queuing tasks.
  --filter-snapshot ID Use a specific filter snapshot UUID instead of the latest.
  --name TEXT          Custom batch name (default: auto-generated).

The batch honours all HarvestEngineConfig settings live (filter, rate limit, etc.).
"""
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        "Launch a full-board harvest batch (fetch_all=True, no 25-h window). "
        "Every company's ATS board is scraped in its entirety."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--platform",
            type=str,
            default="",
            metavar="SLUG",
            help="Restrict to one platform slug (e.g. greenhouse). Default: all platforms.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Print what would be launched without queuing any Celery tasks.",
        )
        parser.add_argument(
            "--filter-snapshot",
            type=str,
            default=None,
            metavar="UUID",
            help="Use a specific HarvestFilterSnapshot UUID instead of auto-creating one.",
        )
        parser.add_argument(
            "--name",
            type=str,
            default="",
            metavar="TEXT",
            help="Custom batch name (default: auto-generated from date + platform).",
        )

    def handle(self, *args, **options):
        from django.utils import timezone
        from harvest.tasks import fetch_raw_jobs_batch_task
        from harvest.models import CompanyPlatformLabel

        platform = options["platform"] or ""
        dry_run = options["dry_run"]
        filter_snapshot_id = options["filter_snapshot"]
        batch_name = options["name"] or (
            f"Full crawl {timezone.now().strftime('%Y-%m-%d %H:%M UTC')}"
            + (f" [{platform}]" if platform else "")
        )

        # Count eligible labels (rough preview)
        qs = CompanyPlatformLabel.objects.filter(portal_alive__in=[True, None])
        if platform:
            qs = qs.filter(platform__slug=platform)
        label_count = qs.count()

        self.stdout.write(self.style.MIGRATE_HEADING(
            f"\n{'[DRY RUN] ' if dry_run else ''}Full harvest: {label_count} eligible labels"
            f"{f' (platform={platform})' if platform else ''}"
        ))
        self.stdout.write(f"  Batch name   : {batch_name}")
        self.stdout.write(f"  fetch_all    : True  ← NO 25-hour window, entire board per company")
        self.stdout.write(f"  filter snap  : {filter_snapshot_id or 'auto'}")

        if dry_run:
            self.stdout.write(self.style.WARNING(
                "\n[DRY RUN] Nothing queued. Remove --dry-run to launch."
            ))
            return

        confirm = input(
            f"\nQueue {label_count} company tasks? This is a PROD WRITE. Type 'yes' to confirm: "
        )
        if confirm.strip().lower() != "yes":
            self.stdout.write(self.style.WARNING("Aborted."))
            return

        result = fetch_raw_jobs_batch_task.apply_async(
            kwargs={
                "fetch_all": True,          # ← full crawl, no 25-h window
                "platform_slug": platform or None,
                "batch_name": batch_name,
                "filter_snapshot_id": filter_snapshot_id,
                "test_mode": False,
            }
        )
        self.stdout.write(self.style.SUCCESS(
            f"\nBatch queued — Celery task id: {result.id}\n"
            "Monitor progress in the GUI → Harvest → Batch Activity."
        ))
