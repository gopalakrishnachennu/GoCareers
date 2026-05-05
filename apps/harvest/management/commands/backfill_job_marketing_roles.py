"""
backfill_job_marketing_roles
============================
For every SYNCED RawJob that has a job_domain set, find the corresponding
Job (via source_raw_job FK or url_hash) and add the matching MarketingRole
to job.marketing_roles (M2M).

Run once after classify_job_domains has finished to retroactively wire up
the 15k already-synced jobs.

Usage:
    python manage.py backfill_job_marketing_roles
    python manage.py backfill_job_marketing_roles --batch-size 500 --dry-run
    python manage.py backfill_job_marketing_roles --overwrite
        # ^ also re-assigns jobs that already have marketing_roles
"""
from __future__ import annotations

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Backfill Job.marketing_roles from RawJob.job_domain for already-synced jobs"

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size", type=int, default=500,
            help="Queryset page size (default 500)",
        )
        parser.add_argument(
            "--dry-run", action="store_true", default=False,
            help="Count matches without writing",
        )
        parser.add_argument(
            "--overwrite", action="store_true", default=False,
            help="Re-assign even for Jobs that already have marketing_roles",
        )

    def handle(self, *args, **options):
        from harvest.models import RawJob
        from jobs.models import Job
        from users.models import MarketingRole

        batch_size = max(50, options["batch_size"])
        dry_run    = options["dry_run"]
        overwrite  = options["overwrite"]

        self.stdout.write(f"dry_run={dry_run}  overwrite={overwrite}")

        # Pre-load MarketingRole slug→obj map (tiny table, fits in memory)
        role_map: dict[str, MarketingRole] = {
            mr.slug: mr for mr in MarketingRole.objects.filter(is_active=True)
        }
        self.stdout.write(f"  Active MarketingRoles loaded: {len(role_map)}")

        # RawJobs that are synced and have a domain
        qs = (
            RawJob.objects
            .filter(sync_status=RawJob.SyncStatus.SYNCED)
            .exclude(job_domain="")
            .select_related("company")
            .order_by("pk")
        )
        total = qs.count()
        self.stdout.write(f"  SYNCED RawJobs with job_domain: {total:,}")
        if dry_run or total == 0:
            return

        assigned = skipped = missing_role = missing_job = 0
        last_pk  = None

        while True:
            page_qs = qs if last_pk is None else qs.filter(pk__gt=last_pk)
            batch = list(page_qs[:batch_size])
            if not batch:
                break
            last_pk = batch[-1].pk

            for rj in batch:
                mr = role_map.get(rj.job_domain)
                if not mr:
                    missing_role += 1
                    continue

                # Find the Job — prefer source_raw_job FK, fall back to url_hash
                job: Job | None = (
                    Job.objects.filter(source_raw_job=rj).first()
                    or (Job.objects.filter(url_hash=rj.url_hash).first() if rj.url_hash else None)
                )
                if not job:
                    missing_job += 1
                    continue

                if not overwrite and job.marketing_roles.exists():
                    skipped += 1
                    continue

                job.marketing_roles.add(mr)
                assigned += 1

        self.stdout.write("")
        self.stdout.write(f"✅  Assigned:       {assigned:,}")
        self.stdout.write(f"   Skipped (already had roles): {skipped:,}")
        self.stdout.write(f"   Missing MarketingRole slug:  {missing_role:,}")
        self.stdout.write(f"   Job not found:               {missing_job:,}")
