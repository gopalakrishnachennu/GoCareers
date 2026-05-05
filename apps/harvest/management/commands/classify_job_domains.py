"""
classify_job_domains
====================
Batch-classify all RawJobs with taxonomy fields using the
detect_job_category()/detect_job_domains() engines in enrichments.py.

Run this once after the domain taxonomy migration, and again any time
CURRENT_DOMAIN_VERSION bumps (i.e. patterns change).

Usage:
    python manage.py classify_job_domains
    python manage.py classify_job_domains --batch-size 2000 --limit 50000
    python manage.py classify_job_domains --reclassify-all   # force even if domain_version is current
    python manage.py classify_job_domains --dry-run          # show stats, no writes
"""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction


class Command(BaseCommand):
    help = "Classify RawJobs into taxonomy fields (job_category + job_domain) using keyword patterns"

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size", type=int, default=1000,
            help="DB update batch size (default 1000)",
        )
        parser.add_argument(
            "--limit", type=int, default=0,
            help="Max jobs to process (0 = all, default 0)",
        )
        parser.add_argument(
            "--reclassify-all", action="store_true", default=False,
            help="Re-run even on jobs whose domain_version already matches current version",
        )
        parser.add_argument(
            "--dry-run", action="store_true", default=False,
            help="Print stats without writing to DB",
        )
        parser.add_argument(
            "--platform", type=str, default="",
            help="Only process RawJobs from this platform_slug",
        )

    def handle(self, *args, **options):
        from harvest.models import RawJob
        from harvest.enrichments import (
            CURRENT_DOMAIN_VERSION,
            detect_job_category,
            detect_job_domains,
        )

        batch_size  = max(100, options["batch_size"])
        limit       = options["limit"]
        reclassify  = options["reclassify_all"]
        dry_run     = options["dry_run"]
        platform    = options["platform"].strip()

        self.stdout.write(f"Domain version: {CURRENT_DOMAIN_VERSION}  "
                          f"reclassify_all={reclassify}  dry_run={dry_run}")

        qs = RawJob.objects.exclude(title="")
        if platform:
            qs = qs.filter(platform_slug=platform)
        if not reclassify:
            qs = qs.exclude(domain_version=CURRENT_DOMAIN_VERSION)

        total = qs.count()
        if limit:
            total = min(total, limit)

        self.stdout.write(f"  Jobs to classify: {total:,}")
        if dry_run or total == 0:
            return

        classified = 0
        unclassified = 0
        domain_counts: dict[str, int] = {}
        offset = 0

        while offset < total:
            batch = list(
                qs.only("pk", "title", "description", "job_category", "department_normalized")
                .order_by("pk")[offset:offset + batch_size]
            )
            if not batch:
                break

            updates: list[RawJob] = []
            for rj in batch:
                domains = detect_job_domains(
                    rj.title or "",
                    rj.description or "",
                    rj.job_category or "",
                    rj.department_normalized or "",
                    max_matches=3,
                )
                domain = domains[0] if domains else ""
                category, _title_match, _desc_match = detect_job_category(
                    rj.title or "",
                    rj.description or "",
                    department_normalized=rj.department_normalized or "",
                    domain_slug=domain,
                )
                rj.job_domain    = domain
                rj.job_domain_candidates = domains[:3]
                rj.job_category = category or rj.job_category or ""
                rj.domain_version = CURRENT_DOMAIN_VERSION if domain else ""
                updates.append(rj)
                if domain:
                    classified += 1
                    domain_counts[domain] = domain_counts.get(domain, 0) + 1
                else:
                    unclassified += 1

            with transaction.atomic():
                RawJob.objects.bulk_update(
                    updates,
                    ["job_category", "job_domain", "job_domain_candidates", "domain_version"],
                )

            offset += len(batch)
            if offset % 10000 == 0 or offset >= total:
                self.stdout.write(f"  {offset:,}/{total:,} processed …")

        self.stdout.write("")
        self.stdout.write(f"✅  Classified:   {classified:,}")
        self.stdout.write(f"   Unclassified: {unclassified:,}")
        self.stdout.write("")
        self.stdout.write("  TOP DOMAINS:")
        for slug, cnt in sorted(domain_counts.items(), key=lambda x: -x[1]):
            self.stdout.write(f"    {cnt:>7,}  {slug}")
