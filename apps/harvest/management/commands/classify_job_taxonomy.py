"""
classify_job_taxonomy
=====================
Convenience wrapper for the full raw-job taxonomy backfill.

This command backfills:
  - job_category
  - job_domain
  - job_domain_candidates
  - domain_version

It delegates to `classify_job_domains`, which now handles both category and
domain classification together.
"""
from __future__ import annotations

from django.core.management import call_command
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Backfill raw job taxonomy (category + domain candidates + primary domain)"

    def add_arguments(self, parser):
        parser.add_argument("--batch-size", type=int, default=1000)
        parser.add_argument("--limit", type=int, default=0)
        parser.add_argument("--reclassify-all", action="store_true", default=False)
        parser.add_argument("--dry-run", action="store_true", default=False)
        parser.add_argument("--platform", type=str, default="")

    def handle(self, *args, **options):
        call_command(
            "classify_job_domains",
            batch_size=options["batch_size"],
            limit=options["limit"],
            reclassify_all=options["reclassify_all"],
            dry_run=options["dry_run"],
            platform=options["platform"],
        )
