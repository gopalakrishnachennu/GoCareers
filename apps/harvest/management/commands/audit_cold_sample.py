from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from harvest.models import HarvestSkippedTitle


class Command(BaseCommand):
    help = "Print sampled COLD/NO_MATCH skipped-title rows for human review."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=7)
        parser.add_argument("--limit", type=int, default=50)
        parser.add_argument("--dry-run", action="store_true", help="Accepted for ops consistency; this command is read-only.")

    def handle(self, *args, **options):
        days = max(1, int(options["days"] or 7))
        limit = max(1, int(options["limit"] or 50))
        qs = (
            HarvestSkippedTitle.objects
            .filter(is_sampled=True, skipped_at__gte=timezone.now() - timedelta(days=days))
            .order_by("-skipped_at")[:limit]
        )
        for row in qs:
            self.stdout.write(
                f"#{row.pk}\t{row.filter_decision}\t{row.platform_slug}\t"
                f"{row.company_name}\t{row.job_title}\t{row.filter_reason}"
            )
