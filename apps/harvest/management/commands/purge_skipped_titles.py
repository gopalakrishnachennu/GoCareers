from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from harvest.models import HarvestSkippedTitle


class Command(BaseCommand):
    help = "Delete HarvestSkippedTitle audit rows older than N days. RawJob rows are preserved."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=30)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        days = max(1, int(options["days"] or 30))
        cutoff = timezone.now() - timedelta(days=days)
        qs = HarvestSkippedTitle.objects.filter(skipped_at__lt=cutoff)
        count = qs.count()
        if not options["dry_run"]:
            qs.delete()
        self.stdout.write(self.style.SUCCESS(f"dry_run={options['dry_run']} cutoff={cutoff.isoformat()} deleted={count}"))
