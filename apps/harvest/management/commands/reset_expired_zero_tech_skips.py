from __future__ import annotations

from django.core.management.base import BaseCommand
from django.utils import timezone

from harvest.models import CompanyPlatformLabel


class Command(BaseCommand):
    help = "Clear expired selective-harvest zero-tech company skip flags."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Report rows without updating them.")

    def handle(self, *args, **options):
        now = timezone.now()
        dry_run = bool(options["dry_run"])
        qs = CompanyPlatformLabel.objects.filter(
            skip_in_selective_harvest=True,
            skip_expires_at__isnull=False,
            skip_expires_at__lte=now,
        )
        count = qs.count()
        if not dry_run:
            qs.update(
                skip_in_selective_harvest=False,
                consecutive_zero_tech_fetches=0,
                zero_tech_last_flagged_at=None,
                skip_expires_at=None,
            )
        self.stdout.write(self.style.SUCCESS(f"dry_run={dry_run} reset={count} expired selective-harvest skip flag(s)."))
