"""
print_scope_summary
===================
Aggregates the current RawJob scope distribution + top countries +
LocationCache totals. Read-only. Fast (single aggregate query).
"""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Count


class Command(BaseCommand):
    help = "Print current RawJob scope distribution and Mapbox cache stats"

    def handle(self, *args, **options):
        from harvest.models import RawJob, LocationCache

        total = RawJob.objects.count()
        self.stdout.write(f"Total RawJobs: {total:,}")
        self.stdout.write("")
        self.stdout.write("Scope status breakdown:")
        for row in (
            RawJob.objects.values("scope_status").annotate(c=Count("id")).order_by("-c")
        ):
            self.stdout.write(
                f"  {(row['scope_status'] or '(empty)').ljust(30)}{format(row['c'], ',').rjust(10)}"
            )

        self.stdout.write("")
        self.stdout.write("Top 15 countries:")
        for row in (
            RawJob.objects.exclude(country_code="")
            .values("country_code")
            .annotate(c=Count("id"))
            .order_by("-c")[:15]
        ):
            self.stdout.write(
                f"  {row['country_code'].ljust(6)}{format(row['c'], ',').rjust(10)}"
            )

        self.stdout.write("")
        cache_total = LocationCache.objects.count()
        cache_mapbox = LocationCache.objects.filter(provider="mapbox").count()
        cache_rules = LocationCache.objects.filter(source="rules").count()
        self.stdout.write(
            f"LocationCache: {cache_total:,} rows ({cache_mapbox:,} mapbox, {cache_rules:,} rules)"
        )
