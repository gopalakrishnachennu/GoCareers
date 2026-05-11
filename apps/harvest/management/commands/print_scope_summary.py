"""
print_scope_summary
===================
Aggregates the current RawJob scope distribution + top countries +
LocationCache totals + sync-readiness / gate breakdown. Read-only. Fast.
"""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Count, Q


class Command(BaseCommand):
    help = "Print current RawJob scope distribution and sync-gate metrics"

    def handle(self, *args, **options):
        from harvest.models import RawJob, LocationCache

        total = RawJob.objects.count()
        self.stdout.write(f"Total RawJobs: {total:,}")
        self.stdout.write("")

        # ── Scope status breakdown ────────────────────────────────────────────
        self.stdout.write("Scope status breakdown:")
        for row in (
            RawJob.objects.values("scope_status").annotate(c=Count("id")).order_by("-c")
        ):
            label = (row["scope_status"] or "(empty)").ljust(30)
            count = format(row["c"], ",").rjust(10)
            self.stdout.write(f"  {label}{count}")

        # ── Sync-gate summary ─────────────────────────────────────────────────
        self.stdout.write("")
        self.stdout.write("Sync-gate eligible (is_priority=True + PRIORITY_TARGET | REVIEW_UNKNOWN_COUNTRY):")
        passable_qs = RawJob.objects.filter(
            is_priority=True,
            scope_status__in=[
                RawJob.ScopeStatus.PRIORITY_TARGET,
                RawJob.ScopeStatus.REVIEW_UNKNOWN_COUNTRY,
            ],
        )
        agg = passable_qs.aggregate(
            total=Count("id"),
            pending=Count("id", filter=Q(sync_status="PENDING")),
            synced=Count("id", filter=Q(sync_status="SYNCED")),
            failed=Count("id", filter=Q(sync_status="FAILED")),
            missing_jd=Count("id", filter=Q(has_description=False)),
            active=Count("id", filter=Q(is_active=True)),
        )
        w = 28
        self.stdout.write(f"  {'Gate-eligible total'.ljust(w)}{format(agg['total'], ',').rjust(10)}")
        self.stdout.write(f"  {'  is_active'.ljust(w)}{format(agg['active'], ',').rjust(10)}")
        self.stdout.write(f"  {'  sync PENDING'.ljust(w)}{format(agg['pending'], ',').rjust(10)}")
        self.stdout.write(f"  {'  sync SYNCED'.ljust(w)}{format(agg['synced'], ',').rjust(10)}")
        self.stdout.write(f"  {'  sync FAILED'.ljust(w)}{format(agg['failed'], ',').rjust(10)}")
        self.stdout.write(f"  {'  missing JD'.ljust(w)}{format(agg['missing_jd'], ',').rjust(10)}")

        cold_total = RawJob.objects.filter(
            scope_status__in=[
                RawJob.ScopeStatus.COLD_NON_TARGET_COUNTRY,
                RawJob.ScopeStatus.COLD_NO_LOCATION,
            ]
        ).count()
        unscoped = RawJob.objects.filter(
            Q(scope_status="") | Q(scope_status=RawJob.ScopeStatus.UNSCOPED)
        ).count()
        unknown = RawJob.objects.filter(
            scope_status=RawJob.ScopeStatus.REVIEW_UNKNOWN_COUNTRY
        ).count()
        self.stdout.write("")
        self.stdout.write(f"  {'Cold (gate-blocked)'.ljust(w)}{format(cold_total, ',').rjust(10)}")
        self.stdout.write(f"  {'REVIEW_UNKNOWN_COUNTRY'.ljust(w)}{format(unknown, ',').rjust(10)}")
        self.stdout.write(f"  {'Unscoped (never evaluated)'.ljust(w)}{format(unscoped, ',').rjust(10)}")

        # ── Top countries ─────────────────────────────────────────────────────
        self.stdout.write("")
        self.stdout.write("Top 15 countries (by country_code):")
        for row in (
            RawJob.objects.exclude(country_code="")
            .values("country_code")
            .annotate(c=Count("id"))
            .order_by("-c")[:15]
        ):
            self.stdout.write(
                f"  {row['country_code'].ljust(6)}{format(row['c'], ',').rjust(10)}"
            )

        # ── LocationCache ─────────────────────────────────────────────────────
        self.stdout.write("")
        cache_total = LocationCache.objects.count()
        cache_mapbox = LocationCache.objects.filter(provider="mapbox").count()
        cache_rules = LocationCache.objects.filter(source="rules").count()
        self.stdout.write(
            f"LocationCache: {cache_total:,} rows ({cache_mapbox:,} mapbox, {cache_rules:,} rules)"
        )
