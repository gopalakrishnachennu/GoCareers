"""
Sample each enabled platform: run a real fetch for up to N labeled companies per platform.

Usage:
  python manage.py smoke_test_harvest
  python manage.py smoke_test_harvest --platform greenhouse --per-platform 2
  python manage.py smoke_test_harvest --dry-run
"""

import time
import traceback

from django.core.management.base import BaseCommand

from harvest.models import CompanyPlatformLabel, JobBoardPlatform

LABEL_METHODS = ("URL_PATTERN", "HTTP_HEAD", "HTML_PARSE", "MANUAL")


class Command(BaseCommand):
    help = (
        "Smoke-test harvesters: for each enabled platform, fetch jobs for up to "
        "N companies that have a tenant (default N=2). Runs synchronously (no Celery)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--per-platform",
            type=int,
            default=2,
            help="Max labeled companies to sample per platform (default 2).",
        )
        parser.add_argument(
            "--platform",
            type=str,
            default="",
            help="Only this platform slug (optional).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="List which companies would be fetched; no HTTP.",
        )
        parser.add_argument(
            "--since-hours",
            type=int,
            default=24,
            help="since_hours passed to fetch_jobs (default 24).",
        )

    def handle(self, *args, **options):
        from harvest.harvesters import get_harvester
        from harvest.tasks import (
            HTML_SCRAPE_PLATFORMS,
            INTER_COMPANY_DELAY_API,
            INTER_COMPANY_DELAY_SCRAPE,
        )

        per_pf = max(1, options["per_platform"])
        slug_filter = (options["platform"] or "").strip().lower()
        dry = options["dry_run"]
        since_hours = options["since_hours"]

        qs = JobBoardPlatform.objects.filter(is_enabled=True).order_by("slug")
        if slug_filter:
            qs = qs.filter(slug=slug_filter)

        platforms = list(qs)
        if not platforms:
            self.stdout.write(self.style.WARNING("No enabled platforms match."))
            return

        self.stdout.write(
            self.style.MIGRATE_HEADING(
                f"Harvest smoke test — {len(platforms)} platform(s), "
                f"up to {per_pf} company/companies each"
                + (" (DRY RUN)" if dry else "")
            )
        )

        any_fetch = False
        errors = 0

        for platform in platforms:
            labels = list(
                CompanyPlatformLabel.objects.filter(
                    platform=platform,
                    tenant_id__gt="",
                    detection_method__in=LABEL_METHODS,
                )
                .select_related("company")
                .order_by("company__name")[:per_pf]
            )

            if not labels:
                self.stdout.write(
                    f"  [{platform.slug}] SKIP — no labeled companies with tenant "
                    f"({', '.join(LABEL_METHODS)})"
                )
                continue

            is_scraper = platform.slug in HTML_SCRAPE_PLATFORMS
            inter_delay = (
                INTER_COMPANY_DELAY_SCRAPE if is_scraper else INTER_COMPANY_DELAY_API
            )

            harvester = get_harvester(platform.slug)
            hname = harvester.__class__.__name__

            for i, label in enumerate(labels):
                if i and not dry:
                    time.sleep(inter_delay)

                company = label.company
                line = (
                    f"  [{platform.slug}] {company.name} (pk={company.pk}) "
                    f"tenant={label.tenant_id[:48]}{'…' if len(label.tenant_id) > 48 else ''} "
                    f"harvester={hname}"
                )

                if dry:
                    self.stdout.write(line + " → dry-run")
                    continue

                any_fetch = True
                try:
                    jobs = harvester.fetch_jobs(
                        company,
                        label.tenant_id,
                        since_hours=since_hours,
                        fetch_all=False,
                    )
                    n = len(jobs or [])
                    total_avail = getattr(harvester, "last_total_available", None)
                    extra = (
                        f" total_avail={total_avail}"
                        if total_avail not in (None, 0, n)
                        else ""
                    )
                    self.stdout.write(
                        self.style.SUCCESS(f"{line} → OK jobs={n}{extra}")
                    )
                except Exception as exc:
                    errors += 1
                    self.stdout.write(
                        self.style.ERROR(f"{line} → ERROR {exc!s}")
                    )
                    self.stdout.write(self.style.ERROR(traceback.format_exc()))

        if dry:
            self.stdout.write(self.style.NOTICE("Dry run finished (no HTTP)."))
            return

        if not any_fetch:
            self.stdout.write(
                self.style.WARNING(
                    "No HTTP fetches ran — add CompanyPlatformLabel rows with tenant_id "
                    "or enable more platforms."
                )
            )
        elif errors:
            self.stdout.write(
                self.style.ERROR(f"Finished with {errors} error(s).")
            )
            raise SystemExit(1)
        else:
            self.stdout.write(self.style.SUCCESS("Smoke test finished OK."))
