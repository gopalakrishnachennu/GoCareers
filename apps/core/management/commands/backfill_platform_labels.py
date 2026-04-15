"""
management command: backfill_platform_labels

Scans every job's original_link, detects the ATS platform from the URL,
and creates/updates CompanyPlatformLabel records — no HTTP requests needed.

This works because consultants already submitted to ATS URLs, so we know
exactly which platform each company uses.

Run once:
    python manage.py backfill_platform_labels

Re-run safely (update_or_create, idempotent):
    python manage.py backfill_platform_labels --force
"""
from django.core.management.base import BaseCommand
from harvest.detectors import URL_PATTERNS, TENANT_EXTRACTORS
from django.utils import timezone


class Command(BaseCommand):
    help = "Auto-label company ATS platforms from existing job original_link URLs"

    def add_arguments(self, parser):
        parser.add_argument(
            "--force",
            action="store_true",
            help="Re-check and overwrite even already-labeled companies.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be done without writing to DB.",
        )

    def handle(self, *args, **options):
        from jobs.models import Job
        from harvest.models import JobBoardPlatform, CompanyPlatformLabel
        from companies.models import Company

        force = options["force"]
        dry_run = options["dry_run"]

        if dry_run:
            self.stdout.write(self.style.WARNING("⚠️  DRY RUN — no DB writes"))

        # Build a map: company_id → (platform_slug, tenant_id, sample_url)
        self.stdout.write("🔍 Scanning job URLs...")
        company_best: dict = {}

        for job in Job.objects.exclude(original_link="").select_related("company_obj").iterator():
            if not job.company_obj_id:
                continue
            cid = job.company_obj_id

            # Skip if already found and not force
            if cid in company_best and not force:
                continue

            raw_url = job.original_link
            url = raw_url.lower()

            for slug, patterns in URL_PATTERNS.items():
                for pattern in patterns:
                    if pattern in url:
                        extractor = TENANT_EXTRACTORS.get(slug)
                        tenant = ""
                        if extractor:
                            m = extractor.search(raw_url)
                            if m:
                                tenant = m.group(1)
                        company_best[cid] = {
                            "slug": slug,
                            "tenant_id": tenant,
                            "sample_url": raw_url,
                        }
                        break
                if cid in company_best and company_best[cid].get("slug") == slug:
                    break

        total_found = len(company_best)
        self.stdout.write(f"📊 Found platform signals for {total_found} companies")

        if dry_run:
            from collections import Counter
            counts = Counter(v["slug"] for v in company_best.values())
            for slug, count in counts.most_common():
                self.stdout.write(f"  {slug:25s} → {count} companies")
            return

        # Load platforms into memory
        platforms = {p.slug: p for p in JobBoardPlatform.objects.filter(is_enabled=True)}

        created = updated = skipped = 0

        for company_id, info in company_best.items():
            slug = info["slug"]
            platform = platforms.get(slug)
            if not platform:
                skipped += 1
                continue

            try:
                company = Company.objects.get(pk=company_id)
            except Company.DoesNotExist:
                skipped += 1
                continue

            # Optionally backfill career_site_url on the company
            if not company.career_site_url and info["sample_url"]:
                # Build a base career URL from the sample job URL
                sample = info["sample_url"]
                # For known clean base URLs, strip the job path
                base = _extract_career_base(slug, sample, info["tenant_id"])
                if base and not dry_run:
                    company.career_site_url = base
                    company.save(update_fields=["career_site_url"])

            obj, was_created = CompanyPlatformLabel.objects.update_or_create(
                company=company,
                defaults={
                    "platform": platform,
                    "confidence": "HIGH",
                    "detection_method": "URL_PATTERN",
                    "tenant_id": info["tenant_id"],
                    "detected_at": timezone.now(),
                    "last_checked_at": timezone.now(),
                    "is_verified": False,
                },
            )

            if was_created:
                created += 1
            else:
                updated += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"\n✅ Done — {created} created, {updated} updated, {skipped} skipped\n"
                f"   Total labeled: {CompanyPlatformLabel.objects.exclude(detection_method='UNDETECTED').count()} companies"
            )
        )

        # Summary by platform
        from collections import Counter
        counts = Counter(
            CompanyPlatformLabel.objects.exclude(detection_method="UNDETECTED")
            .values_list("platform__slug", flat=True)
        )
        self.stdout.write("\n📊 Labels by platform:")
        for slug, count in counts.most_common():
            self.stdout.write(f"  {slug:25s} {count} companies")


def _extract_career_base(slug: str, sample_url: str, tenant_id: str) -> str:
    """
    Build a clean career page base URL from a job URL and tenant ID.
    """
    if slug == "workday" and tenant_id:
        return f"https://{tenant_id}.myworkdayjobs.com/careers"
    if slug == "greenhouse" and tenant_id:
        return f"https://boards.greenhouse.io/{tenant_id}"
    if slug == "lever" and tenant_id:
        return f"https://jobs.lever.co/{tenant_id}"
    if slug == "ashby" and tenant_id:
        return f"https://jobs.ashbyhq.com/{tenant_id}"
    # For others, just return the base domain of the sample URL
    try:
        from urllib.parse import urlparse
        p = urlparse(sample_url)
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return ""
