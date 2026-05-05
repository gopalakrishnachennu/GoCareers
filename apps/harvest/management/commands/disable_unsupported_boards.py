"""
Management command: disable_unsupported_boards

Sets is_enabled=False and support_tier='unsupported' in the DB for boards
that have no active harvester. Safe to re-run (idempotent).

Usage:
    python manage.py disable_unsupported_boards [--dry-run]
"""
from django.core.management.base import BaseCommand

UNSUPPORTED = {
    "applytojob": "No active harvester — legacy platform, no documented public API.",
    "adp":        "ADP Workforce Now requires OAuth auth — not supported in public harvest.",
    "applicantpro": "No documented public API; HTML scraper would be fragile.",
    "dayforce":   "Ceridian Dayforce — authenticated API, no public job feed endpoint.",
}


class Command(BaseCommand):
    help = "Mark unsupported job board platforms as disabled in the DB."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Show what would change without writing to the DB.",
        )

    def handle(self, *args, **options):
        from harvest.models import JobBoardPlatform

        dry_run = options["dry_run"]
        updated = 0
        created = 0

        for slug, note in UNSUPPORTED.items():
            obj, was_created = JobBoardPlatform.objects.get_or_create(
                slug=slug,
                defaults={
                    "name": slug.title(),
                    "support_tier": JobBoardPlatform.SupportTier.UNSUPPORTED,
                    "is_enabled": False,
                    "notes": note,
                },
            )
            if was_created:
                if dry_run:
                    self.stdout.write(self.style.WARNING(
                        f"[DRY RUN] Would create: {slug} (unsupported, disabled)"
                    ))
                else:
                    created += 1
                    self.stdout.write(self.style.SUCCESS(f"Created: {slug}"))
            else:
                needs_update = (
                    obj.support_tier != JobBoardPlatform.SupportTier.UNSUPPORTED
                    or obj.is_enabled
                )
                if needs_update:
                    if dry_run:
                        self.stdout.write(self.style.WARNING(
                            f"[DRY RUN] Would update: {slug} "
                            f"(tier={obj.support_tier}→unsupported, "
                            f"is_enabled={obj.is_enabled}→False)"
                        ))
                    else:
                        obj.support_tier = JobBoardPlatform.SupportTier.UNSUPPORTED
                        obj.is_enabled = False
                        if not obj.notes:
                            obj.notes = note
                        obj.save(update_fields=["support_tier", "is_enabled", "notes"])
                        updated += 1
                        self.stdout.write(self.style.SUCCESS(f"Updated: {slug}"))
                else:
                    self.stdout.write(f"Already correct: {slug}")

        if not dry_run:
            self.stdout.write(self.style.SUCCESS(
                f"\nDone. Created: {created}, Updated: {updated}."
            ))
        else:
            self.stdout.write(self.style.WARNING("\n[DRY RUN] No changes written."))
