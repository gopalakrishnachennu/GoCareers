from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Validate that every domain-classifier slug exists in active MarketingRole records"

    def handle(self, *args, **options):
        from harvest.enrichments import _DOMAIN_PATTERNS
        from users.models import MarketingRole

        classifier_slugs = {slug for slug, _pattern in _DOMAIN_PATTERNS}
        active_role_slugs = set(
            MarketingRole.objects.filter(is_active=True).values_list("slug", flat=True)
        )
        missing = sorted(classifier_slugs - active_role_slugs)
        if missing:
            raise CommandError(
                "Missing MarketingRole rows for classifier slugs: " + ", ".join(missing)
            )
        self.stdout.write(
            self.style.SUCCESS(
                f"Marketing role taxonomy OK. Validated {len(classifier_slugs)} classifier slugs."
            )
        )
