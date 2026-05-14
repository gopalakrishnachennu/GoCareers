"""audit_phrase_quality — find phrases that contain seniority words.

After the role_filter fix (normalize_phrase no longer strips seniority from
phrases), any phrase that contains a seniority word (senior, staff, lead, etc.)
will ONLY match job titles that contain that exact seniority word — but our
title normalizer strips those words.  Net result: the phrase never fires.

This command finds those dead phrases and tells you what to do with them.

Usage:
    python manage.py audit_phrase_quality
    python manage.py audit_phrase_quality --fix   # auto-strip seniority from phrases and save
"""
from __future__ import annotations

import re

from django.core.management.base import BaseCommand

SENIORITY_PATTERNS = [
    (r"\bsenior\b",     "senior"),
    (r"\bjunior\b",     "junior"),
    (r"\bstaff\b",      "staff"),
    (r"\bprincipal\b",  "principal"),
    (r"\blead\b",       "lead"),
    (r"\bsr\.?\b",      "sr"),
    (r"\bjr\.?\b",      "jr"),
    (r"\bhead of\b",    "head of"),
    (r"\bdirector of\b","director of"),
    (r"\bvp of\b",      "vp of"),
    (r"\bdistinguished\b","distinguished"),
    (r"\bassociate\b",  "associate"),
    (r"\bmid-?level\b", "mid-level"),
    (r"\bentry-?level\b","entry-level"),
]


def _has_seniority(phrase: str) -> list[str]:
    found = []
    p = phrase.lower()
    for pattern, word in SENIORITY_PATTERNS:
        if re.search(pattern, p):
            found.append(word)
    return found


def _strip_seniority(phrase: str) -> str:
    p = phrase.lower().strip()
    for pattern, _ in SENIORITY_PATTERNS:
        p = re.sub(pattern, " ", p)
    return re.sub(r"\s+", " ", p).strip()


class Command(BaseCommand):
    help = "Audit HarvestRoleCategory phrases for seniority words that make them dead after the normalize_phrase fix."

    def add_arguments(self, parser):
        parser.add_argument(
            "--fix",
            action="store_true",
            default=False,
            help="Auto-strip seniority words from affected phrases and save to DB.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Print what would change without saving.",
        )

    def handle(self, *args, **options):
        from harvest.models import HarvestRoleCategory

        fix_mode = options["fix"] and not options["dry_run"]
        issues_found = 0
        categories_fixed = 0

        for cat in HarvestRoleCategory.objects.order_by("priority", "name"):
            include_issues = []
            exclude_issues = []

            for phrase in cat.include_phrases or []:
                hits = _has_seniority(phrase)
                if hits:
                    include_issues.append((phrase, hits))

            for phrase in cat.exclude_phrases or []:
                hits = _has_seniority(phrase)
                if hits:
                    exclude_issues.append((phrase, hits))

            if not include_issues and not exclude_issues:
                continue

            self.stdout.write(self.style.WARNING(f"\n[{cat.slug}] {cat.name}"))

            new_includes = list(cat.include_phrases or [])
            new_excludes = list(cat.exclude_phrases or [])

            for phrase, words in include_issues:
                issues_found += 1
                stripped = _strip_seniority(phrase)
                self.stdout.write(
                    f"  INCLUDE  {phrase!r:40} → contains [{', '.join(words)}]"
                    + (f" → would become {stripped!r}" if stripped else " → would become EMPTY (delete)")
                )
                if fix_mode:
                    new_includes = [p for p in new_includes if p != phrase]
                    if stripped and stripped not in new_includes:
                        new_includes.append(stripped)

            for phrase, words in exclude_issues:
                issues_found += 1
                stripped = _strip_seniority(phrase)
                self.stdout.write(
                    f"  EXCLUDE  {phrase!r:40} → contains [{', '.join(words)}]"
                    + (f" → would become {stripped!r}" if stripped else " → would become EMPTY (delete)")
                )
                if fix_mode:
                    new_excludes = [p for p in new_excludes if p != phrase]
                    if stripped and stripped not in new_excludes:
                        new_excludes.append(stripped)

            if fix_mode:
                cat.include_phrases = new_includes
                cat.exclude_phrases = new_excludes
                cat.save(update_fields=["include_phrases", "exclude_phrases", "updated_at"])
                categories_fixed += 1
                self.stdout.write(self.style.SUCCESS(f"  ✓ Saved"))

        self.stdout.write("")
        if issues_found == 0:
            self.stdout.write(self.style.SUCCESS("✓ All phrases are clean — no seniority words found."))
        else:
            self.stdout.write(
                self.style.WARNING(f"Found {issues_found} phrase(s) with seniority words.")
            )
            if fix_mode:
                self.stdout.write(self.style.SUCCESS(f"Fixed {categories_fixed} category/categories."))
            else:
                self.stdout.write("Run with --fix to auto-clean them (or edit via Selective Filter admin).")
                self.stdout.write(
                    self.style.NOTICE(
                        "\nNote: After fix, 'Staff Engineer' titles will be COLD unless you have\n"
                        "'software engineer' (or similar) as an include phrase — which you should."
                    )
                )
