"""Narrow Gem / Join.com JobBoardPlatform url_patterns to reduce accidental substring matches."""

from django.db import migrations

# Replaces bare gem.com / join.com (matches too many unrelated URLs as substrings).
GEM_PATTERNS = [
    "jobs.gem.com",
    "careers.gem.com",
    "gem.com/careers",
    "gem.com/jobs",
]
JOIN_PATTERNS = [
    ".join.com/jobs",
    "boards.join.com",
    "join.com/careers",
    "apply.join.com",
]

GEM_NOTES = (
    "Planned — OpenPostings parity. Patterns use Gem ATS-style paths (not bare gem.com)."
)
JOIN_NOTES = (
    "Planned — OpenPostings parity. Patterns use Join.com board paths (not bare join.com)."
)

LEGACY_GEM = ["gem.com"]
LEGACY_JOIN = ["join.com"]
LEGACY_NOTES = "Planned — OpenPostings parity."


def forward(apps, schema_editor):
    JobBoardPlatform = apps.get_model("harvest", "JobBoardPlatform")
    rows = [
        ("gem", GEM_PATTERNS, GEM_NOTES),
        ("join", JOIN_PATTERNS, JOIN_NOTES),
    ]
    for slug, patterns, notes in rows:
        JobBoardPlatform.objects.filter(slug=slug).update(
            url_patterns=patterns,
            notes=notes,
        )


def backward(apps, schema_editor):
    JobBoardPlatform = apps.get_model("harvest", "JobBoardPlatform")
    JobBoardPlatform.objects.filter(slug="gem").update(
        url_patterns=LEGACY_GEM,
        notes=LEGACY_NOTES,
    )
    JobBoardPlatform.objects.filter(slug="join").update(
        url_patterns=LEGACY_JOIN,
        notes=LEGACY_NOTES,
    )


class Migration(migrations.Migration):
    dependencies = [
        ("harvest", "0010_teamtailor_zoho_patterns_and_planned_platforms"),
    ]

    operations = [
        migrations.RunPython(forward, backward),
    ]
