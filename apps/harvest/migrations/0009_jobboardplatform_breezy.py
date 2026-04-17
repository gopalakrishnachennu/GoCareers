"""Register Breezy HR (OpenPostings-aligned dedicated HTML harvester)."""

from django.db import migrations

BREEZY = {
    "name": "Breezy HR",
    "slug": "breezy",
    "url_patterns": [".breezy.hr", "breezy.hr/p/"],
    "api_type": "html_scrape",
    "color_hex": "#00B386",
    "rate_limit_per_min": 5,
    "notes": "Subdomain career sites — parser aligned with OpenPostings parseBreezyPostingsFromHtml",
}


def seed(apps, schema_editor):
    JobBoardPlatform = apps.get_model("harvest", "JobBoardPlatform")
    JobBoardPlatform.objects.get_or_create(slug=BREEZY["slug"], defaults=BREEZY)


def unseed(apps, schema_editor):
    JobBoardPlatform = apps.get_model("harvest", "JobBoardPlatform")
    JobBoardPlatform.objects.filter(slug=BREEZY["slug"]).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("harvest", "0008_companyfetchrun_jobs_total_available"),
    ]

    operations = [
        migrations.RunPython(seed, unseed),
    ]
