"""Teamtailor platform row; extend Zoho patterns; optional disabled planned ATS rows (matrix parity)."""

from django.db import migrations

TEAMTAILOR = {
    "name": "Teamtailor",
    "slug": "teamtailor",
    "url_patterns": ["teamtailor.com/jobs", ".teamtailor.com"],
    "api_type": "html_scrape",
    "color_hex": "#FF6B6B",
    "rate_limit_per_min": 5,
    "notes": "Dedicated harvester — /jobs block-grid-item HTML (OpenPostings-aligned).",
}

ZOHO_NOTES = (
    "Dedicated harvester — hidden jobs/meta inputs (OpenPostings-aligned). "
    "Portal + zohorecruit.com hosts."
)

# OpenPostings matrix rows still marked PLANNED — disabled in admin until implemented.
PLANNED_DISABLED = [
    {
        "name": "ApplicantAI",
        "slug": "applicantai",
        "url_patterns": ["applicantai.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings HTML blocks parser.",
    },
    {
        "name": "CareerPlug",
        "slug": "career_plug",
        "url_patterns": ["careerplug.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
    {
        "name": "CareerPuck",
        "slug": "career_puck",
        "url_patterns": ["careerpuck.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
    {
        "name": "Fountain",
        "slug": "fountain",
        "url_patterns": ["fountain.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
    {
        "name": "Getro",
        "slug": "getro",
        "url_patterns": ["getro.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
    {
        "name": "HRM Direct",
        "slug": "hrm_direct",
        "url_patterns": ["hrmdirect.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
    {
        "name": "Talent Lyft",
        "slug": "talent_lyft",
        "url_patterns": ["talentlyft.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
    {
        "name": "Talexio",
        "slug": "talexio",
        "url_patterns": ["talexio.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
    {
        "name": "Talent Reef",
        "slug": "talent_reef",
        "url_patterns": ["talentreef.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
    {
        "name": "Manatal",
        "slug": "manatal",
        "url_patterns": ["manatal.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
    {
        "name": "Gem",
        "slug": "gem",
        "url_patterns": [
            "jobs.gem.com",
            "careers.gem.com",
            "gem.com/careers",
            "gem.com/jobs",
        ],
        "api_type": "html_scrape",
        "notes": (
            "Planned — OpenPostings parity. Patterns use Gem ATS-style paths "
            "(not bare gem.com)."
        ),
    },
    {
        "name": "Jobaps",
        "slug": "jobaps",
        "url_patterns": ["jobaps.com"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
    {
        "name": "Join.com",
        "slug": "join",
        "url_patterns": [
            ".join.com/jobs",
            "boards.join.com",
            "join.com/careers",
            "apply.join.com",
        ],
        "api_type": "html_scrape",
        "notes": (
            "Planned — OpenPostings parity. Patterns use Join.com board paths "
            "(not bare join.com)."
        ),
    },
    {
        "name": "SAP HR Cloud",
        "slug": "saphrcloud",
        "url_patterns": ["saphrcloud"],
        "api_type": "html_scrape",
        "notes": "Planned — OpenPostings parity.",
    },
]


def seed_teamtailor_and_zoho(apps, schema_editor):
    JobBoardPlatform = apps.get_model("harvest", "JobBoardPlatform")
    JobBoardPlatform.objects.get_or_create(
        slug=TEAMTAILOR["slug"],
        defaults=TEAMTAILOR,
    )

    z = JobBoardPlatform.objects.filter(slug="zoho").first()
    if z:
        patterns = list(z.url_patterns or [])
        for p in (".zohorecruit.com", "zohorecruit.com"):
            if p not in patterns:
                patterns.append(p)
        z.url_patterns = patterns
        z.notes = ZOHO_NOTES
        z.save(update_fields=["url_patterns", "notes"])


def unseed_teamtailor_and_zoho(apps, schema_editor):
    JobBoardPlatform = apps.get_model("harvest", "JobBoardPlatform")
    JobBoardPlatform.objects.filter(slug=TEAMTAILOR["slug"]).delete()

    z = JobBoardPlatform.objects.filter(slug="zoho").first()
    if z:
        patterns = list(z.url_patterns or [])
        for rm in (".zohorecruit.com", "zohorecruit.com"):
            while rm in patterns:
                patterns.remove(rm)
        z.url_patterns = patterns
        if z.notes == ZOHO_NOTES:
            z.notes = ""
        z.save(update_fields=["url_patterns", "notes"])


def seed_planned_disabled(apps, schema_editor):
    JobBoardPlatform = apps.get_model("harvest", "JobBoardPlatform")
    for row in PLANNED_DISABLED:
        slug = row["slug"]
        defaults = {
            "name": row["name"],
            "url_patterns": row["url_patterns"],
            "api_type": row["api_type"],
            "notes": row["notes"],
            "is_enabled": False,
            "rate_limit_per_min": 5,
        }
        JobBoardPlatform.objects.get_or_create(slug=slug, defaults=defaults)


class Migration(migrations.Migration):
    dependencies = [
        ("harvest", "0009_jobboardplatform_breezy"),
    ]

    operations = [
        migrations.RunPython(seed_teamtailor_and_zoho, unseed_teamtailor_and_zoho),
        migrations.RunPython(seed_planned_disabled, migrations.RunPython.noop),
    ]
