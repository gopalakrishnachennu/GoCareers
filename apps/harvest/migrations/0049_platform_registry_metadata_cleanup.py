from django.db import migrations


PLANNED_SLUGS = [
    "applicantai",
    "career_plug",
    "career_puck",
    "fountain",
    "gem",
    "getro",
    "hrm_direct",
    "jobaps",
    "join",
    "manatal",
    "saphrcloud",
    "talent_lyft",
    "talent_reef",
    "talexio",
]

GENERIC_ACTIVE_TIERS = {
    "adp": "degraded",
    "applicantpro": "experimental",
    "applytojob": "experimental",
    "theapplicantmanager": "experimental",
}


def cleanup_platform_registry(apps, schema_editor):
    JobBoardPlatform = apps.get_model("harvest", "JobBoardPlatform")

    JobBoardPlatform.objects.filter(slug__in=PLANNED_SLUGS).update(
        is_enabled=False,
        support_tier="unsupported",
    )

    for slug, support_tier in GENERIC_ACTIVE_TIERS.items():
        JobBoardPlatform.objects.filter(slug=slug).update(support_tier=support_tier)

    JobBoardPlatform.objects.filter(slug="dayforce").update(
        is_enabled=True,
        support_tier="healthy",
    )

    debugboard = JobBoardPlatform.objects.filter(slug="debugboard").first()
    if debugboard:
        debugboard.is_enabled = False
        debugboard.support_tier = "unsupported"
        debugboard.api_type = "unknown"
        note = "Disabled automatically: debug/test platform has no URL patterns or verified harvester."
        if note not in (debugboard.notes or ""):
            debugboard.notes = f"{debugboard.notes.strip()}\n{note}".strip()
        debugboard.save(update_fields=["is_enabled", "support_tier", "api_type", "notes"])


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0048_engine_guardrails_and_audit_flags"),
    ]

    operations = [
        migrations.RunPython(cleanup_platform_registry, noop_reverse),
    ]
