"""
Data migration: set list_has_description=True for platforms whose list API
includes job description text — enabling Tier-2 JD gate at zero HTTP cost.

Platforms marked True:
  lever       — descriptionPlain + listsPlain in every list response
  ashby       — descriptionHtml fetched during list phase (Step 2 in harvester)
  greenhouse  — full content field when fetched with ?content=true

All other platforms default to False (require a detail fetch for snippet).
"""
from django.db import migrations


def set_list_has_description(apps, schema_editor):
    JobBoardPlatform = apps.get_model("harvest", "JobBoardPlatform")
    # These three platforms return description text in their list endpoint response.
    slugs_with_description = {"lever", "ashby", "greenhouse"}
    updated = JobBoardPlatform.objects.filter(slug__in=slugs_with_description).update(
        list_has_description=True
    )
    print(f"  set list_has_description=True on {updated} platforms")


def reverse_set_list_has_description(apps, schema_editor):
    JobBoardPlatform = apps.get_model("harvest", "JobBoardPlatform")
    JobBoardPlatform.objects.all().update(list_has_description=False)


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0057_advanced_engine_phase1"),
    ]

    operations = [
        migrations.RunPython(
            set_list_has_description,
            reverse_set_list_has_description,
        ),
    ]
