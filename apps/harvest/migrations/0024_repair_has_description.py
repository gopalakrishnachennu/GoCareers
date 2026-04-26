"""
One-time repair: sync has_description for rows where the backfill wrote a real
description via .update() (which bypasses Model.save()) but left has_description=False.

Going forward this is prevented because _backfill_process_one_job now includes
has_description in every .update() call.
"""
from django.db import migrations


def repair_has_description(apps, schema_editor):
    schema_editor.execute("""
        UPDATE harvest_rawjob
        SET has_description = TRUE
        WHERE has_description = FALSE
          AND length(trim(coalesce(description, ''))) > 1
    """)
    schema_editor.execute("""
        UPDATE harvest_rawjob
        SET has_description = FALSE
        WHERE has_description = TRUE
          AND length(trim(coalesce(description, ''))) <= 1
    """)


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0023_rawjob_posted_date_index"),
    ]

    operations = [
        migrations.RunPython(repair_has_description, migrations.RunPython.noop),
    ]
