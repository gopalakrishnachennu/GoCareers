from django.db import migrations, models


def backfill_has_description(apps, schema_editor):
    # Use raw SQL for speed on 100k+ rows — avoids loading every object into Python
    schema_editor.execute(
        "UPDATE harvest_rawjob SET has_description = (length(trim(coalesce(description, ''))) > 1)"
    )


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0019_alter_harvestengineconfig_api_stagger_ms_and_more"),
    ]

    operations = [
        # 1. Add the denormalized boolean field
        migrations.AddField(
            model_name="rawjob",
            name="has_description",
            field=models.BooleanField(default=False, db_index=True),
        ),
        # 2. Backfill from existing description values
        migrations.RunPython(backfill_has_description, migrations.RunPython.noop),
        # 3. Add missing single-column indexes
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["fetched_at"], name="harvest_raw_fetched_idx"),
        ),
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["is_remote"], name="harvest_raw_remote_idx"),
        ),
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["has_description"], name="harvest_raw_hasdesc_idx"),
        ),
        # 4. Composite indexes for filter + ORDER BY fetched_at DESC
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["sync_status", "-fetched_at"], name="harvest_raw_sync_fetched_idx"),
        ),
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["is_active", "-fetched_at"], name="harvest_raw_active_fetched_idx"),
        ),
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["is_remote", "-fetched_at"], name="harvest_raw_remote_fetched_idx"),
        ),
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["has_description", "-fetched_at"], name="harvest_raw_hasdesc_fetched_idx"),
        ),
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["platform_slug", "-fetched_at"], name="harvest_raw_platform_fetched_idx"),
        ),
    ]
