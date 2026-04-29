"""State-only migration — indexes already exist in the DB (created by 0020 RunPython).
This just brings Django's migration state in sync with model Meta so
`manage.py migrate` stops reporting pending changes."""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0020_rawjob_has_description_and_indexes"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[],   # indexes already exist, nothing to do
            state_operations=[
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
            ],
        ),
    ]
