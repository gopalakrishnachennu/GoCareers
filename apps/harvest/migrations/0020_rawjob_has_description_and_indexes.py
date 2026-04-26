from django.db import migrations, models


def backfill_has_description(apps, schema_editor):
    schema_editor.execute(
        "UPDATE harvest_rawjob SET has_description = (length(trim(coalesce(description, ''))) > 1)"
    )


def add_column_if_missing(apps, schema_editor):
    schema_editor.execute("""
        ALTER TABLE harvest_rawjob
        ADD COLUMN IF NOT EXISTS has_description boolean NOT NULL DEFAULT false
    """)
    backfill_has_description(apps, schema_editor)


def add_index_if_missing(apps, schema_editor, index_sql, index_name):
    schema_editor.execute(f"""
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE tablename = 'harvest_rawjob' AND indexname = '{index_name}'
          ) THEN
            {index_sql};
          END IF;
        END $$
    """)


def create_indexes(apps, schema_editor):
    indexes = [
        ("CREATE INDEX harvest_raw_fetched_idx        ON harvest_rawjob (fetched_at DESC)",         "harvest_raw_fetched_idx"),
        ("CREATE INDEX harvest_raw_remote_idx          ON harvest_rawjob (is_remote)",               "harvest_raw_remote_idx"),
        ("CREATE INDEX harvest_raw_hasdesc_idx         ON harvest_rawjob (has_description)",          "harvest_raw_hasdesc_idx"),
        ("CREATE INDEX harvest_raw_sync_fetched_idx    ON harvest_rawjob (sync_status, fetched_at DESC)", "harvest_raw_sync_fetched_idx"),
        ("CREATE INDEX harvest_raw_active_fetched_idx  ON harvest_rawjob (is_active, fetched_at DESC)", "harvest_raw_active_fetched_idx"),
        ("CREATE INDEX harvest_raw_remote_fetched_idx  ON harvest_rawjob (is_remote, fetched_at DESC)", "harvest_raw_remote_fetched_idx"),
        ("CREATE INDEX harvest_raw_hasdesc_fetched_idx ON harvest_rawjob (has_description, fetched_at DESC)", "harvest_raw_hasdesc_fetched_idx"),
        ("CREATE INDEX harvest_raw_platform_fetched_idx ON harvest_rawjob (platform_slug, fetched_at DESC)", "harvest_raw_platform_fetched_idx"),
    ]
    for sql, name in indexes:
        add_index_if_missing(apps, schema_editor, sql, name)


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0019_alter_harvestengineconfig_api_stagger_ms_and_more"),
    ]

    operations = [
        # Use SeparateDatabaseAndState so Django's state gets the field
        # even though we handle the actual ALTER TABLE ourselves (IF NOT EXISTS).
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunPython(add_column_if_missing, migrations.RunPython.noop),
            ],
            state_operations=[
                migrations.AddField(
                    model_name="rawjob",
                    name="has_description",
                    field=models.BooleanField(default=False, db_index=True),
                ),
            ],
        ),
        # All indexes use IF NOT EXISTS via RunPython
        migrations.RunPython(create_indexes, migrations.RunPython.noop),
    ]
