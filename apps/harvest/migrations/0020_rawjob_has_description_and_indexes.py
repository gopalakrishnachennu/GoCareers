from django.db import migrations, models


def backfill_has_description(apps, schema_editor):
    schema_editor.execute(
        "UPDATE harvest_rawjob SET has_description = (length(trim(coalesce(description, ''))) > 1)"
    )


def add_column_if_missing(apps, schema_editor):
    vendor = schema_editor.connection.vendor
    if vendor == "postgresql":
        schema_editor.execute(
            "ALTER TABLE harvest_rawjob ADD COLUMN IF NOT EXISTS has_description boolean NOT NULL DEFAULT false"
        )
        backfill_has_description(apps, schema_editor)
    else:
        # SQLite / other: check if column exists, add via schema editor if not.
        with schema_editor.connection.cursor() as cursor:
            cursor.execute("PRAGMA table_info(harvest_rawjob)")
            cols = [row[1] for row in cursor.fetchall()]
        if "has_description" not in cols:
            RawJob = apps.get_model("harvest", "RawJob")
            field = models.BooleanField(default=False)
            field.set_attributes_from_name("has_description")
            schema_editor.add_field(RawJob, field)
            backfill_has_description(apps, schema_editor)


def create_indexes(apps, schema_editor):
    vendor = schema_editor.connection.vendor
    indexes = [
        ("harvest_raw_fetched_idx",         "CREATE INDEX {ifne}harvest_raw_fetched_idx        ON harvest_rawjob (fetched_at DESC)"),
        ("harvest_raw_remote_idx",           "CREATE INDEX {ifne}harvest_raw_remote_idx          ON harvest_rawjob (is_remote)"),
        ("harvest_raw_hasdesc_idx",          "CREATE INDEX {ifne}harvest_raw_hasdesc_idx         ON harvest_rawjob (has_description)"),
        ("harvest_raw_sync_fetched_idx",     "CREATE INDEX {ifne}harvest_raw_sync_fetched_idx    ON harvest_rawjob (sync_status, fetched_at DESC)"),
        ("harvest_raw_active_fetched_idx",   "CREATE INDEX {ifne}harvest_raw_active_fetched_idx  ON harvest_rawjob (is_active, fetched_at DESC)"),
        ("harvest_raw_remote_fetched_idx",   "CREATE INDEX {ifne}harvest_raw_remote_fetched_idx  ON harvest_rawjob (is_remote, fetched_at DESC)"),
        ("harvest_raw_hasdesc_fetched_idx",  "CREATE INDEX {ifne}harvest_raw_hasdesc_fetched_idx ON harvest_rawjob (has_description, fetched_at DESC)"),
        ("harvest_raw_platform_fetched_idx", "CREATE INDEX {ifne}harvest_raw_platform_fetched_idx ON harvest_rawjob (platform_slug, fetched_at DESC)"),
    ]
    if vendor == "postgresql":
        for name, sql_tpl in indexes:
            pg_sql = f"""
                DO $$ BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes
                    WHERE tablename = 'harvest_rawjob' AND indexname = '{name}'
                  ) THEN
                    {sql_tpl.format(ifne="")};
                  END IF;
                END $$
            """
            schema_editor.execute(pg_sql)
    else:
        # SQLite supports CREATE INDEX IF NOT EXISTS (3.9+)
        for _name, sql_tpl in indexes:
            try:
                schema_editor.execute(sql_tpl.format(ifne="IF NOT EXISTS "))
            except Exception:
                pass  # index already exists


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0019_alter_harvestengineconfig_api_stagger_ms_and_more"),
    ]

    operations = [
        # SeparateDatabaseAndState: state gets the field via standard AddField;
        # database uses RunPython so we can do IF NOT EXISTS on Postgres (column
        # may already exist from a manual migration on older deployments).
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
        migrations.RunPython(create_indexes, migrations.RunPython.noop),
    ]
