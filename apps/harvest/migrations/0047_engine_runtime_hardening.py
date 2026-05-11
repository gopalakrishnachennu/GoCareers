from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0046_rawjobpayloadsnapshot"),
    ]

    operations = [
        migrations.AlterField(
            model_name="harvestopsrun",
            name="operation",
            field=models.CharField(
                choices=[
                    ("detect_platforms", "Detect platforms"),
                    ("backfill_jd", "Backfill JD"),
                    ("validate_urls", "Validate live links"),
                    ("sync_pool", "Sync to vet pool"),
                    ("cleanup", "Cleanup harvested"),
                    ("classify", "Classify raw jobs"),
                    ("classify_domains", "Classify domains"),
                    ("llm_classify", "LLM classify (second pass)"),
                    ("evaluate_scope", "Evaluate RawJob scope"),
                    ("backfill_roles", "Backfill marketing roles"),
                    ("refetch_locations", "Refetch ambiguous locations"),
                    ("backfill_enrichment", "Backfill enrichment"),
                    ("config_failure", "Config read failure"),
                ],
                db_index=True,
                max_length=64,
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="ready_stage_min_confidence",
            field=models.FloatField(
                default=0.55,
                help_text="Minimum effective classification confidence (0-1) required before a RawJob is considered READY in pipeline analytics and queue counts.",
                verbose_name="Ready stage minimum confidence",
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="legacy_hash_bridge_enabled",
            field=models.BooleanField(
                default=True,
                help_text="Temporarily reconcile old SHA256 url_hash rows during upsert. Turn off after the historical hash migration/backfill has completed.",
                verbose_name="Legacy SHA256 URL hash bridge",
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="jd_backfill_lock_stale_minutes",
            field=models.PositiveSmallIntegerField(
                default=15,
                help_text="If a JD backfill worker crashes, locks older than this are reclaimed. Keep above the longest normal single-job fetch duration.",
                verbose_name="JD backfill stale lock minutes",
            ),
        ),
    ]
