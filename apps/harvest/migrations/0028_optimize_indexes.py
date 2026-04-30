"""
Optimization migration:
- Add index on original_url (used in backfill WHERE clauses)
- Add compound index (has_description, jd_backfill_locked_at) for backfill eligibility queries
- Remove unused indexes on education_required and quality_score (never populated)
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0027_harvestengineconfig_resume_jd_thresholds"),
    ]

    operations = [
        # Index original_url — used in backfill dedup and URL lookups
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["original_url"], name="harvest_raw_origurl_idx"),
        ),
        # Compound index for fast backfill eligibility: WHERE has_description=FALSE AND (lock IS NULL OR lock < stale)
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(
                fields=["has_description", "jd_backfill_locked_at"],
                name="harvest_raw_has_desc_lock_idx",
            ),
        ),
        # Remove write-bloat indexes that were never queried
        migrations.RemoveIndex(
            model_name="rawjob",
            name="harvest_raw_educati_3988c6_idx",
        ),
        migrations.RemoveIndex(
            model_name="rawjob",
            name="harvest_raw_quality_df4f12_idx",
        ),
    ]
