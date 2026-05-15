from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0061_fetchbatch_is_full_crawl"),
    ]

    operations = [
        migrations.CreateModel(
            name="VetGateConfig",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "allow_unknown_country",
                    models.BooleanField(
                        default=True,
                        help_text=(
                            "Include jobs whose country could not be determined. "
                            "Turn off to sync only confirmed target-country jobs."
                        ),
                        verbose_name="Allow REVIEW_UNKNOWN_COUNTRY",
                    ),
                ),
                (
                    "allow_possible_filter",
                    models.BooleanField(
                        default=True,
                        help_text=(
                            "Include jobs where the pre-storage filter scored POSSIBLE "
                            "(not just STRONG). Turn off for highest-confidence only."
                        ),
                        verbose_name="Allow POSSIBLE filter decision",
                    ),
                ),
                (
                    "require_description",
                    models.BooleanField(
                        default=True,
                        help_text="Block jobs that have no fetched JD. Highly recommended.",
                        verbose_name="Require job description",
                    ),
                ),
                (
                    "min_word_count",
                    models.PositiveSmallIntegerField(
                        default=80,
                        help_text="Jobs with fewer words in the description are blocked. Default: 80.",
                        verbose_name="Minimum word count",
                    ),
                ),
                (
                    "min_char_count",
                    models.PositiveSmallIntegerField(
                        default=400,
                        help_text="Jobs whose description text is shorter than this are blocked. Default: 400.",
                        verbose_name="Minimum character count",
                    ),
                ),
                (
                    "auto_lane_min_vet_priority",
                    models.FloatField(
                        default=0.75,
                        help_text="Jobs scoring at or above this go straight to AUTO lane (no human review needed). 0.0–1.0.",
                        verbose_name="AUTO lane: min vet priority score",
                    ),
                ),
                (
                    "auto_lane_min_data_quality",
                    models.FloatField(
                        default=0.72,
                        help_text="Data-quality score threshold for AUTO lane. 0.0–1.0.",
                        verbose_name="AUTO lane: min data quality score",
                    ),
                ),
                (
                    "auto_lane_min_trust",
                    models.FloatField(
                        default=0.70,
                        help_text="Trust score threshold for AUTO lane. 0.0–1.0.",
                        verbose_name="AUTO lane: min trust score",
                    ),
                ),
                (
                    "blocked_domains",
                    models.JSONField(
                        blank=True,
                        default=list,
                        help_text=(
                            'JSON list of job_domain slugs to exclude from sync. '
                            'Example: ["hr-recruiter", "sales", "finance-accounting"]. '
                            "Jobs whose job_domain matches any entry are blocked regardless of other scores."
                        ),
                        verbose_name="Blocked job domains",
                    ),
                ),
                (
                    "default_chunk_size",
                    models.PositiveSmallIntegerField(
                        default=500,
                        help_text="How many RawJobs to process per database page during sync. 50–2000.",
                        verbose_name="Default chunk size",
                    ),
                ),
                (
                    "auto_sync_after_harvest",
                    models.BooleanField(
                        default=False,
                        help_text="Automatically trigger a qualified sync when a full-crawl batch completes.",
                        verbose_name="Auto-sync after full harvest",
                    ),
                ),
            ],
            options={
                "verbose_name": "Vet Gate Config",
                "verbose_name_plural": "Vet Gate Config",
            },
        ),
    ]
