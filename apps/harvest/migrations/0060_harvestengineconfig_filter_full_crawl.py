from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0059_pre_storage_filter_enabled"),
    ]

    operations = [
        migrations.AddField(
            model_name="harvestengineconfig",
            name="filter_full_crawl",
            field=models.BooleanField(
                default=False,
                verbose_name="Enforce filter during full crawls",
                help_text=(
                    "By default, full-crawl fetches (fetch_all=True) run in filter audit mode — jobs are "
                    "classified but nothing is suppressed, so admin bulk imports are always complete. "
                    "When this flag is True, the selective filter enforces (drops HARD_NO jobs) even "
                    "during full crawls. Enable for selective harvesting from day one."
                ),
            ),
        ),
    ]
