from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0060_harvestengineconfig_filter_full_crawl"),
    ]

    operations = [
        migrations.AddField(
            model_name="fetchbatch",
            name="is_full_crawl",
            field=models.BooleanField(
                default=False,
                help_text=(
                    "True when the batch was launched with fetch_all=True — "
                    "every company fetches its entire board, ignoring the since_hours window. "
                    "False = incremental (last 25 h only)."
                ),
            ),
        ),
    ]
