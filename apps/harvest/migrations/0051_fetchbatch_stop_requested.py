from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0050_harvestengineconfig_ops_controls"),
    ]

    operations = [
        migrations.AddField(
            model_name="fetchbatch",
            name="stop_requested",
            field=models.BooleanField(
                default=False,
                help_text="If True, queued tasks for this batch will exit immediately on pickup.",
            ),
        ),
    ]
