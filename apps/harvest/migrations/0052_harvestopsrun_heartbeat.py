from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0051_fetchbatch_stop_requested"),
    ]

    operations = [
        migrations.AddField(
            model_name="harvestopsrun",
            name="last_heartbeat_at",
            field=models.DateTimeField(
                blank=True,
                db_index=True,
                help_text="Last progress heartbeat written by the worker. Used to detect orphaned RUNNING ops.",
                null=True,
            ),
        ),
    ]
