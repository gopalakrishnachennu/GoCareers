"""Rename two indexes whose names exceeded Django's 30-char limit."""
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0021_rawjob_index_state_sync"),
    ]

    operations = [
        migrations.RenameIndex(
            model_name="rawjob",
            old_name="harvest_raw_hasdesc_fetched_idx",
            new_name="harvest_raw_hd_fetched_idx",
        ),
        migrations.RenameIndex(
            model_name="rawjob",
            old_name="harvest_raw_platform_fetched_idx",
            new_name="harvest_raw_plat_fetched_idx",
        ),
    ]
