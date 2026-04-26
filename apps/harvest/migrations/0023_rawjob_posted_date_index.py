from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0022_rename_long_indexes"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["posted_date"], name="harvest_raw_posted_idx"),
        ),
    ]
