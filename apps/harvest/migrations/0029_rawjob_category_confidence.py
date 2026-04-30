from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0028_optimize_indexes"),
    ]

    operations = [
        migrations.AddField(
            model_name="rawjob",
            name="category_confidence",
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="rawjob",
            name="enrichment_version",
            field=models.CharField(blank=True, default="v3", max_length=16),
        ),
        migrations.AddField(
            model_name="rawjob",
            name="classification_source",
            field=models.CharField(blank=True, max_length=16),
        ),
    ]
