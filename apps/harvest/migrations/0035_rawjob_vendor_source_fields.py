from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0034_harvestopsrun"),
    ]

    operations = [
        migrations.AddField(
            model_name="rawjob",
            name="responsibilities",
            field=models.TextField(blank=True),
        ),
        migrations.AddField(
            model_name="rawjob",
            name="vendor_degree_level",
            field=models.CharField(blank=True, max_length=128),
        ),
        migrations.AddField(
            model_name="rawjob",
            name="vendor_job_category",
            field=models.CharField(blank=True, max_length=128),
        ),
        migrations.AddField(
            model_name="rawjob",
            name="vendor_job_identification",
            field=models.CharField(blank=True, max_length=128),
        ),
        migrations.AddField(
            model_name="rawjob",
            name="vendor_job_schedule",
            field=models.CharField(blank=True, max_length=128),
        ),
        migrations.AddField(
            model_name="rawjob",
            name="vendor_job_shift",
            field=models.CharField(blank=True, max_length=128),
        ),
        migrations.AddField(
            model_name="rawjob",
            name="vendor_location_block",
            field=models.CharField(blank=True, max_length=512),
        ),
    ]
