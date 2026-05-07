from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0042_add_geocoding_provider_token"),
    ]

    operations = [
        migrations.AddField(
            model_name="rawjob",
            name="country_codes",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="All resolved ISO country codes from location_candidates.",
            ),
        ),
        migrations.AddField(
            model_name="rawjob",
            name="location_candidates",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="All vendor/detail locations for multi-location postings.",
            ),
        ),
    ]
