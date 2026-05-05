from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0039_rawjob_job_domain"),
    ]

    operations = [
        migrations.AddField(
            model_name="rawjob",
            name="job_domain_candidates",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="Ordered candidate MarketingRole slugs considered during domain routing.",
            ),
        ),
    ]
