from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("jobs", "0018_job_classification_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="job",
            name="auto_marketing_role_slugs",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="Auto-assigned MarketingRole slugs from harvested domain routing. Manual role edits stay in the M2M.",
            ),
        ),
    ]
