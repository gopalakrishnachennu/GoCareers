from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0026_expand_marketing_role_fallbacks"),
    ]

    operations = [
        migrations.AddField(
            model_name="consultantprofile",
            name="preferred_seniority_levels",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="Preferred seniority bands for job routing, e.g. ['mid', 'senior'].",
            ),
        ),
        migrations.AddField(
            model_name="consultantprofile",
            name="work_countries",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="Preferred work countries for job routing, e.g. ['United States', 'Canada'].",
            ),
        ),
    ]
