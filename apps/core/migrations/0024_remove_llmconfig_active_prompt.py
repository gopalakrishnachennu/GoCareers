from django.db import migrations


class Migration(migrations.Migration):
    """Drop LLMConfig.active_prompt FK — prompts_app has been removed."""

    dependencies = [
        ("core", "0023_remove_organisation"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="llmconfig",
            name="active_prompt",
        ),
    ]
