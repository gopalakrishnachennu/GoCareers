from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0024_remove_llmconfig_active_prompt"),
    ]

    operations = [
        migrations.AddField(
            model_name="platformconfig",
            name="nav_layout",
            field=models.CharField(
                choices=[("top", "Top Header"), ("left", "Left Sidebar")],
                default="top",
                help_text="Choose between a top navigation bar or a left sidebar panel.",
                max_length=10,
            ),
        ),
        migrations.AddField(
            model_name="platformconfig",
            name="footer_text",
            field=models.TextField(
                blank=True,
                help_text="Custom footer text shown at the bottom of every page. Leave blank for none.",
            ),
        ),
    ]
