from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0025_platformconfig_nav_layout_footer_text"),
    ]

    operations = [
        migrations.AddField(
            model_name="platformconfig",
            name="color_theme",
            field=models.CharField(
                choices=[
                    ("indigo",  "Indigo"),
                    ("violet",  "Violet"),
                    ("blue",    "Blue"),
                    ("emerald", "Emerald"),
                    ("teal",    "Teal"),
                    ("rose",    "Rose"),
                    ("amber",   "Amber"),
                    ("slate",   "Slate"),
                ],
                default="indigo",
                help_text="Primary brand color used across the entire platform.",
                max_length=20,
            ),
        ),
    ]
