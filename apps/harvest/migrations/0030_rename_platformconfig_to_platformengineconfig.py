from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0029_rawjob_category_confidence"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="PlatformConfig",
            new_name="PlatformEngineConfig",
        ),
    ]
