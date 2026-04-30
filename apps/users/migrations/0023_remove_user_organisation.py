from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0022_phase2_3_embeddings_analytics"),
        ("core", "0023_remove_organisation"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="user",
            name="organisation",
        ),
    ]
