from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0022_recruiter_designation_messaging"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="broadcastmessage",
            name="organisation",
        ),
        # Keep Organisation in migration state for legacy cross-app migrations
        # (users/messaging) that still reference core.organisation historically.
        # Runtime code no longer uses this model.
    ]
