from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0022_recruiter_designation_messaging"),
        ("users", "0023_remove_user_organisation"),
        ("messaging", "0003_remove_thread_organisation"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="broadcastmessage",
            name="organisation",
        ),
        migrations.DeleteModel(
            name="Organisation",
        ),
    ]
