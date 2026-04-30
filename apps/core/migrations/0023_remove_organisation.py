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
        migrations.DeleteModel(
            name="Organisation",
        ),
    ]
