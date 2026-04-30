from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("messaging", "0002_messaging_thread_org_and_message_fields"),
        ("core", "0023_remove_organisation"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="thread",
            name="organisation",
        ),
    ]
