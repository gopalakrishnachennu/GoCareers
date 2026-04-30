# Originally removed LLMConfig.active_prompt. 0004 is now a no-op (prompts_app
# removed from INSTALLED_APPS), so this migration is also a no-op to keep the
# dependency chain intact without referencing a field that was never added.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0018_messaging_thread_org_and_message_fields'),
    ]

    operations = []
