# Originally added LLMConfig.active_prompt FK → prompts_app.Prompt.
# prompts_app has been removed from INSTALLED_APPS; the FK column is dropped by
# 0024_remove_llmconfig_active_prompt (production) or never created (fresh DB).
# Both operations are no-ops here so Django can build the migration graph without
# needing prompts_app in the installed app list.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0003_llmconfig_llmconfigversion_llmusagelog'),
    ]

    operations = []
