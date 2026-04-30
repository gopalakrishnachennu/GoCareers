# Drops LLMConfig.active_prompt FK — prompts_app has been removed.
# 0004 is a no-op on fresh DBs so the column may or may not exist; the production
# column is left as an orphaned DB column (harmless — Django ORM no longer references it).
# This migration is kept as a no-op to preserve the dependency chain.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0023_remove_organisation"),
    ]

    operations = []
