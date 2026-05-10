from django.db import migrations


ALIAS_SLUGS = {
    "devops-engineer": "devops-cloud",
    "mlai-engineer": "ml-ai-engineer",
    "security-engineer": "cybersecurity",
}


def deactivate_alias_roles(apps, schema_editor):
    MarketingRole = apps.get_model("users", "MarketingRole")
    MarketingRole.objects.filter(slug__in=ALIAS_SLUGS).update(is_active=False)


def reactivate_alias_roles(apps, schema_editor):
    MarketingRole = apps.get_model("users", "MarketingRole")
    MarketingRole.objects.filter(slug__in=ALIAS_SLUGS).update(is_active=True)


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0027_consultantprofile_routing_preferences"),
    ]

    operations = [
        migrations.RunPython(deactivate_alias_roles, reverse_code=reactivate_alias_roles),
    ]
