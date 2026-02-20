from django.db import migrations, models
import uuid


def backfill_generation_id(apps, schema_editor):
    ResumeDraft = apps.get_model("resumes", "ResumeDraft")
    for draft in ResumeDraft.objects.filter(generation_id__isnull=True):
        draft.generation_id = uuid.uuid4()
        draft.save(update_fields=["generation_id"])


class Migration(migrations.Migration):
    dependencies = [
        ("resumes", "0011_resumetemplatepack"),
    ]

    operations = [
        migrations.AddField(
            model_name="resumedraft",
            name="generation_id",
            field=models.UUIDField(null=True, editable=False),
        ),
        migrations.RunPython(backfill_generation_id, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="resumedraft",
            name="generation_id",
            field=models.UUIDField(default=uuid.uuid4, unique=True, editable=False),
        ),
    ]
