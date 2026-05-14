from datetime import timedelta

from django.db import migrations, models
import django.db.models.deletion


def mark_historical_test_rows(apps, schema_editor):
    RawJob = apps.get_model("harvest", "RawJob")
    CompanyFetchRun = apps.get_model("harvest", "CompanyFetchRun")

    test_runs = (
        CompanyFetchRun.objects.filter(is_test_run=True)
        .exclude(started_at__isnull=True)
        .order_by("started_at")
    )
    for run in test_runs.iterator(chunk_size=200):
        started_at = run.started_at
        completed_at = run.completed_at or run.started_at
        if not started_at or not completed_at:
            continue
        window_start = started_at - timedelta(minutes=5)
        window_end = completed_at + timedelta(minutes=5)
        qs = RawJob.objects.filter(
            platform_label_id=run.label_id,
            fetched_at__gte=window_start,
            fetched_at__lte=window_end,
        )
        updates = {"is_test_run": True}
        if run.batch_id:
            updates["fetch_batch_id"] = run.batch_id
        qs.update(**updates)


def unmark_historical_test_rows(apps, schema_editor):
    RawJob = apps.get_model("harvest", "RawJob")
    RawJob.objects.filter(is_test_run=True).update(is_test_run=False, fetch_batch_id=None)


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0055_alter_harvestengineconfig_api_stagger_ms_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="rawjob",
            name="fetch_batch",
            field=models.ForeignKey(
                blank=True,
                help_text="Batch that produced or last refreshed this raw job.",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="raw_jobs",
                to="harvest.fetchbatch",
            ),
        ),
        migrations.AddField(
            model_name="rawjob",
            name="is_test_run",
            field=models.BooleanField(
                db_index=True,
                default=False,
                help_text="True for smoke/test harvest rows that must stay out of production backlog counts.",
            ),
        ),
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["is_test_run"], name="harvest_raw_is_test_b44001_idx"),
        ),
        migrations.AddIndex(
            model_name="rawjob",
            index=models.Index(fields=["is_test_run", "-fetched_at"], name="harvest_raw_test_fetched_idx"),
        ),
        migrations.RunPython(mark_historical_test_rows, unmark_historical_test_rows),
    ]
