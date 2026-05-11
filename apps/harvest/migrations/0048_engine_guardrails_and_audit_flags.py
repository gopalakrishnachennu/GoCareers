from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0047_engine_runtime_hardening"),
    ]

    operations = [
        migrations.AddField(
            model_name="companyfetchrun",
            name="is_test_run",
            field=models.BooleanField(
                db_index=True,
                default=False,
                help_text="True when this run intentionally capped writes for a smoke/test harvest.",
            ),
        ),
        migrations.AddField(
            model_name="companyfetchrun",
            name="jobs_cap_applied",
            field=models.BooleanField(
                default=False,
                help_text="True when the platform returned more jobs than this run was allowed to write.",
            ),
        ),
        migrations.AddField(
            model_name="companyplatformlabel",
            name="portal_consecutive_failures",
            field=models.PositiveSmallIntegerField(
                db_index=True,
                default=0,
                help_text="Consecutive failed health checks. Portal is marked down only after the configured threshold.",
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="geocoding_hourly_limit",
            field=models.PositiveIntegerField(
                default=1000,
                help_text="Hard stop for provider requests per hour so one bad harvest cannot burn the month in a spike.",
                verbose_name="Provider hourly hard limit",
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="geocoding_warning_pct",
            field=models.PositiveSmallIntegerField(
                default=80,
                help_text="Log a warning when monthly or hourly provider usage reaches this percentage.",
                verbose_name="Provider warning threshold percent",
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="portal_health_failure_threshold",
            field=models.PositiveSmallIntegerField(
                default=2,
                help_text="Consecutive failed portal checks required before a company portal is marked down. Prevents one transient 5xx from locking out a company.",
                verbose_name="Portal health failure threshold",
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="rescope_on_target_country_change",
            field=models.BooleanField(
                default=True,
                help_text="Queue a safe background pass over cold/review RawJobs when the target-country list changes so newly enabled markets do not stay cold forever.",
                verbose_name="Re-scope cold jobs when target countries change",
            ),
        ),
    ]
