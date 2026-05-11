"""
0050_harvestengineconfig_ops_controls
======================================
Add configurable operation-control fields to HarvestEngineConfig so every
previously-hardcoded constant in the harvest pipeline is tunable from the GUI
without a code deploy.

New fields (all have safe defaults matching the previous hardcoded values):
  full_fetch_cooldown_minutes   — Full Crawl inter-run cooldown (was 120 min)
  backfill_jd_workers           — parallel JD workers (was 8 hardcoded)
  backfill_jd_reset_locks       — auto-clear stale locks before each run
  backfill_jd_include_cold      — extend backfill to COLD/REVIEW rows
  validate_links_include_synced — also validate SYNCED pool jobs
  validate_links_recent_hours   — time window for link validation
  cleanup_inactive_age_days     — Phase 3 purge age threshold (was 7 days)
  cleanup_pending_safe_minutes  — Phase 2 race guard (was 0)
  classify_chunk_limit          — cap rows per classify run (0 = unlimited)
  classify_lock_ttl_minutes     — classify singleton lock lifetime (was 180 min)
  detect_batch_size             — companies per detection run (was 200)
  retry_failed_days             — look-back window for retry-failed (was 7 days)
"""
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0049_platform_registry_metadata_cleanup"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # Full-fetch cooldown
        migrations.AddField(
            model_name="harvestengineconfig",
            name="full_fetch_cooldown_minutes",
            field=models.PositiveSmallIntegerField(
                default=120,
                verbose_name="Full fetch cooldown (minutes)",
                help_text=(
                    "Minimum minutes between two Full Crawl runs. "
                    "Enforced both in the UI and in the task via a cache key so direct API "
                    "calls also respect the limit. Default 120 (2 h)."
                ),
            ),
        ),
        # JD backfill
        migrations.AddField(
            model_name="harvestengineconfig",
            name="backfill_jd_workers",
            field=models.PositiveSmallIntegerField(
                default=4,
                verbose_name="JD backfill parallel workers",
                help_text=(
                    "Concurrent chunk-worker threads for description backfill. "
                    "Hard-capped at 8 internally. Keep ≤ half your Celery pool size."
                ),
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="backfill_jd_reset_locks",
            field=models.BooleanField(
                default=True,
                verbose_name="Reset stale JD locks by default",
                help_text=(
                    "Auto-clear stale backfill locks before each run so crashed "
                    "workers never permanently block re-eligibility."
                ),
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="backfill_jd_include_cold",
            field=models.BooleanField(
                default=False,
                verbose_name="Include COLD / REVIEW jobs in JD backfill",
                help_text=(
                    "When False only PRIORITY (target-country) jobs get JD backfill. "
                    "Enable to also fetch descriptions for COLD and REVIEW_* jobs."
                ),
            ),
        ),
        # Link validation
        migrations.AddField(
            model_name="harvestengineconfig",
            name="validate_links_include_synced",
            field=models.BooleanField(
                default=False,
                verbose_name="Validate links for SYNCED pool jobs too",
                help_text=(
                    "When False only PENDING jobs are validated. Enable to also "
                    "re-validate jobs already promoted to the pool."
                ),
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="validate_links_recent_hours",
            field=models.PositiveSmallIntegerField(
                default=168,
                verbose_name="Validate links — recent hours window",
                help_text="Only check jobs fetched within this many hours (0 = all).",
            ),
        ),
        # Cleanup
        migrations.AddField(
            model_name="harvestengineconfig",
            name="cleanup_inactive_age_days",
            field=models.PositiveSmallIntegerField(
                default=7,
                verbose_name="Cleanup — inactive row max age (days)",
                help_text=(
                    "Inactive rows older than this are purged by Phase 3 of cleanup."
                ),
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="cleanup_pending_safe_minutes",
            field=models.PositiveSmallIntegerField(
                default=10,
                verbose_name="Cleanup — PENDING safe buffer (minutes)",
                help_text=(
                    "Phase 2 only deletes inactive+PENDING rows fetched more than this "
                    "many minutes ago. Prevents a race with in-flight sync tasks."
                ),
            ),
        ),
        # Classify
        migrations.AddField(
            model_name="harvestengineconfig",
            name="classify_chunk_limit",
            field=models.PositiveIntegerField(
                default=0,
                verbose_name="Classify chunk limit (0 = unlimited)",
                help_text=(
                    "Maximum RawJobs processed per classify run. "
                    "Set >0 to prevent timeouts on very large backlogs."
                ),
            ),
        ),
        migrations.AddField(
            model_name="harvestengineconfig",
            name="classify_lock_ttl_minutes",
            field=models.PositiveSmallIntegerField(
                default=180,
                verbose_name="Classify lock TTL (minutes)",
                help_text=(
                    "How long the classify singleton lock is held. Self-expires on "
                    "worker crash. Staff can also clear it via Force Unlock button."
                ),
            ),
        ),
        # Detection
        migrations.AddField(
            model_name="harvestengineconfig",
            name="detect_batch_size",
            field=models.PositiveSmallIntegerField(
                default=200,
                verbose_name="Detection batch size",
                help_text="Companies processed per Run Detection task.",
            ),
        ),
        # Retry-failed
        migrations.AddField(
            model_name="harvestengineconfig",
            name="retry_failed_days",
            field=models.PositiveSmallIntegerField(
                default=7,
                verbose_name="Retry failed — look-back window (days)",
                help_text=(
                    "Retry Failed re-queues company fetch tasks that FAILED within "
                    "this many days."
                ),
            ),
        ),
    ]
