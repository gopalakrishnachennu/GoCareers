from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from harvest.models import HarvestFilterSnapshot, RawJob
from harvest.role_filter import COLD, NO_MATCH, classify_title


class Command(BaseCommand):
    help = "Classify existing RawJobs with the current selective harvest role filter."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Report counts without updating rows.")
        parser.add_argument("--limit", type=int, default=0, help="Maximum rows to scan. 0 means all.")
        parser.add_argument(
            "--only-unclassified",
            action="store_true",
            help="Only classify rows where filter_decision is NULL.",
        )

    def handle(self, *args, **options):
        dry_run = bool(options["dry_run"])
        limit = max(0, int(options["limit"] or 0))
        snapshot = HarvestFilterSnapshot.create_snapshot(notes="classify_existing_rawjobs")
        categories = snapshot.get_categories()
        hard_negatives = snapshot.get_hard_negatives()

        qs = RawJob.objects.select_related("platform_label").order_by("pk")
        if options["only_unclassified"]:
            qs = qs.filter(filter_decision__isnull=True)
        if limit:
            qs = qs[:limit]

        counts: dict[str, int] = {}
        updates: list[RawJob] = []
        for raw_job in qs.iterator(chunk_size=1000):
            label = raw_job.platform_label
            custom_phrases = label.custom_include_phrases if label else []
            result = classify_title(
                title=raw_job.title,
                department=raw_job.department,
                categories=categories,
                hard_negatives=hard_negatives,
                custom_phrases=custom_phrases or [],
                snapshot_id=str(snapshot.snapshot_id),
            )
            counts[result.decision] = counts.get(result.decision, 0) + 1
            if dry_run:
                continue
            raw_job.role_category = result.category
            raw_job.filter_decision = result.decision
            raw_job.filter_reason = result.reason[:512]
            raw_job.filter_snapshot_id = snapshot.snapshot_id
            raw_job.is_cold = result.decision in {COLD, NO_MATCH}
            raw_job.jd_fetch_skipped = result.decision in {COLD, NO_MATCH}
            updates.append(raw_job)
            if len(updates) >= 1000:
                with transaction.atomic():
                    RawJob.objects.bulk_update(
                        updates,
                        [
                            "role_category",
                            "filter_decision",
                            "filter_reason",
                            "filter_snapshot_id",
                            "is_cold",
                            "jd_fetch_skipped",
                        ],
                    )
                updates.clear()

        if updates and not dry_run:
            with transaction.atomic():
                RawJob.objects.bulk_update(
                    updates,
                    [
                        "role_category",
                        "filter_decision",
                        "filter_reason",
                        "filter_snapshot_id",
                        "is_cold",
                        "jd_fetch_skipped",
                    ],
                )

        self.stdout.write(self.style.SUCCESS(f"snapshot={snapshot.snapshot_id} dry_run={dry_run} counts={counts}"))
