from django.core.management.base import BaseCommand

from harvest.models import RawJob, RawJobPayloadSnapshot
from harvest.payload_archive import capture_rawjob_payload_snapshot


class Command(BaseCommand):
    help = "Backfill RawJobPayloadSnapshot rows from existing RawJob.raw_payload values."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=1000)
        parser.add_argument("--start-id", type=int, default=0)

    def handle(self, *args, **options):
        limit = max(1, int(options["limit"]))
        start_id = max(0, int(options["start_id"]))
        qs = (
            RawJob.objects
            .filter(pk__gt=start_id)
            .exclude(raw_payload={})
            .only("id", "raw_payload", "platform_slug", "original_url")
            .order_by("pk")[:limit]
        )
        scanned = archived = 0
        last_id = start_id
        for raw_job in qs:
            scanned += 1
            last_id = raw_job.pk
            snapshot = capture_rawjob_payload_snapshot(
                raw_job,
                payload=raw_job.raw_payload or {},
                payload_kind=RawJobPayloadSnapshot.PayloadKind.BACKFILL,
                source_url=raw_job.original_url,
                platform_slug=raw_job.platform_slug,
                schema_version="legacy-raw-payload-v1",
                source_metadata={"ingest": "archive_existing_raw_payloads"},
            )
            if snapshot:
                archived += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Scanned {scanned}; archived/deduped {archived}; last_id={last_id}"
            )
        )
