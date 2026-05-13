from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from harvest.models import (
    CompanyFetchRun,
    FetchBatch,
    HarvestOpsRun,
    HarvestSkippedTitle,
    RawJob,
    RawJobDuplicatePair,
    RawJobPayloadSnapshot,
)


class Command(BaseCommand):
    help = "Dry-run or purge harvest staging/ops data for selective-harvest testing."

    CONFIRM_PHRASE = "DELETE_HARVEST_TEST_DATA"

    TABLES = [
        "harvest_harvestskippedtitle",
        "harvest_rawjobduplicatepair",
        "harvest_rawjobpayloadsnapshot",
        "harvest_rawjob",
        "harvest_companyfetchrun",
        "harvest_fetchbatch",
        "harvest_harvestopsrun",
    ]

    def add_arguments(self, parser):
        parser.add_argument(
            "--execute",
            action="store_true",
            help="Actually purge data. Without this flag the command is a dry-run.",
        )
        parser.add_argument(
            "--confirm",
            default="",
            help=f"Required phrase for execution: {self.CONFIRM_PHRASE}",
        )

    def handle(self, *args, **options):
        execute = options["execute"]
        confirm = options["confirm"]

        counts = self._counts()
        self.stdout.write("Harvest test-data purge scope:")
        for label, count in counts.items():
            self.stdout.write(f"  {label}: {count:,}")

        self.stdout.write("")
        self.stdout.write("Preserved: users, companies, resumes, core jobs, platform config, filter categories.")

        if not execute:
            self.stdout.write(self.style.WARNING("Dry-run only. Add --execute with the confirmation phrase to purge."))
            return

        if confirm != self.CONFIRM_PHRASE:
            raise CommandError(f"Refusing to purge. Pass --confirm {self.CONFIRM_PHRASE!r}.")

        quoted_tables = ", ".join(connection.ops.quote_name(table) for table in self.TABLES)
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {quoted_tables} RESTART IDENTITY CASCADE")

        self.stdout.write(self.style.SUCCESS("Harvest test data purged."))

    def _counts(self):
        return {
            "RawJob": RawJob.objects.count(),
            "RawJobPayloadSnapshot": RawJobPayloadSnapshot.objects.count(),
            "RawJobDuplicatePair": RawJobDuplicatePair.objects.count(),
            "HarvestSkippedTitle": HarvestSkippedTitle.objects.count(),
            "CompanyFetchRun": CompanyFetchRun.objects.count(),
            "FetchBatch": FetchBatch.objects.count(),
            "HarvestOpsRun": HarvestOpsRun.objects.count(),
        }
