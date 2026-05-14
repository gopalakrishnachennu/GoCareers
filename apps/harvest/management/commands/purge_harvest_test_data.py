from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from django.db.models import Q

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
    help = "Dry-run or purge smoke/test harvest rows without touching production raw jobs by default."

    CONFIRM_PHRASE = "DELETE_HARVEST_TEST_DATA"
    CONFIRM_ALL_PHRASE = "DELETE_ALL_HARVEST_STAGING_DATA"

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
        parser.add_argument(
            "--all-harvest-data",
            action="store_true",
            help=(
                "Danger zone: purge all harvest staging/ops tables. Requires "
                f"--confirm {self.CONFIRM_ALL_PHRASE!r}."
            ),
        )

    def handle(self, *args, **options):
        execute = options["execute"]
        confirm = options["confirm"]
        purge_all = options["all_harvest_data"]

        counts = self._counts(purge_all=purge_all)
        scope = "ALL harvest staging data" if purge_all else "smoke/test RawJob rows only"
        self.stdout.write(f"Harvest purge scope: {scope}")
        for label, count in counts.items():
            self.stdout.write(f"  {label}: {count:,}")

        self.stdout.write("")
        self.stdout.write("Preserved: users, companies, resumes, core jobs, platform config, filter categories.")

        if not execute:
            self.stdout.write(self.style.WARNING("Dry-run only. Add --execute with the confirmation phrase to purge."))
            return

        if purge_all:
            if confirm != self.CONFIRM_ALL_PHRASE:
                raise CommandError(f"Refusing to purge all harvest data. Pass --confirm {self.CONFIRM_ALL_PHRASE!r}.")
            quoted_tables = ", ".join(connection.ops.quote_name(table) for table in self.TABLES)
            with connection.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {quoted_tables} RESTART IDENTITY CASCADE")
            self.stdout.write(self.style.SUCCESS("All harvest staging data purged."))
            return

        if confirm != self.CONFIRM_PHRASE:
            raise CommandError(f"Refusing to purge. Pass --confirm {self.CONFIRM_PHRASE!r}.")

        raw_ids = list(RawJob.objects.filter(is_test_run=True).values_list("id", flat=True))
        if raw_ids:
            HarvestSkippedTitle.objects.filter(raw_job_id__in=raw_ids).delete()
            RawJob.objects.filter(id__in=raw_ids).delete()

        self.stdout.write(self.style.SUCCESS("Harvest smoke/test raw rows purged."))

    def _counts(self, *, purge_all: bool = False):
        if purge_all:
            raw_qs = RawJob.objects.all()
            skipped_qs = HarvestSkippedTitle.objects.all()
        else:
            raw_qs = RawJob.objects.filter(is_test_run=True)
            skipped_qs = HarvestSkippedTitle.objects.filter(raw_job__is_test_run=True)
        return {
            "RawJob": raw_qs.count(),
            "RawJobPayloadSnapshot": RawJobPayloadSnapshot.objects.filter(raw_job__in=raw_qs).count(),
            "RawJobDuplicatePair": RawJobDuplicatePair.objects.filter(
                Q(primary__in=raw_qs) | Q(duplicate__in=raw_qs)
            ).count(),
            "HarvestSkippedTitle": skipped_qs.count(),
            "CompanyFetchRun": CompanyFetchRun.objects.count() if purge_all else 0,
            "FetchBatch": FetchBatch.objects.count() if purge_all else 0,
            "HarvestOpsRun": HarvestOpsRun.objects.count() if purge_all else 0,
        }
