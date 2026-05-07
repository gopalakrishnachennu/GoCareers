from __future__ import annotations

from collections import Counter

from django.core.management.base import BaseCommand
from django.db import transaction

from harvest.location_resolver import evaluate_rawjob_scope
from harvest.models import HarvestEngineConfig, RawJob


class Command(BaseCommand):
    help = "Resolve RawJob country/scope fields for scoped harvest processing."

    def add_arguments(self, parser):
        parser.add_argument("--all", action="store_true", help="Evaluate all RawJobs.")
        parser.add_argument("--platform", default="", help="Limit to one platform slug.")
        parser.add_argument("--limit", type=int, default=0, help="Maximum rows to process.")
        parser.add_argument("--batch-size", type=int, default=1000, help="Bulk update batch size.")
        parser.add_argument("--only-unscoped", action="store_true", help="Only rows with blank/UNSCOPED scope.")
        parser.add_argument("--only-unknown", action="store_true", help="Only rows with missing country_code.")
        parser.add_argument("--provider", action="store_true", help="Allow external provider fallback, guarded by engine quota.")
        parser.add_argument("--dry-run", action="store_true", help="Do not write updates.")

    def handle(self, *args, **options):
        if not options["all"] and not options["platform"] and not options["only_unscoped"] and not options["only_unknown"]:
            self.stderr.write("Refusing broad default. Pass --all, --platform, --only-unscoped, or --only-unknown.")
            return

        cfg = HarvestEngineConfig.get()
        qs = RawJob.objects.all().order_by("id")
        if options["platform"]:
            qs = qs.filter(platform_slug=options["platform"])
        if options["only_unscoped"]:
            qs = qs.filter(scope_status__in=["", RawJob.ScopeStatus.UNSCOPED])
        if options["only_unknown"]:
            qs = qs.filter(country_code="")
        if options["limit"] and options["limit"] > 0:
            qs = qs[: options["limit"]]

        batch_size = max(100, min(int(options["batch_size"] or 1000), 5000))
        use_provider = bool(options["provider"])
        dry_run = bool(options["dry_run"])

        self.stdout.write(
            f"Evaluating RawJob scope: batch={batch_size:,}, provider={use_provider}, dry_run={dry_run}, "
            f"targets={','.join(cfg.get_target_countries())}"
        )

        counters = Counter()
        buffer: list[RawJob] = []
        fields = [
            "country_code",
            "country_confidence",
            "country_source",
            "scope_status",
            "scope_reason",
            "is_priority",
            "last_scope_evaluated_at",
            "country",
            "state",
            "city",
        ]

        def flush():
            if not buffer:
                return
            if not dry_run:
                with transaction.atomic():
                    RawJob.objects.bulk_update(buffer, fields, batch_size=batch_size)
            buffer.clear()

        processed = 0
        iterator = qs.iterator(chunk_size=batch_size) if not isinstance(qs, list) else iter(qs)
        for raw_job in iterator:
            updates = evaluate_rawjob_scope(raw_job, cfg=cfg, use_provider=use_provider, save=False)
            for field, value in updates.items():
                setattr(raw_job, field, value)
            counters[updates.get("scope_status") or "UNSCOPED"] += 1
            counters[f"country:{updates.get('country_code') or 'UNKNOWN'}"] += 1
            buffer.append(raw_job)
            processed += 1
            if len(buffer) >= batch_size:
                flush()
                self.stdout.write(f"Processed {processed:,}...")

        flush()

        self.stdout.write(self.style.SUCCESS(f"Scope evaluation complete: {processed:,} rows"))
        for key, value in counters.most_common():
            self.stdout.write(f"  {key}: {value:,}")
