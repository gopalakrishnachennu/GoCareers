from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from harvest.jarvis import JobJarvis
from harvest.location_resolver import (
    evaluate_rawjob_scope,
    extract_location_candidates,
    is_placeholder_location_value,
    split_multi_location_text,
)
from harvest.models import HarvestEngineConfig, RawJob


class Command(BaseCommand):
    help = "Refetch detail pages for RawJobs whose stored location is a multi-location placeholder."

    def add_arguments(self, parser):
        parser.add_argument("--platform", default="", help="Limit to one platform slug, e.g. jobvite.")
        parser.add_argument("--limit", type=int, default=500, help="Maximum rows to refetch.")
        parser.add_argument("--batch-size", type=int, default=100, help="Progress print interval.")
        parser.add_argument("--provider", action="store_true", help="Allow provider fallback during scope evaluation.")
        parser.add_argument("--dry-run", action="store_true", help="Fetch and report without saving.")

    def handle(self, *args, **options):
        cfg = HarvestEngineConfig.get()
        qs = (
            RawJob.objects
            .filter(original_url__gt="")
            .filter(
                Q(country_source="ambiguous_multi_location")
                | Q(scope_reason__icontains="ambiguous_multi_location")
                | Q(location_raw__icontains="locations")
                | Q(location_raw__icontains="multiple locations")
            )
            .order_by("id")
        )
        if options["platform"]:
            qs = qs.filter(platform_slug=options["platform"])
        limit = max(1, int(options["limit"] or 500))
        qs = qs[:limit]

        jarvis = JobJarvis()
        dry_run = bool(options["dry_run"])
        use_provider = bool(options["provider"])
        batch_size = max(10, int(options["batch_size"] or 100))

        processed = updated = no_location = failed = 0
        self.stdout.write(
            f"Refetching ambiguous locations: limit={limit:,}, platform={options['platform'] or 'all'}, "
            f"provider={use_provider}, dry_run={dry_run}"
        )

        fields = [
            "location_raw",
            "city",
            "state",
            "country",
            "location_candidates",
            "country_code",
            "country_confidence",
            "country_source",
            "country_codes",
            "scope_status",
            "scope_reason",
            "is_priority",
            "last_scope_evaluated_at",
            "raw_payload",
            "updated_at",
        ]

        for raw_job in qs.iterator(chunk_size=batch_size):
            processed += 1
            try:
                data = jarvis.ingest(raw_job.original_url)
                detail_location_raw = data.get("location_raw") or ""
                detail_candidates = data.get("location_candidates") or []
                candidates = list(detail_candidates)
                if detail_location_raw:
                    candidates.extend(split_multi_location_text(detail_location_raw))
                if not candidates:
                    candidates = extract_location_candidates(
                        location_raw=detail_location_raw or raw_job.location_raw or "",
                        city=data.get("city") or raw_job.city or "",
                        state=data.get("state") or raw_job.state or "",
                        country=data.get("country") or raw_job.country or "",
                        vendor_location_block=raw_job.vendor_location_block or "",
                        raw_payload=data.get("raw_payload") or raw_job.raw_payload or {},
                    )

                if not candidates:
                    no_location += 1
                    continue

                if detail_location_raw and not is_placeholder_location_value(detail_location_raw):
                    raw_job.location_raw = detail_location_raw[:512]
                else:
                    raw_job.location_raw = " | ".join(candidates)[:512]
                raw_job.location_candidates = candidates
                if data.get("city"):
                    raw_job.city = str(data.get("city"))[:128]
                if data.get("state"):
                    raw_job.state = str(data.get("state"))[:128]
                if data.get("country"):
                    raw_job.country = str(data.get("country"))[:128]

                payload = dict(raw_job.raw_payload or {})
                payload["location_refetch"] = {
                    "at": timezone.now().isoformat(),
                    "source": data.get("strategy") or data.get("platform_slug") or "jarvis",
                    "location_raw": detail_location_raw,
                    "location_candidates": candidates,
                }
                raw_job.raw_payload = payload

                updates = evaluate_rawjob_scope(raw_job, cfg=cfg, use_provider=use_provider, save=False)
                for field, value in updates.items():
                    setattr(raw_job, field, value)

                if not dry_run:
                    raw_job.save(update_fields=fields)
                updated += 1
            except Exception as exc:
                failed += 1
                if failed <= 5:
                    self.stderr.write(f"Failed raw_job={raw_job.pk}: {type(exc).__name__}: {exc}")

            if processed % batch_size == 0:
                self.stdout.write(f"Processed {processed:,}; updated={updated:,}; no_location={no_location:,}; failed={failed:,}")

        self.stdout.write(self.style.SUCCESS(
            f"Done. processed={processed:,}; updated={updated:,}; no_location={no_location:,}; failed={failed:,}"
        ))
