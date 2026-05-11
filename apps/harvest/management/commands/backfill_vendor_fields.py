"""
backfill_vendor_fields
======================
Re-parse vendor-native fields (vendor_job_identification, vendor_job_category,
vendor_location_block, vendor_job_schedule, vendor_degree_level) for existing
RawJob rows whose raw_payload is already stored.

These fields were added to the harvesters after the initial bulk harvest, so
historical rows have empty vendor fields even though the data is in raw_payload.
This command extracts them without re-fetching from any external source.

Usage:
    python manage.py backfill_vendor_fields
    python manage.py backfill_vendor_fields --platform workday
    python manage.py backfill_vendor_fields --limit 5000 --dry-run
"""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q


PLATFORM_EXTRACTORS: dict[str, callable] = {}


def _register(slug: str):
    def decorator(fn):
        PLATFORM_EXTRACTORS[slug] = fn
        return fn
    return decorator


@_register("workday")
def _extract_workday(payload: dict) -> dict:
    job = payload.get("job") or payload
    dept = (
        (job.get("jobFamilyGroup") or [{}])[0].get("jobFamilyGroupName", "")
        if isinstance(job.get("jobFamilyGroup"), list)
        else str(job.get("jobFamilyGroup") or "")
    )
    ext_id = ""
    bullet = job.get("bulletFields") or []
    if bullet:
        ext_id = str(bullet[0])
    loc_block = job.get("locationsText") or ""
    sched = str(job.get("jobScheduleType") or "")
    degree = ""
    mq = job.get("minimumQualifications")
    if isinstance(mq, dict):
        degree = mq.get("descriptor", "")
    elif isinstance(mq, str):
        degree = mq
    degree = degree or str(job.get("educationLevel") or job.get("degreeLevel") or "")
    return {
        "vendor_job_identification": ext_id[:128],
        "vendor_job_category": dept[:128],
        "vendor_location_block": loc_block[:512],
        "vendor_job_schedule": sched[:128],
        "vendor_degree_level": degree[:128],
    }


@_register("greenhouse")
def _extract_greenhouse(payload: dict) -> dict:
    ext_id = str(payload.get("id") or payload.get("requisition_id") or "")
    dept = (payload.get("departments") or [{}])
    dept_name = dept[0].get("name", "") if dept else ""
    loc_block = ", ".join(
        (loc.get("name") or "") for loc in (payload.get("offices") or []) if loc.get("name")
    )
    return {
        "vendor_job_identification": ext_id[:128],
        "vendor_job_category": dept_name[:128],
        "vendor_location_block": loc_block[:512],
        "vendor_job_schedule": "",
        "vendor_degree_level": "",
    }


@_register("lever")
def _extract_lever(payload: dict) -> dict:
    cats = payload.get("categories") or {}
    return {
        "vendor_job_identification": str(payload.get("id") or "")[:128],
        "vendor_job_category": str(cats.get("department") or "")[:128],
        "vendor_location_block": str(cats.get("location") or "")[:512],
        "vendor_job_schedule": str(cats.get("commitment") or "")[:128],
        "vendor_degree_level": "",
    }


@_register("ashby")
def _extract_ashby(payload: dict) -> dict:
    dept = (payload.get("department") or {}).get("name") or ""
    loc = payload.get("locationName") or payload.get("location") or ""
    if isinstance(loc, dict):
        loc = loc.get("name") or ""
    sched = payload.get("employmentType") or ""
    return {
        "vendor_job_identification": str(payload.get("id") or "")[:128],
        "vendor_job_category": str(dept)[:128],
        "vendor_location_block": str(loc)[:512],
        "vendor_job_schedule": str(sched)[:128],
        "vendor_degree_level": "",
    }


@_register("smartrecruiters")
def _extract_smartrecruiters(payload: dict) -> dict:
    dept = (payload.get("department") or {}).get("label") or ""
    loc = payload.get("location") or {}
    loc_block = ", ".join(
        str(loc.get(k) or "") for k in ("city", "region", "country") if loc.get(k)
    )
    return {
        "vendor_job_identification": str(payload.get("id") or "")[:128],
        "vendor_job_category": str(dept)[:128],
        "vendor_location_block": loc_block[:512],
        "vendor_job_schedule": str((payload.get("typeOfEmployment") or {}).get("label") or "")[:128],
        "vendor_degree_level": "",
    }


VENDOR_FIELDS = [
    "vendor_job_identification",
    "vendor_job_category",
    "vendor_location_block",
    "vendor_job_schedule",
    "vendor_degree_level",
]


class Command(BaseCommand):
    help = "Backfill vendor-native fields from stored raw_payload without re-fetching"

    def add_arguments(self, parser):
        parser.add_argument("--platform", default="", help="Limit to one platform slug.")
        parser.add_argument("--limit", type=int, default=0, help="Max rows to process (0=all).")
        parser.add_argument("--batch-size", type=int, default=500, help="Bulk update batch size.")
        parser.add_argument("--only-empty", action="store_true", default=True,
                            help="Only rows with all vendor fields empty (default True).")
        parser.add_argument("--overwrite", action="store_true", default=False,
                            help="Also overwrite rows that already have vendor fields.")
        parser.add_argument("--dry-run", action="store_true", default=False)

    def handle(self, *args, **options):
        from harvest.models import RawJob

        platform = options["platform"].strip()
        limit = int(options["limit"] or 0)
        batch_size = max(50, min(int(options["batch_size"] or 500), 5000))
        overwrite = options["overwrite"]
        dry_run = options["dry_run"]

        supported = list(PLATFORM_EXTRACTORS)
        if platform and platform not in supported:
            self.stderr.write(f"Platform {platform!r} not supported. Supported: {supported}")
            return

        qs = RawJob.objects.filter(raw_payload__isnull=False).exclude(raw_payload={})
        if platform:
            qs = qs.filter(platform_slug=platform)
        else:
            qs = qs.filter(platform_slug__in=supported)
        if not overwrite:
            qs = qs.filter(
                Q(vendor_job_identification="") | Q(vendor_job_category="") | Q(vendor_location_block="")
            )
        if limit:
            qs = qs[:limit]

        qs = qs.only("id", "platform_slug", "raw_payload", *VENDOR_FIELDS)

        total = qs.count() if not limit else limit
        self.stdout.write(f"Backfilling vendor fields: {total:,} rows  dry_run={dry_run}")

        updated = skipped = errors = 0
        buffer = []

        def flush():
            nonlocal updated
            if not buffer or dry_run:
                updated += len(buffer)
                buffer.clear()
                return
            with transaction.atomic():
                RawJob.objects.bulk_update(buffer, VENDOR_FIELDS, batch_size=batch_size)
            updated += len(buffer)
            buffer.clear()

        for rj in qs.iterator(chunk_size=batch_size):
            extractor = PLATFORM_EXTRACTORS.get(rj.platform_slug)
            if not extractor:
                skipped += 1
                continue
            try:
                fields = extractor(rj.raw_payload or {})
            except Exception:
                errors += 1
                continue
            changed = False
            for field, value in fields.items():
                if value and (overwrite or not getattr(rj, field)):
                    setattr(rj, field, value)
                    changed = True
            if changed:
                buffer.append(rj)
                if len(buffer) >= batch_size:
                    flush()
                    self.stdout.write(f"  {updated:,} updated…")
            else:
                skipped += 1

        flush()
        self.stdout.write(self.style.SUCCESS(
            f"Done — updated: {updated:,}  skipped: {skipped:,}  errors: {errors:,}"
        ))
