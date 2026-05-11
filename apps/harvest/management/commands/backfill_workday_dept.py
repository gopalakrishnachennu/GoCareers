"""
backfill_workday_dept
=====================
Fetch the Workday CXS detail endpoint for Workday RawJobs that are missing
the department field. The list endpoint rarely returns jobFamilyGroup; the
detail endpoint always does.

This command calls the detail endpoint in polite batches and writes back:
  - department (from jobFamilyGroup / jobFamilyGroupName)
  - vendor_job_category (same value — the vendor's category label)

Works by reconstructing the detail URL from raw_payload.externalPath and
the stored tenant_id / platform_label.

Usage:
    python manage.py backfill_workday_dept
    python manage.py backfill_workday_dept --limit 1000 --dry-run
    python manage.py backfill_workday_dept --company "Acme Corp"
"""
from __future__ import annotations

import re
import time

import requests
from django.core.management.base import BaseCommand
from django.db import transaction


MIN_DELAY = 0.5   # seconds between requests — polite to Workday servers
MAX_FAILURES = 5  # abort a company after this many consecutive 4xx/5xx


def _extract_dept_from_payload(payload: dict) -> str:
    """Pull department from a Workday detail API response."""
    for key in ("jobFamilyGroup", "jobFamily"):
        val = payload.get(key) or (payload.get("jobPostingInfo") or {}).get(key) or []
        if isinstance(val, list) and val:
            return str(val[0].get("jobFamilyGroupName") or val[0].get("name") or "").strip()
        if isinstance(val, dict):
            return str(val.get("jobFamilyGroupName") or val.get("descriptor") or "").strip()
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def _build_detail_url(rj) -> str:
    """Reconstruct the Workday CXS detail URL from stored fields."""
    raw = rj.raw_payload or {}
    ext_path = raw.get("externalPath") or raw.get("ext_path") or ""
    if not ext_path:
        return ""
    # Derive tenant + jobboard from the label's tenant_id (e.g. "ACME/Careers")
    label = getattr(rj, "_label", None)
    tenant_id = (label.tenant_id if label else None) or ""
    # tenant_id format: "subdomain/jobboard"
    if "/" in tenant_id:
        subdomain, jobboard = tenant_id.split("/", 1)
    else:
        subdomain, jobboard = tenant_id, ""
    if not subdomain:
        return ""
    return (
        f"https://{subdomain}.myworkdayjobs.com"
        f"/wday/cxs/{subdomain}/{jobboard}{ext_path}"
    )


class Command(BaseCommand):
    help = "Backfill Workday department field by calling the CXS detail endpoint"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=2000, help="Max rows (default 2000).")
        parser.add_argument("--batch-size", type=int, default=500, help="DB batch size.")
        parser.add_argument("--company", default="", help="Limit to one company name substring.")
        parser.add_argument("--dry-run", action="store_true", default=False)

    def handle(self, *args, **options):
        from harvest.models import RawJob

        limit = max(1, int(options["limit"] or 2000))
        batch_size = max(50, min(int(options["batch_size"] or 500), 2000))
        company_filter = (options["company"] or "").strip()
        dry_run = options["dry_run"]

        qs = (
            RawJob.objects
            .filter(platform_slug="workday", department="", raw_payload__isnull=False)
            .exclude(raw_payload={})
            .select_related("platform_label")
            .only("id", "company_name", "department", "vendor_job_category", "raw_payload", "platform_label_id")
            .order_by("id")
        )
        if company_filter:
            qs = qs.filter(company_name__icontains=company_filter)
        qs = qs[:limit]

        total = qs.count()
        self.stdout.write(f"Workday dept backfill: {total:,} rows  dry_run={dry_run}")

        session = requests.Session()
        session.headers["User-Agent"] = "GoCareers-Bot/1.0 (dept-backfill)"

        updated = skipped = failed = 0
        buffer = []
        consecutive_fails = 0

        def flush():
            if not buffer or dry_run:
                buffer.clear()
                return
            with transaction.atomic():
                RawJob.objects.bulk_update(buffer, ["department", "vendor_job_category"], batch_size=batch_size)
            buffer.clear()

        for rj in qs.iterator(chunk_size=50):
            raw = rj.raw_payload or {}
            ext_path = raw.get("externalPath") or raw.get("ext_path") or ""
            if not ext_path:
                skipped += 1
                continue

            # Try to reconstruct URL from original_url pattern
            original_url = raw.get("original_url") or ""
            m = re.search(
                r"(https://[\w-]+\.myworkdayjobs\.com)/(?:wday/cxs/)?([\w-]+)/([\w-]+)",
                original_url or "",
            )
            if m:
                base, tenant, jobboard = m.group(1), m.group(2), m.group(3)
                detail_url = f"{base}/wday/cxs/{tenant}/{jobboard}{ext_path}"
            else:
                # Fall back to label tenant_id
                label = getattr(rj, "platform_label", None)
                tid = (label.tenant_id if label else None) or ""
                if "/" in tid:
                    subdomain, jobboard = tid.split("/", 1)
                else:
                    subdomain, jobboard = tid, ""
                if not subdomain:
                    skipped += 1
                    continue
                detail_url = (
                    f"https://{subdomain}.myworkdayjobs.com"
                    f"/wday/cxs/{subdomain}/{jobboard}{ext_path}"
                )

            if dry_run:
                self.stdout.write(f"  [dry-run] would fetch: {detail_url}")
                updated += 1
                continue

            try:
                resp = session.get(detail_url, headers={"Accept": "application/json"}, timeout=10)
                time.sleep(MIN_DELAY)
                if not resp.ok:
                    consecutive_fails += 1
                    if consecutive_fails >= MAX_FAILURES:
                        self.stderr.write(f"  {MAX_FAILURES} consecutive failures — aborting.")
                        break
                    failed += 1
                    continue
                consecutive_fails = 0
                data = resp.json()
                dept = _extract_dept_from_payload(data)
                if dept:
                    rj.department = dept
                    rj.vendor_job_category = dept[:128]
                    buffer.append(rj)
                    updated += 1
                    if len(buffer) >= batch_size:
                        flush()
                        self.stdout.write(f"  {updated:,} updated…")
                else:
                    skipped += 1
            except Exception as exc:
                failed += 1
                self.stderr.write(f"  Error for pk={rj.pk}: {exc}")

        flush()
        self.stdout.write(self.style.SUCCESS(
            f"Done — updated: {updated:,}  skipped: {skipped:,}  failed: {failed:,}"
        ))
