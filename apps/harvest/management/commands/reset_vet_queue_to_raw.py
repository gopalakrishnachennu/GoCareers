from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Dict, List, Optional, Set

from django.core.management.base import BaseCommand
from django.db import connection
from django.db import transaction
from django.utils import timezone


@dataclass
class ResetPlan:
    total_pool_jobs: int
    matched_raw_jobs: int
    create_raw_jobs: int
    unresolved_pool_jobs: int


class Command(BaseCommand):
    help = (
        "Move active Vet Queue (POOL jobs) back to Harvest Raw pipeline. "
        "Matched RawJob rows are set to PENDING and POOL jobs are archived."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--execute",
            action="store_true",
            help="Apply changes. Without this flag, command runs in dry-run mode.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Optional cap on number of POOL jobs to process (0 = all).",
        )
        parser.add_argument(
            "--no-create-missing-raw",
            action="store_true",
            help="Do not auto-create RawJob rows for unmatched POOL jobs.",
        )

    @staticmethod
    def _has_field(model, name: str) -> bool:
        return any(f.name == name for f in model._meta.get_fields())

    @staticmethod
    def _db_columns(model) -> Set[str]:
        table = model._meta.db_table
        with connection.cursor() as cursor:
            return {col.name for col in connection.introspection.get_table_description(cursor, table)}

    @staticmethod
    def _platform_slug_from_source(job_source: str) -> str:
        source = (job_source or "").strip().lower()
        if source.startswith("harvested_"):
            return source.replace("harvested_", "", 1)
        if source:
            return source[:64]
        return "jarvis"

    @staticmethod
    def _fallback_hash(original_link: str, job_id: int) -> str:
        base = (original_link or "").strip() or f"vet-reset:{job_id}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()

    def _plan(
        self,
        jobs: List[dict],
        create_missing_raw: bool,
    ) -> ResetPlan:
        from harvest.models import RawJob
        from harvest.normalizer import compute_url_hash

        explicit_raw_ids: Set[int] = {int(j["source_raw_job_id"]) for j in jobs if j.get("source_raw_job_id")}
        url_hashes: Set[str] = {j["url_hash"] for j in jobs if j.get("url_hash")}
        urls: Set[str] = {j["original_link"] for j in jobs if j.get("original_link")}

        by_hash = {
            row["url_hash"]: int(row["id"])
            for row in RawJob.objects.filter(url_hash__in=url_hashes).values("id", "url_hash")
            if row["url_hash"]
        } if url_hashes else {}
        by_url = {
            row["original_url"]: int(row["id"])
            for row in RawJob.objects.filter(original_url__in=urls).values("id", "original_url")
            if row["original_url"]
        } if urls else {}

        matched = 0
        create_needed = 0
        unresolved = 0
        for row in jobs:
            if row.get("source_raw_job_id"):
                matched += 1
                continue
            raw_hash = (row.get("url_hash") or "").strip()
            if raw_hash and raw_hash in by_hash:
                matched += 1
                continue
            original_link = (row.get("original_link") or "").strip()
            if original_link and original_link in by_url:
                matched += 1
                continue
            computed_hash = compute_url_hash(original_link) if original_link else ""
            if computed_hash and computed_hash in by_hash:
                matched += 1
                continue
            if create_missing_raw:
                create_needed += 1
            else:
                unresolved += 1

        matched += len(explicit_raw_ids)
        return ResetPlan(
            total_pool_jobs=len(jobs),
            matched_raw_jobs=matched,
            create_raw_jobs=create_needed,
            unresolved_pool_jobs=unresolved,
        )

    def handle(self, *args, **options):
        from harvest.models import RawJob
        from harvest.normalizer import compute_url_hash
        from jobs.models import Job
        from companies.models import Company

        execute: bool = bool(options.get("execute"))
        limit: int = int(options.get("limit") or 0)
        create_missing_raw: bool = not bool(options.get("no_create_missing_raw"))
        now = timezone.now()
        job_db_cols = self._db_columns(Job)
        raw_db_cols = self._db_columns(RawJob)

        qs = Job.objects.filter(status=Job.Status.POOL, is_archived=False).order_by("id")
        if limit and limit > 0:
            qs = qs[:limit]

        value_fields = ["id", "title", "company", "company_obj_id", "location", "description", "original_link", "url_hash", "job_source"]
        if "source_raw_job_id" in job_db_cols and self._has_field(Job, "source_raw_job"):
            value_fields.append("source_raw_job_id")
        jobs = list(qs.values(*value_fields))

        plan = self._plan(jobs=jobs, create_missing_raw=create_missing_raw)

        self.stdout.write(
            f"POOL jobs selected: {plan.total_pool_jobs:,}\n"
            f"Matched RawJob rows: {plan.matched_raw_jobs:,}\n"
            f"Raw rows to create: {plan.create_raw_jobs:,}\n"
            f"Unresolved POOL jobs: {plan.unresolved_pool_jobs:,}"
        )

        if not execute:
            self.stdout.write(
                self.style.WARNING(
                    "Dry-run only. Re-run with --execute to apply the reset."
                )
            )
            return

        pool_job_ids = [int(j["id"]) for j in jobs]
        if not pool_job_ids:
            self.stdout.write(self.style.SUCCESS("No active POOL jobs found. Nothing to reset."))
            return

        url_hashes: Set[str] = {j["url_hash"] for j in jobs if j.get("url_hash")}
        urls: Set[str] = {j["original_link"] for j in jobs if j.get("original_link")}
        by_hash: Dict[str, int] = {}
        if url_hashes:
            for row in RawJob.objects.filter(url_hash__in=url_hashes).values("id", "url_hash"):
                if row["url_hash"]:
                    by_hash[str(row["url_hash"])] = int(row["id"])
        by_url: Dict[str, int] = {}
        if urls:
            for row in RawJob.objects.filter(original_url__in=urls).values("id", "original_url"):
                if row["original_url"]:
                    by_url[str(row["original_url"])] = int(row["id"])

        company_ids = {int(j["company_obj_id"]) for j in jobs if j.get("company_obj_id")}
        company_by_id = {c.id: c for c in Company.objects.filter(id__in=company_ids)} if company_ids else {}

        created_raw = 0
        unresolved = 0
        raw_ids_to_pending: Set[int] = set()

        with transaction.atomic():
            for row in jobs:
                raw_id: Optional[int] = None
                source_raw_job_id = row.get("source_raw_job_id")
                if source_raw_job_id:
                    raw_id = int(source_raw_job_id)
                else:
                    job_hash = (row.get("url_hash") or "").strip()
                    original_link = (row.get("original_link") or "").strip()
                    if job_hash and job_hash in by_hash:
                        raw_id = by_hash[job_hash]
                    elif original_link and original_link in by_url:
                        raw_id = by_url[original_link]
                    else:
                        computed_hash = compute_url_hash(original_link) if original_link else ""
                        if computed_hash and computed_hash in by_hash:
                            raw_id = by_hash[computed_hash]
                        elif create_missing_raw:
                            company_obj = None
                            company_obj_id = row.get("company_obj_id")
                            if company_obj_id:
                                company_obj = company_by_id.get(int(company_obj_id))
                            if not company_obj:
                                company_name = (row.get("company") or "Unknown Company").strip()[:255] or "Unknown Company"
                                company_obj, _ = Company.objects.get_or_create(name=company_name)
                                company_by_id[company_obj.id] = company_obj

                            final_hash = (
                                job_hash
                                or computed_hash
                                or self._fallback_hash(original_link=original_link, job_id=int(row["id"]))
                            )
                            defaults = {
                                "company": company_obj,
                                "external_id": f"vet-reset-{row['id']}",
                                "original_url": original_link,
                                "apply_url": original_link,
                                "title": (row.get("title") or "Untitled Job")[:512],
                                "company_name": (row.get("company") or company_obj.name or "")[:256],
                                "location_raw": (row.get("location") or "")[:512],
                                "description": row.get("description") or "",
                                "description_clean": row.get("description") or "",
                                "platform_slug": self._platform_slug_from_source(row.get("job_source") or ""),
                                "sync_status": RawJob.SyncStatus.PENDING,
                                "is_active": True,
                                "raw_payload": {
                                    "reset_from_vet": {
                                        "job_id": int(row["id"]),
                                        "at": now.isoformat(),
                                    }
                                },
                            }
                            raw_obj, created = RawJob.objects.get_or_create(url_hash=final_hash, defaults=defaults)
                            raw_id = raw_obj.id
                            by_hash[final_hash] = raw_obj.id
                            if original_link:
                                by_url[original_link] = raw_obj.id
                            if created:
                                created_raw += 1

                if raw_id:
                    raw_ids_to_pending.add(int(raw_id))
                else:
                    unresolved += 1

            raw_updated = 0
            if raw_ids_to_pending:
                raw_updates = {"sync_status": RawJob.SyncStatus.PENDING}
                if "updated_at" in raw_db_cols:
                    raw_updates["updated_at"] = now
                raw_updated = RawJob.objects.filter(id__in=raw_ids_to_pending).update(**raw_updates)

            updates = {
                "status": Job.Status.CLOSED,
                "is_archived": True,
                "archived_at": now,
            }
            if "stage" in job_db_cols and self._has_field(Job, "stage"):
                updates["stage"] = Job.Stage.ARCHIVED
            if "stage_changed_at" in job_db_cols and self._has_field(Job, "stage_changed_at"):
                updates["stage_changed_at"] = now
            if "pipeline_reason_code" in job_db_cols and self._has_field(Job, "pipeline_reason_code"):
                updates["pipeline_reason_code"] = "RESET_TO_RAW"
            if "pipeline_reason_detail" in job_db_cols and self._has_field(Job, "pipeline_reason_detail"):
                updates["pipeline_reason_detail"] = (
                    "Reset from Vet Queue to Harvest Raw for pipeline reprocessing."
                )
            if "hard_gate_passed" in job_db_cols and self._has_field(Job, "hard_gate_passed"):
                updates["hard_gate_passed"] = False
            if "gate_status" in job_db_cols and self._has_field(Job, "gate_status"):
                updates["gate_status"] = Job.GateStatus.REVIEW
            if "vet_lane" in job_db_cols and self._has_field(Job, "vet_lane"):
                updates["vet_lane"] = Job.VetLane.HUMAN

            jobs_archived = Job.objects.filter(
                id__in=pool_job_ids, status=Job.Status.POOL, is_archived=False
            ).update(**updates)

        pool_remaining = Job.objects.filter(status=Job.Status.POOL, is_archived=False).count()
        raw_pending = RawJob.objects.filter(sync_status=RawJob.SyncStatus.PENDING).count()

        self.stdout.write(
            self.style.SUCCESS(
                "Vet queue reset complete.\n"
                f"Archived POOL jobs: {jobs_archived:,}\n"
                f"Raw rows set back to PENDING: {raw_updated:,}\n"
                f"New Raw rows created: {created_raw:,}\n"
                f"Unresolved POOL rows: {unresolved:,}\n"
                f"Remaining active POOL jobs: {pool_remaining:,}\n"
                f"Current total Raw PENDING: {raw_pending:,}"
            )
        )
