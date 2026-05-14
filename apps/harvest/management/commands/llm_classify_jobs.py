"""
llm_classify_jobs
=================
Second-pass LLM classification for priority RawJobs that the rule engine
could not categorise (category_confidence = 0.0 or NULL).

Sends jobs to GPT-4o-mini in batches of 10, writes back:
  - job_category          (LLM-chosen from fixed 16-item list)
  - job_domain            (derived via detect_job_domains() using new category)
  - job_domain_candidates
  - category_confidence   (LLM-reported confidence, capped to 0.82 for non-obvious)
  - classification_source = "llm"
  - domain_version        (current CURRENT_DOMAIN_VERSION)

Usage:
    python manage.py llm_classify_jobs
    python manage.py llm_classify_jobs --limit 500 --batch-size 10 --dry-run
    python manage.py llm_classify_jobs --reclassify-all   # overwrite existing
    python manage.py llm_classify_jobs --model gpt-4o     # use smarter model
"""
from __future__ import annotations

import time
from django.db import transaction

from harvest.models import HarvestOpsRun

from ._ops_base import OpsTrackedCommand

UPDATE_FIELDS = [
    "job_category",
    "job_domain",
    "job_domain_candidates",
    "category_confidence",
    "classification_source",
    "domain_version",
]


class Command(OpsTrackedCommand):
    help = "LLM second-pass classification for unclassified priority RawJobs"
    ops_operation = HarvestOpsRun.Operation.LLM_CLASSIFY

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit", type=int, default=1000,
            help="Max jobs to process per run (default 1000)",
        )
        parser.add_argument(
            "--batch-size", type=int, default=10,
            help="Jobs per LLM API call (default 10, max 20)",
        )
        parser.add_argument(
            "--model", type=str, default="gpt-4o-mini",
            help="OpenAI model (default gpt-4o-mini)",
        )
        parser.add_argument(
            "--reclassify-all", action="store_true", default=False,
            help="Also re-run on jobs already classified by rules",
        )
        parser.add_argument(
            "--dry-run", action="store_true", default=False,
            help="Show what would be classified without writing",
        )
        parser.add_argument(
            "--confidence-threshold", type=float, default=0.5,
            help="Only reclassify jobs whose category_confidence is below this (default 0.5)",
        )

    def handle(self, *args, **options):
        from harvest.models import RawJob
        from harvest.enrichments import CURRENT_DOMAIN_VERSION, detect_job_domains
        from harvest.llm_classifier import classify_batch, BATCH_SIZE as DEFAULT_BATCH

        limit = max(1, options["limit"])
        batch_size = max(1, min(20, options["batch_size"]))
        model = options["model"].strip()
        reclassify_all = options["reclassify_all"]
        dry_run = options["dry_run"]
        conf_threshold = float(options["confidence_threshold"])

        self.stdout.write(
            f"LLM classify: model={model}  limit={limit:,}  batch={batch_size}  "
            f"dry_run={dry_run}  reclassify_all={reclassify_all}  conf_threshold={conf_threshold}"
        )

        qs = RawJob.objects.filter(is_priority=True).exclude(title="")
        if not reclassify_all:
            from django.db.models import Q
            qs = qs.filter(
                Q(category_confidence__isnull=True)
                | Q(category_confidence__lt=conf_threshold)
            )

        total = min(qs.count(), limit)
        self.stdout.write(f"  Eligible jobs: {total:,}")
        if total == 0:
            self.stdout.write("Nothing to classify.")
            return

        self.ops_start(total=total, message=f"LLM classifying {total:,} jobs…")

        classified = skipped = failed_calls = 0
        offset = 0

        while offset < total:
            batch_qs = list(
                qs.only("id", "title", "description", "job_category", "job_domain")
                .order_by("id")[offset: offset + batch_size]
            )
            if not batch_qs:
                break

            jobs_payload = [
                {
                    "id": rj.pk,
                    "title": rj.title or "",
                    "description": (rj.description or "")[:300],
                }
                for rj in batch_qs
            ]

            if dry_run:
                for rj in batch_qs:
                    self.stdout.write(f"  [dry-run] would classify pk={rj.pk} title={rj.title!r}")
                offset += len(batch_qs)
                classified += len(batch_qs)
                self.ops_progress(offset)
                continue

            results = classify_batch(jobs_payload, model=model)
            if not results:
                failed_calls += 1
                if failed_calls >= 3:
                    self.stderr.write("3 consecutive LLM call failures — aborting.")
                    break
                time.sleep(2)
                offset += len(batch_qs)
                continue
            failed_calls = 0

            updates: list[RawJob] = []
            for rj in batch_qs:
                hit = results.get(rj.pk)
                if not hit:
                    skipped += 1
                    continue
                new_category = hit["category"]
                new_confidence = hit["confidence"]
                domains = detect_job_domains(
                    rj.title or "",
                    (rj.description or "")[:500],
                    job_category=new_category,
                    max_matches=3,
                )
                rj.job_category = new_category
                rj.job_domain = domains[0] if domains else ""
                rj.job_domain_candidates = domains[:3]
                rj.category_confidence = round(new_confidence, 3)
                rj.classification_source = "llm"
                rj.domain_version = CURRENT_DOMAIN_VERSION
                updates.append(rj)
                classified += 1

            if updates:
                with transaction.atomic():
                    RawJob.objects.bulk_update(updates, UPDATE_FIELDS)

            offset += len(batch_qs)
            self.ops_progress(offset, message=f"{offset:,}/{total:,} processed")
            if offset % 100 == 0 or offset >= total:
                self.stdout.write(f"  {offset:,}/{total:,}  classified={classified:,}  skipped={skipped:,}")

            # Polite delay between API calls
            time.sleep(0.3)

        self.stdout.write("")
        self.stdout.write(f"✅  LLM classified: {classified:,}")
        self.stdout.write(f"   Skipped (no LLM result): {skipped:,}")
        self.ops_finish(audit_payload={
            "model": model,
            "classified": classified,
            "skipped": skipped,
            "dry_run": dry_run,
        })
