"""
Management command: validate_live_links

Runs the URL liveness check synchronously — no Celery required.
Checks all active RawJob URLs and marks expired ones is_active=False.

Usage:
    python manage.py validate_live_links
    python manage.py validate_live_links --batch-size 500 --concurrency 30
    python manage.py validate_live_links --platform greenhouse
    python manage.py validate_live_links --limit 1000   # spot-check
    python manage.py validate_live_links --dry-run      # report without writing
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from django.core.management.base import BaseCommand
from django.utils import timezone


class Command(BaseCommand):
    help = "Check all active RawJob URLs for liveness and mark expired ones inactive."

    def add_arguments(self, parser):
        parser.add_argument("--batch-size", type=int, default=500)
        parser.add_argument("--concurrency", type=int, default=30)
        parser.add_argument("--platform", type=str, default="")
        parser.add_argument("--limit", type=int, default=0, help="Cap total checked (0 = all)")
        parser.add_argument("--dry-run", action="store_true", help="Report only, don't write to DB")

    def handle(self, *args, **options):
        from harvest.models import RawJob
        from harvest.url_health import check_job_posting_live, is_definitive_inactive

        batch_size = options["batch_size"]
        concurrency = options["concurrency"]
        platform = options["platform"].strip()
        limit = options["limit"]
        dry_run = options["dry_run"]

        qs = RawJob.objects.filter(is_active=True).exclude(original_url="").order_by("id")
        if platform:
            qs = qs.filter(platform_slug=platform)
        if limit:
            qs = qs[:limit]

        total = qs.count()
        self.stdout.write(f"Checking {total:,} active raw job URLs"
                          f"{' [DRY RUN]' if dry_run else ''} …\n")

        checked = alive = dead = inconclusive = 0
        reason_counts: dict[str, int] = {}
        dead_ids: list[int] = []

        def check_one(job_id, url, slug):
            try:
                return job_id, check_job_posting_live(url, platform_slug=slug or "")
            except Exception as exc:
                from harvest.url_health import LinkHealthResult
                return job_id, LinkHealthResult(True, 0, f"exception:{exc!s:.60}", url)

        offset = 0
        while True:
            chunk = list(
                qs.values("id", "original_url", "platform_slug")[offset: offset + batch_size]
            )
            if not chunk:
                break

            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = {
                    pool.submit(check_one, r["id"], r["original_url"], r.get("platform_slug", "")): r["id"]
                    for r in chunk
                }
                chunk_dead: list[int] = []
                for fut in as_completed(futures):
                    job_id, result = fut.result()
                    checked += 1
                    reason = (getattr(result, "reason", "") or "").strip()[:80]
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1

                    if result.is_live:
                        alive += 1
                    elif is_definitive_inactive(result):
                        dead += 1
                        chunk_dead.append(job_id)
                        dead_ids.append(job_id)
                    else:
                        inconclusive += 1

            if chunk_dead and not dry_run:
                now = timezone.now()
                RawJob.objects.filter(pk__in=chunk_dead).update(is_active=False)
                for raw in RawJob.objects.filter(pk__in=chunk_dead).only("id", "raw_payload"):
                    payload = dict(raw.raw_payload or {})
                    payload["link_health"] = {
                        "is_live": False,
                        "checked_at": now.isoformat(),
                        "decisive": True,
                    }
                    raw.raw_payload = payload
                    raw.save(update_fields=["raw_payload", "updated_at"])

            pct = int(checked / total * 100) if total else 0
            self.stdout.write(
                f"\r  {pct:3d}%  checked={checked:,}  alive={alive:,}  "
                f"dead={dead:,}  inconclusive={inconclusive:,}",
                ending="",
            )
            self.stdout.flush()
            offset += batch_size
            if limit and offset >= limit:
                break

        self.stdout.write("\n")
        self.stdout.write(
            self.style.SUCCESS(
                f"\nDone. checked={checked:,} | alive={alive:,} | "
                f"deactivated={dead:,} | inconclusive={inconclusive:,}\n"
            )
        )
        if reason_counts:
            self.stdout.write("Top reasons:\n")
            for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1])[:15]:
                self.stdout.write(f"  {count:6,}  {reason}\n")
