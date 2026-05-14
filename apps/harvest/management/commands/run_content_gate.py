"""
Management command: run_content_gate

Runs the Tier-2 JD content gate on existing AMBIGUOUS / POSSIBLE jobs.
Use this to backfill the gate on the current RawJob backlog before enabling
enforcement (jd_gate_enabled=True, jd_gate_audit_mode=False).

Usage:
  # Dry run — show what would happen, touch nothing
  python manage.py run_content_gate --batch-size 200 --dry-run

  # Audit run — gate runs, decisions recorded, no suppression
  python manage.py run_content_gate --batch-size 500 --audit-mode

  # Full enforcement on AMBIGUOUS titles only (safest)
  python manage.py run_content_gate --batch-size 500

  # Extend to POSSIBLE/COLD-with-tech-signal (catch more false negatives)
  python manage.py run_content_gate --batch-size 500 --scope all_possible

  # Use a different model
  python manage.py run_content_gate --model gpt-4o --threshold 0.70

  # Loop until all eligible jobs processed
  python manage.py run_content_gate --batch-size 500 --loop

Ops safety:
  Always run with --dry-run first.
  Use --loop to drain large backlogs (it will keep running until queue is empty).
"""
import time

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Run Tier-2 JD content gate on existing AMBIGUOUS / POSSIBLE RawJobs"

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Number of jobs to process per run (default 100).",
        )
        parser.add_argument(
            "--scope",
            default=None,
            choices=["ambiguous_only", "all_possible", "all_non_hard_no"],
            help=(
                "Which jobs to include. "
                "ambiguous_only = AMBIGUOUS title gate only (default from config). "
                "all_possible = AMBIGUOUS + legacy POSSIBLE. "
                "all_non_hard_no = everything except explicit rejects."
            ),
        )
        parser.add_argument(
            "--model",
            default=None,
            help="Override LLM model (e.g. gpt-4o-mini, gpt-4o). Default from config.",
        )
        parser.add_argument(
            "--threshold",
            type=float,
            default=None,
            help="Override confidence threshold (0.0–1.0). Default from config.",
        )
        parser.add_argument(
            "--snippet-chars",
            type=int,
            default=None,
            help="Max JD snippet chars sent to LLM. Default from config (800).",
        )
        parser.add_argument(
            "--audit-mode",
            action="store_true",
            help="Record gate decisions without suppressing any jobs (safe for tuning).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Evaluate gate and print results, but write NOTHING to the database.",
        )
        parser.add_argument(
            "--loop",
            action="store_true",
            help="Keep running until the eligible queue is empty (drains large backlogs).",
        )
        parser.add_argument(
            "--no-trigger-backfill",
            action="store_true",
            help="Do not automatically queue CONFIRMED jobs for JD backfill.",
        )

    def handle(self, *args, **options):
        from harvest.content_gate import run_content_gate

        batch_size       = options["batch_size"]
        scope            = options["scope"]
        model            = options["model"]
        threshold        = options["threshold"]
        snippet_chars    = options["snippet_chars"]
        audit_mode       = options["audit_mode"] or None  # None = use config value
        dry_run          = options["dry_run"]
        loop             = options["loop"]
        trigger_backfill = not options["no_trigger_backfill"]

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no database changes will be made."))

        total_confirmed = total_rejected = total_uncertain = total_errors = 0
        run_count = 0

        while True:
            run_count += 1
            self.stdout.write(f"\n[Run {run_count}] Processing up to {batch_size} jobs...")

            t0 = time.monotonic()
            result = run_content_gate(
                batch_size=batch_size,
                dry_run=dry_run,
                scope=scope,
                model=model,
                confidence_threshold=threshold,
                snippet_chars=snippet_chars,
                audit_mode=audit_mode,
                trigger_backfill_on_confirm=trigger_backfill,
            )
            elapsed = time.monotonic() - t0

            total_confirmed  += result.confirmed
            total_rejected   += result.rejected
            total_uncertain  += result.uncertain
            total_errors     += result.errors

            # ── Print run summary ─────────────────────────────────────────────
            self.stdout.write(
                f"  Processed: {result.total_processed}  "
                f"Confirmed: {self.style.SUCCESS(str(result.confirmed))}  "
                f"Rejected: {self.style.ERROR(str(result.rejected))}  "
                f"Uncertain: {self.style.WARNING(str(result.uncertain))}  "
                f"Errors: {result.errors}  "
                f"({elapsed:.1f}s)"
            )
            self.stdout.write(
                f"  Snippets — list(free): {result.snippet_from_list}  "
                f"detail(fetch): {result.snippet_from_detail}"
            )
            if result.audit_mode:
                self.stdout.write(self.style.WARNING("  [AUDIT MODE] No jobs were suppressed."))
            if result.errors_detail:
                for err in result.errors_detail[:5]:
                    self.stdout.write(self.style.ERROR(f"  Error: {err}"))

            # ── Stop conditions ───────────────────────────────────────────────
            if result.total_processed == 0:
                self.stdout.write("  Queue is empty — nothing left to process.")
                break

            if not loop:
                break

            # Brief pause between loops (avoid hammering the LLM API)
            if result.total_processed >= batch_size:
                time.sleep(2)

        # ── Final summary ─────────────────────────────────────────────────────
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(
            f"TOTAL after {run_count} run(s): "
            f"Confirmed={self.style.SUCCESS(str(total_confirmed))}  "
            f"Rejected={self.style.ERROR(str(total_rejected))}  "
            f"Uncertain={self.style.WARNING(str(total_uncertain))}  "
            f"Errors={total_errors}"
        )

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — nothing was written."))
        elif total_confirmed > 0 and trigger_backfill:
            self.stdout.write(
                self.style.SUCCESS(
                    f"{total_confirmed} CONFIRMED jobs queued for JD backfill automatically."
                )
            )
        self.stdout.write(self.style.SUCCESS("Done."))
