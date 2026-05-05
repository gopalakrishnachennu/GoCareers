"""
analyze_job_taxonomy
====================
Queries the live DB to surface the real title/category distribution across
all RawJobs so we can build a data-driven domain taxonomy.

Usage (on prod server):
    python manage.py analyze_job_taxonomy
    python manage.py analyze_job_taxonomy --top-titles 500 --output /tmp/taxonomy.txt
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from django.core.management.base import BaseCommand
from django.db.models import Count


class Command(BaseCommand):
    help = "Analyse RawJob title/category distribution to build the domain taxonomy"

    def add_arguments(self, parser):
        parser.add_argument("--top-titles", type=int, default=300,
                            help="How many top normalized titles to show (default 300)")
        parser.add_argument("--output", type=str, default="",
                            help="Write report to file instead of stdout")
        parser.add_argument("--min-count", type=int, default=5,
                            help="Skip titles appearing fewer than N times (default 5)")

    def handle(self, *args, **options):
        from harvest.models import RawJob
        try:
            from jobs.models import Job
            has_jobs = True
        except ImportError:
            has_jobs = False

        lines: list[str] = []

        def p(text=""):
            lines.append(text)
            self.stdout.write(text)

        p("=" * 72)
        p("JOB TAXONOMY ANALYSIS — LIVE DB")
        p("=" * 72)

        # ── 1. Overall counts ────────────────────────────────────────────────
        total = RawJob.objects.count()
        p(f"\n▶ Total RawJobs: {total:,}")
        p(f"  Synced:  {RawJob.objects.filter(sync_status='SYNCED').count():,}")
        p(f"  Pending: {RawJob.objects.filter(sync_status='PENDING').count():,}")
        p(f"  With title:      {RawJob.objects.exclude(title='').count():,}")
        p(f"  With description:{RawJob.objects.filter(has_description=True).count():,}")
        p(f"  With job_category set: {RawJob.objects.exclude(job_category='').count():,}")

        # ── 2. Existing job_category distribution ────────────────────────────
        p("\n" + "─" * 72)
        p("EXISTING job_category values (enrichment engine output):")
        p("─" * 72)
        cat_qs = (
            RawJob.objects
            .exclude(job_category="")
            .values("job_category")
            .annotate(n=Count("id"))
            .order_by("-n")
        )
        for row in cat_qs:
            p(f"  {row['n']:>7,}  {row['job_category']}")

        # ── 3. department_normalized distribution ────────────────────────────
        p("\n" + "─" * 72)
        p("EXISTING department_normalized values on RawJob:")
        p("─" * 72)
        dept_qs = (
            RawJob.objects
            .exclude(department_normalized="")
            .values("department_normalized")
            .annotate(n=Count("id"))
            .order_by("-n")
        )
        for row in dept_qs:
            p(f"  {row['n']:>7,}  {row['department_normalized']}")

        # ── 4. Platform distribution ─────────────────────────────────────────
        p("\n" + "─" * 72)
        p("JOBS BY PLATFORM:")
        p("─" * 72)
        plat_qs = (
            RawJob.objects
            .values("platform_slug")
            .annotate(n=Count("id"))
            .order_by("-n")
        )
        for row in plat_qs:
            p(f"  {row['n']:>7,}  {row['platform_slug'] or '(none)'}")

        # ── 5. Normalize and count titles ────────────────────────────────────
        p("\n" + "─" * 72)
        p(f"TOP {options['top_titles']} NORMALIZED TITLE TOKENS (min {options['min_count']} occurrences):")
        p("─" * 72)
        p("(stripping seniority prefixes, locations, IDs, punctuation)")

        # Seniority/qualifier words to strip from titles for grouping
        _STRIP_WORDS = {
            "senior", "sr", "junior", "jr", "lead", "principal", "staff",
            "associate", "executive", "chief", "head", "director",
            "manager", "vp", "i", "ii", "iii", "iv", "v", "1", "2", "3",
            "remote", "contract", "contractor", "temporary", "temp", "intern",
            "entry", "level", "mid", "experienced", "new", "grad",
        }

        def _normalize_title(raw: str) -> str:
            # lowercase, strip parens/brackets and contents
            t = raw.lower().strip()
            t = re.sub(r"\([^)]*\)", " ", t)
            t = re.sub(r"\[[^\]]*\]", " ", t)
            # strip trailing location " - City, ST"
            t = re.sub(r"\s*[-–|/,]\s*(remote|onsite|hybrid|us|usa|india|uk|canada|australia).*$", "", t)
            # strip roman numerals and level numbers at end
            t = re.sub(r"\s+[ivxl]+\s*$", "", t)
            t = re.sub(r"\s+[1-9]\s*$", "", t)
            # strip req/job IDs
            t = re.sub(r"\b(req|job|id|#)\s*[\w-]+", "", t)
            # strip punctuation
            t = re.sub(r"[^\w\s/-]", " ", t)
            # strip seniority words
            words = [w for w in t.split() if w not in _STRIP_WORDS and len(w) > 1]
            return " ".join(words).strip()

        top_n = options["top_titles"]
        min_count = options["min_count"]
        batch_size = 5000
        title_counter: Counter = Counter()

        total_titles = RawJob.objects.exclude(title="").count()
        p(f"  Processing {total_titles:,} job titles in batches...")

        offset = 0
        processed = 0
        while True:
            batch = list(
                RawJob.objects
                .exclude(title="")
                .values_list("title", flat=True)
                [offset:offset + batch_size]
            )
            if not batch:
                break
            for t in batch:
                norm = _normalize_title(t)
                if norm and len(norm) > 3:
                    title_counter[norm] += 1
            processed += len(batch)
            offset += batch_size
            if processed % 20000 == 0:
                self.stdout.write(f"  ...{processed:,}/{total_titles:,}")

        p(f"\n  Unique normalized titles: {len(title_counter):,}")
        p(f"  Titles with ≥{min_count} occurrences: "
          f"{sum(1 for c in title_counter.values() if c >= min_count):,}")
        p()
        p(f"{'COUNT':>8}  NORMALIZED TITLE")
        p(f"{'─'*8}  {'─'*50}")

        shown = 0
        for title, count in title_counter.most_common(top_n * 3):
            if count < min_count:
                break
            if shown >= top_n:
                break
            p(f"  {count:>6,}  {title}")
            shown += 1

        # ── 6. Keyword frequency in titles (useful for domain detection) ──────
        p("\n" + "─" * 72)
        p("TOP SINGLE-WORD TOKENS IN ALL TITLES (domain signal words):")
        p("─" * 72)
        _NOISE = {
            "engineer", "engineering", "developer", "development", "manager", "management",
            "specialist", "analyst", "associate", "director", "lead", "senior", "junior",
            "staff", "principal", "head", "chief", "officer", "vice", "president",
            "consultant", "advisor", "coordinator", "administrator", "admin",
            "supervisor", "technician", "operator", "representative",
            "and", "of", "the", "in", "for", "a", "an", "to", "with", "at", "or",
            "i", "ii", "iii", "iv", "v", "1", "2", "3",
        }
        word_counter: Counter = Counter()
        for title, count in title_counter.items():
            for word in title.split():
                if word not in _NOISE and len(word) > 2:
                    word_counter[word] += count

        p(f"{'COUNT':>8}  KEYWORD")
        p(f"{'─'*8}  {'─'*40}")
        for word, count in word_counter.most_common(100):
            if count < 20:
                break
            p(f"  {count:>6,}  {word}")

        # ── 7. job_category + title cross-tab (top 5 titles per category) ─────
        p("\n" + "─" * 72)
        p("TOP 5 TITLES PER job_category (spot-check classification accuracy):")
        p("─" * 72)
        from django.db.models import Q

        categories = [
            row["job_category"] for row in
            RawJob.objects.exclude(job_category="")
            .values("job_category").distinct()
        ]
        for cat in sorted(categories):
            top_in_cat = (
                RawJob.objects
                .filter(job_category=cat)
                .exclude(title="")
                .values("title")
                .annotate(n=Count("id"))
                .order_by("-n")[:5]
            )
            titles_str = " | ".join(f"{r['title']} ({r['n']})" for r in top_in_cat)
            p(f"\n  [{cat}]")
            p(f"    {titles_str}")

        # ── 8. Unclassified jobs (job_category is blank) ─────────────────────
        p("\n" + "─" * 72)
        p("TOP 50 TITLES WITH NO job_category (unclassified — need domain coverage):")
        p("─" * 72)
        unclassified_titles = (
            RawJob.objects
            .filter(job_category="")
            .exclude(title="")
            .values("title")
            .annotate(n=Count("id"))
            .order_by("-n")[:50]
        )
        for row in unclassified_titles:
            p(f"  {row['n']:>6,}  {row['title']}")

        # ── 9. Jobs model department distribution ────────────────────────────
        if has_jobs:
            p("\n" + "─" * 72)
            p("SYNCED Job.department distribution (15k synced jobs):")
            p("─" * 72)
            jdept_qs = (
                Job.objects
                .values("department")
                .annotate(n=Count("id"))
                .order_by("-n")
            )
            for row in jdept_qs:
                p(f"  {row['n']:>7,}  {row['department'] or '(blank)'}")

        # ── Write output file ────────────────────────────────────────────────
        output_path = options.get("output", "")
        if output_path:
            with open(output_path, "w") as f:
                f.write("\n".join(lines))
            self.stdout.write(f"\n✅ Report written to {output_path}")
        else:
            p("\n✅ Done. Run with --output /tmp/taxonomy.txt to save to file.")
