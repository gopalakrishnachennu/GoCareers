"""
Board Analytics Service
=======================
Single source of truth for per-platform dashboard metrics.

Produces ONE unified payload per platform using a SINGLE denominator
(total RawJob rows) so "blocked > jobs" is impossible.

Usage:
    from harvest.board_analytics import get_board_analytics
    data = get_board_analytics(window_days=30)
"""

from __future__ import annotations

from datetime import timedelta
from django.db import models
from django.db.models import Avg, Case, Count, F, FloatField, Max, Min, Q, Sum, When
from django.utils import timezone

from .board_capabilities import get_capabilities, capability_gap


# Boards that harvest via Jarvis (manual paste) — excluded from ATS ranking.
JARVIS_SLUGS = {"jarvis"}

# Boards known to be operationally broken / no active harvester.
UNSUPPORTED_SLUGS = {"applytojob", "adp", "applicantpro", "dayforce"}

# Minimum runs required before we trust the risk score.
MIN_RUNS_FOR_RISK = 5


def _pct(numerator, denominator, decimals=1):
    if not denominator:
        return None
    return round(numerator / denominator * 100, decimals)


def get_board_analytics(window_days: int = 30) -> dict:
    """
    Returns a dict with:
      - platforms: list of per-platform metric rows (ATS boards only, no Jarvis)
      - jarvis: separate summary for Jarvis/manual ingest
      - unsupported: list of slugs marked as UNSUPPORTED
      - generated_at: ISO timestamp
      - window_days: the run-history window used
    """
    from harvest.models import CompanyFetchRun, JobBoardPlatform, RawJob

    now = timezone.now()
    run_window = now - timedelta(days=window_days)

    # ── 1. Per-platform RawJob metrics (job-level, all-time) ──────────────────
    # Detect which optional fields exist in this DB (handles schema drift gracefully).
    _raw_fields = {f.name for f in RawJob._meta.get_fields()}

    _field_annotations = {
        "total":       Count("id"),
        "synced":      Count("id", filter=Q(sync_status="SYNCED")),
        "pending":     Count("id", filter=Q(sync_status="PENDING")),
        "failed_sync": Count("id", filter=Q(sync_status="FAILED")),
        "skipped":     Count("id", filter=Q(sync_status="SKIPPED")),
        "inactive":    Count("id", filter=Q(is_active=False)),
        "missing_jd":  Count("id", filter=Q(has_description=False)),
        "has_salary":  Count("id", filter=Q(salary_min__isnull=False) | Q(salary_max__isnull=False)),
    }
    # Blocker reason counts (only if sync_skip_reason field exists)
    if "sync_skip_reason" in _raw_fields:
        _field_annotations["blocked_inactive"]    = Count("id", filter=Q(sync_skip_reason="INACTIVE_POSTING"))
        _field_annotations["blocked_jd_weak"]     = Count("id", filter=Q(sync_skip_reason="JD_TOO_WEAK"))
        _field_annotations["blocked_mismatch"]    = Count("id", filter=Q(sync_skip_reason="PLATFORM_MISMATCH"))
        _field_annotations["blocked_duplicate"]   = Count("id", filter=Q(sync_skip_reason__in=["DUPLICATE_RISK", "DUPLICATE_EXISTING"]))
        _field_annotations["blocked_no_company"]  = Count("id", filter=Q(sync_skip_reason="COMPANY_UNRESOLVED"))
    if "requirements" in _raw_fields:
        _field_annotations["has_requirements"] = Count("id", filter=~Q(requirements=""))
    if "responsibilities" in _raw_fields:
        _field_annotations["has_responsibilities"] = Count("id", filter=~Q(responsibilities=""))
    if "department" in _raw_fields:
        _field_annotations["has_department"] = Count("id", filter=~Q(department=""))
    if "city" in _raw_fields:
        _field_annotations["has_geo"] = Count("id", filter=~Q(city="") | ~Q(country=""))
    if "education_required" in _raw_fields:
        _field_annotations["has_education"] = Count("id", filter=~Q(education_required=""))
    if "employment_type" in _raw_fields:
        _field_annotations["has_schedule"] = Count("id", filter=~Q(employment_type="UNKNOWN"))

    job_qs = (
        RawJob.objects
        .exclude(platform_slug__in=JARVIS_SLUGS)
        .values("platform_slug")
        .annotate(**_field_annotations)
    )
    job_by_slug = {row["platform_slug"]: row for row in job_qs}

    # ── 2. Per-platform Run metrics (run-level, within window) ────────────────
    run_qs = (
        CompanyFetchRun.objects
        .filter(started_at__gte=run_window)
        .exclude(label__platform__slug__in=JARVIS_SLUGS)
        .values("label__platform__slug")
        .annotate(
            runs=Count("id"),
            success_runs=Count("id", filter=Q(status=CompanyFetchRun.Status.SUCCESS)),
            empty_runs=Count("id", filter=Q(status=CompanyFetchRun.Status.EMPTY)),
            partial_runs=Count("id", filter=Q(status=CompanyFetchRun.Status.PARTIAL)),
            failed_runs=Count("id", filter=Q(status=CompanyFetchRun.Status.FAILED)),
            parse_error_runs=Count("id", filter=Q(error_type=CompanyFetchRun.ErrorType.PARSE_ERROR)),
            timeout_runs=Count("id", filter=Q(error_type=CompanyFetchRun.ErrorType.TIMEOUT)),
            rate_limited_runs=Count("id", filter=Q(error_type=CompanyFetchRun.ErrorType.RATE_LIMITED)),
            avg_jobs_found=Avg("jobs_found"),
            avg_duration_secs=Avg(
                Case(
                    When(
                        completed_at__isnull=False,
                        then=models.ExpressionWrapper(
                            F("completed_at") - F("started_at"),
                            output_field=models.DurationField(),
                        ),
                    ),
                    output_field=models.DurationField(),
                )
            ),
            last_run=Max("started_at"),
        )
    )
    run_by_slug = {row["label__platform__slug"]: row for row in run_qs}

    # ── 3. Platform registry (support_tier, is_enabled) ──────────────────────
    platform_meta = {
        p.slug: {"support_tier": p.support_tier, "name": p.name, "is_enabled": p.is_enabled}
        for p in JobBoardPlatform.objects.all()
    }

    # ── 4. Merge into per-platform rows ──────────────────────────────────────
    all_slugs = set(job_by_slug) | set(run_by_slug)
    ats_rows = []
    unsupported_rows = []
    jarvis_rows = []

    for slug in sorted(all_slugs):
        j = job_by_slug.get(slug, {})
        r = run_by_slug.get(slug, {})
        meta = platform_meta.get(slug, {})
        tier = meta.get("support_tier", "healthy")

        total = j.get("total", 0)
        runs = r.get("runs", 0)
        success_runs = r.get("success_runs", 0)
        empty_runs = r.get("empty_runs", 0)
        partial_runs = r.get("partial_runs", 0)
        failed_runs = r.get("failed_runs", 0)
        parse_error_runs = r.get("parse_error_runs", 0)

        # "blocked" = PENDING RawJobs (same denominator as total)
        pending = j.get("pending", 0)
        inactive = j.get("inactive", 0)
        missing_jd = j.get("missing_jd", 0)

        # Risk score: weighted combination of failure signals (0–100)
        # Requires MIN_RUNS_FOR_RISK runs to be trustworthy.
        risk_score = None
        risk_trusted = False
        if runs >= MIN_RUNS_FOR_RISK:
            empty_rate = empty_runs / runs if runs else 0
            fail_rate  = failed_runs / runs if runs else 0
            parse_rate = parse_error_runs / runs if runs else 0
            block_rate = pending / total if total else 0
            risk_score = round(
                (empty_rate * 25) +
                (fail_rate  * 35) +
                (parse_rate * 25) +
                (block_rate * 15),
                1,
            )
            risk_trusted = True

        avg_duration = r.get("avg_duration_secs")
        avg_duration_secs = (
            avg_duration.total_seconds() if hasattr(avg_duration, "total_seconds") else None
        )

        coverage = {
            "requirements":     _pct(j.get("has_requirements", 0), total),
            "responsibilities": _pct(j.get("has_responsibilities", 0), total),
            "salary":           _pct(j.get("has_salary", 0), total),
            "department":       _pct(j.get("has_department", 0), total),
            "geo":              _pct(j.get("has_geo", 0), total),
            "education":        _pct(j.get("has_education", 0), total),
            "schedule":         _pct(j.get("has_schedule", 0), total),
        }

        caps = get_capabilities(slug)
        gaps = capability_gap(slug, coverage)

        # Build human-readable issue reasons for the dashboard badge
        issue_reasons: list[str] = []
        if runs >= MIN_RUNS_FOR_RISK:
            if risk_score and risk_score >= 35:
                if empty_runs / runs >= 0.4:
                    issue_reasons.append("high zero-yield")
                if failed_runs / runs >= 0.2:
                    issue_reasons.append("high fail rate")
                if parse_error_runs / runs >= 0.2:
                    issue_reasons.append("parse errors")
                if total and pending / total >= 0.3:
                    issue_reasons.append("blocked sync")
        if total and missing_jd / total >= 0.2:
            issue_reasons.append("missing JD")

        row = {
            "slug": slug,
            "name": meta.get("name", slug),
            "support_tier": tier,
            "is_enabled": meta.get("is_enabled", True),
            # job-level (all-time)
            "total_jobs": total,
            "synced": j.get("synced", 0),
            "pending": pending,
            "failed_sync": j.get("failed_sync", 0),
            "skipped": j.get("skipped", 0),
            "inactive": inactive,
            "missing_jd": missing_jd,
            "inactive_pct":  _pct(inactive, total),
            "missing_jd_pct": _pct(missing_jd, total),
            "pending_pct":   _pct(pending, total),
            # field coverage
            "coverage": coverage,
            # capability matrix + gap analysis
            "capabilities": {k: v for k, v in caps.items() if isinstance(v, bool)},
            "capability_gaps": gaps,
            "source_reliability": caps.get("source_reliability", "unknown"),
            # run-level (window_days)
            "runs": runs,
            "success_runs": success_runs,
            "empty_runs": empty_runs,
            "partial_runs": partial_runs,
            "failed_runs": failed_runs,
            "parse_error_runs": parse_error_runs,
            "timeout_runs": r.get("timeout_runs", 0),
            "rate_limited_runs": r.get("rate_limited_runs", 0),
            "zero_yield_pct": _pct(empty_runs, runs),
            "fail_rate_pct":  _pct(failed_runs, runs),
            "success_rate_pct": _pct(success_runs + partial_runs * 0.5, runs),
            "avg_jobs_per_run": round(float(r.get("avg_jobs_found") or 0), 1),
            "avg_duration_secs": round(avg_duration_secs, 1) if avg_duration_secs else None,
            "last_run": r.get("last_run").isoformat() if r.get("last_run") else None,
            "risk_score": risk_score,
            "risk_trusted": risk_trusted,   # False when runs < MIN_RUNS_FOR_RISK
            "issue_reasons": issue_reasons, # list of human-readable issue tags
            # Blocker breakdown — why jobs are failing sync (zero = not set / pre-migration)
            "blockers": {
                "inactive":   j.get("blocked_inactive", 0),
                "jd_weak":    j.get("blocked_jd_weak", 0),
                "mismatch":   j.get("blocked_mismatch", 0),
                "duplicate":  j.get("blocked_duplicate", 0),
                "no_company": j.get("blocked_no_company", 0),
            },
        }

        if slug in UNSUPPORTED_SLUGS or tier == JobBoardPlatform.SupportTier.UNSUPPORTED:
            unsupported_rows.append(row)
        else:
            ats_rows.append(row)

    # Sort: highest risk first (None = no runs → put last)
    ats_rows.sort(key=lambda x: (x["risk_score"] is None, -(x["risk_score"] or 0), -x["total_jobs"]))

    # ── 5. Jarvis summary ────────────────────────────────────────────────────
    jarvis_qs = RawJob.objects.filter(platform_slug="jarvis")
    jarvis_total = jarvis_qs.count()
    jarvis_summary = {
        "total_jobs": jarvis_total,
        "synced": jarvis_qs.filter(sync_status="SYNCED").count(),
        "pending": jarvis_qs.filter(sync_status="PENDING").count(),
        "missing_jd": jarvis_qs.filter(has_description=False).count(),
        "inactive": jarvis_qs.filter(is_active=False).count(),
        "note": "Manual/Jarvis ingest — not ranked alongside ATS boards.",
    }

    return {
        "platforms": ats_rows,
        "unsupported": unsupported_rows,
        "jarvis": jarvis_summary,
        "generated_at": now.isoformat(),
        "window_days": window_days,
        "totals": {
            "ats_boards": len(ats_rows),
            "total_rawjobs": sum(r["total_jobs"] for r in ats_rows),
            "total_synced":  sum(r["synced"]     for r in ats_rows),
            "total_pending": sum(r["pending"]     for r in ats_rows),
        },
    }
