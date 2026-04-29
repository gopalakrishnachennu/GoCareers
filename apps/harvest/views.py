import json
import logging
from urllib.parse import urlencode

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.core.cache import cache
from django.core.serializers.json import DjangoJSONEncoder
from django.db import transaction
from django.db.models import Avg, Count, F, Q, Sum, Value, Max, Min
from django.db.models.functions import Coalesce, Length, Trim
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse, reverse_lazy
from datetime import timedelta

from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views.decorators.cache import never_cache
from django.views.generic import CreateView, DetailView, ListView, TemplateView, UpdateView, View

from core.http import redirect_with_task_progress

from .forms import JobBoardPlatformForm
from .models import (
    CompanyFetchRun,
    CompanyPlatformLabel,
    FetchBatch,
    HarvestEngineConfig,
    JobBoardPlatform,
    RawJob,
)
from .resume_profile import build_resume_job_profile
from .jd_gate import evaluate_raw_job_resume_gate

logger = logging.getLogger(__name__)


def _find_existing_live_job_for_rawjob(raw_job):
    """Find existing non-archived Job mapped to a RawJob URL hash/link."""
    from jobs.dedup import find_existing_job_by_url
    from jobs.models import Job

    if raw_job.url_hash:
        by_hash = Job.objects.filter(url_hash=raw_job.url_hash, is_archived=False).first()
        if by_hash:
            return by_hash
    if raw_job.original_url:
        by_url = find_existing_job_by_url(raw_job.original_url)
        if by_url and not by_url.is_archived:
            return by_url
        by_link = Job.objects.filter(original_link=raw_job.original_url, is_archived=False).first()
        if by_link:
            return by_link
    return None


def _invalidate_rawjobs_dashboard_cache() -> None:
    """Best-effort cache bust for Raw Jobs KPI cards."""
    try:
        cache.delete("rawjobs_dashboard_stats")
        cache.delete("rawjobs_expired_missing_jd")
        for h in (3, 6, 12, 24):
            cache.delete(f"rawjobs_workflow_insights_{h}")
    except Exception:
        pass


def _load_rawjobs_dashboard_stats(*, force_refresh: bool = False) -> dict:
    """
    Unified dashboard stats payload used by both HTML and JSON views.

    Keeping this in one place avoids KPI drift between initial render and polling.
    """
    from django.utils.timezone import now as _now

    stats_key = "rawjobs_dashboard_stats"
    expired_key = "rawjobs_expired_missing_jd"
    stats_ttl_sec = 20
    expired_ttl_sec = 120

    stats = None if force_refresh else cache.get(stats_key)
    if stats is not None:
        return stats

    last_24h_cutoff = _now() - timedelta(hours=24)
    agg = RawJob.objects.aggregate(
        total=Count("id"),
        active=Count("id", filter=Q(is_active=True)),
        remote=Count("id", filter=Q(is_remote=True)),
        synced=Count("id", filter=Q(sync_status="SYNCED")),
        pending=Count("id", filter=Q(sync_status="PENDING")),
        failed=Count("id", filter=Q(sync_status="FAILED")),
        # Rolling 24h avoids timezone-midnight confusion for global users.
        new_today=Count("id", filter=Q(fetched_at__gte=last_24h_cutoff)),
        missing_jd=Count("id", filter=Q(has_description=False)),
    )
    expired_missing = None if force_refresh else cache.get(expired_key)
    if expired_missing is None:
        expired_missing = raw_jobs_missing_jd_expired_count()
        cache.set(expired_key, expired_missing, expired_ttl_sec)
    agg["expired_missing"] = expired_missing
    cache.set(stats_key, agg, stats_ttl_sec)
    return agg


def _sync_rawjob_to_pool(raw_job, *, posted_by):
    """
    Sync one RawJob into Job pool (same mapping as bulk sync task).

    Returns ``(job, created_new)``.
    """
    from django.utils import timezone as _tz
    from jobs.models import Job
    from jobs.quality import compute_quality_score
    from jobs.gating import apply_gate_result_to_job, evaluate_raw_job_gate
    from .url_health import check_job_posting_live

    existing = _find_existing_live_job_for_rawjob(raw_job)
    if existing:
        if raw_job.sync_status != "SYNCED":
            raw_job.sync_status = "SKIPPED"
            raw_job.save(update_fields=["sync_status", "updated_at"])
            _invalidate_rawjobs_dashboard_cache()
        return existing, False

    # Last-mile link-health check before any promotion.
    if (raw_job.original_url or "").strip():
        live = check_job_posting_live(
            raw_job.original_url,
            platform_slug=(raw_job.platform_slug or ""),
        )
        if not live.is_live:
            payload = dict(raw_job.raw_payload or {})
            payload["link_health"] = {
                "is_live": False,
                "reason": live.reason,
                "status_code": live.status_code,
                "checked_at": _tz.now().isoformat(),
                "final_url": live.final_url,
            }
            raw_job.is_active = False
            raw_job.raw_payload = payload
            raw_job.save(update_fields=["is_active", "raw_payload", "updated_at"])
            _invalidate_rawjobs_dashboard_cache()
            raise ValueError(
                f"Cannot promote to vet queue: posting appears inactive ({live.reason})."
            )

    gate = evaluate_raw_job_gate(raw_job)
    if not gate.passed:
        raise ValueError(
            f"Cannot promote to vet queue: blocked by gate ({gate.reason_code}). "
            f"Reasons: {', '.join(gate.reasons[:3])}"
        )

    platform_slug = raw_job.platform_slug or (raw_job.job_platform.slug if raw_job.job_platform else "")
    with transaction.atomic():
        job = Job.objects.create(
            title=raw_job.title,
            company=raw_job.company_name or (raw_job.company.name if raw_job.company else ""),
            company_obj=raw_job.company,
            location=raw_job.location_raw or "",
            description=raw_job.description or raw_job.title,
            original_link=raw_job.original_url,
            salary_range=raw_job.salary_raw or "",
            job_type=raw_job.employment_type if raw_job.employment_type != "UNKNOWN" else "FULL_TIME",
            status="POOL",
            stage=Job.Stage.VETTED,
            stage_changed_at=_tz.now(),
            url_hash=raw_job.url_hash or "",
            job_source=f"HARVESTED_{platform_slug.upper()}" if platform_slug else "HARVESTED",
            posted_by=posted_by,
            source_raw_job=raw_job,
            queue_entered_at=_tz.now(),
        )
        apply_gate_result_to_job(job, gate)
        job.quality_score = compute_quality_score(job)
        job.validation_score = int(round(gate.vet_priority_score * 100))
        job.validation_result = {
            "score": job.validation_score,
            "lane": gate.lane,
            "gate_status": gate.status,
            "reason_code": gate.reason_code,
            "reasons": gate.reasons,
            "checks": gate.checks,
            "multi_score": {
                "data_quality": gate.data_quality_score,
                "trust": gate.trust_score,
                "candidate_fit": gate.candidate_fit_score,
                "vet_priority": gate.vet_priority_score,
            },
        }
        job.validation_run_at = _tz.now()
        job.gate_checked_at = _tz.now()
        job.save(
            update_fields=[
                "hard_gate_passed", "gate_status", "vet_lane",
                "pipeline_reason_code", "pipeline_reason_detail",
                "hard_gate_failures", "hard_gate_checks",
                "data_quality_score", "trust_score", "candidate_fit_score", "vet_priority_score",
                "quality_score", "validation_score", "validation_result",
                "validation_run_at", "gate_checked_at",
            ]
        )
        payload = dict(raw_job.raw_payload or {})
        payload["vet_gate"] = {
            "status": "eligible",
            "lane": gate.lane,
            "reason_code": gate.reason_code,
            "checks": gate.checks,
            "scores": {
                "data_quality": gate.data_quality_score,
                "trust": gate.trust_score,
                "candidate_fit": gate.candidate_fit_score,
                "vet_priority": gate.vet_priority_score,
            },
            "job_id": job.pk,
            "checked_at": _tz.now().isoformat(),
        }
        raw_job.sync_status = "SYNCED"
        raw_job.raw_payload = payload
        raw_job.save(update_fields=["sync_status", "raw_payload", "updated_at"])
    _invalidate_rawjobs_dashboard_cache()
    return job, True


def _raw_jobs_missing_jd_base_qs():
    """Rows with no real JD text (same rule as Jobs Browser ``has_jd``)."""
    from .tasks import BACKFILL_LOCK_STALE_MINUTES

    stale_before = timezone.now() - timedelta(minutes=BACKFILL_LOCK_STALE_MINUTES)
    return (
        RawJob.objects.annotate(
            _jd_len=Length(Trim(Coalesce(F("description"), Value("")))),
        )
        .filter(_jd_len__lte=1)
        .exclude(original_url="")
        .filter(
            Q(jd_backfill_locked_at__isnull=True)
            | Q(jd_backfill_locked_at__lt=stale_before),
        )
    )


def raw_jobs_missing_description_count() -> int:
    """Count jobs with empty/trivial description that have a URL (backfill candidates)."""
    return _raw_jobs_missing_jd_base_qs().count()


def raw_jobs_missing_jd_expired_count() -> int:
    """Subset of missing-JD rows that look expired (aligned with RawJob.is_expired_listing)."""
    today = timezone.now().date()
    now = timezone.now()
    stale_days = max(30, int(getattr(settings, "HARVEST_JD_STALE_DAYS", 120)))
    stale_cutoff = today - timedelta(days=stale_days)
    return (
        _raw_jobs_missing_jd_base_qs()
        .filter(
            Q(expires_at__lt=now)
            | Q(closing_date__lt=today)
            | Q(is_active=False)
            | Q(raw_payload__active=False)
            | Q(posted_date__lt=stale_cutoff),
        )
        .count()
    )


def _raw_jobs_workflow_insights(*, stale_pending_hours: int = 6) -> dict:
    """
    Aggregate operational insights for the Raw Jobs workflow board.
    """
    cache_key = f"rawjobs_workflow_insights_{max(1, int(stale_pending_hours))}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    now = timezone.now()
    stale_cutoff = now - timedelta(hours=max(1, int(stale_pending_hours)))
    recent_cutoff = now - timedelta(hours=24)

    base = RawJob.objects.all()
    total = base.count()

    parsed = base.filter(has_description=True).count()
    enriched = base.filter(Q(quality_score__isnull=False) | Q(jd_quality_score__isnull=False)).count()
    classified = base.filter(classification_confidence__gte=0.01).count()
    ready = base.filter(has_description=True, classification_confidence__gte=0.55, is_active=True).count()
    synced = base.filter(sync_status=RawJob.SyncStatus.SYNCED).count()

    pending_qs = base.filter(sync_status=RawJob.SyncStatus.PENDING)
    pending_total = pending_qs.count()
    pending_stale_qs = pending_qs.filter(fetched_at__lt=stale_cutoff)
    pending_stale = pending_stale_qs.count()

    pending_aging = {
        "lt_1h": pending_qs.filter(fetched_at__gte=now - timedelta(hours=1)).count(),
        "h_1_6": pending_qs.filter(fetched_at__lt=now - timedelta(hours=1), fetched_at__gte=now - timedelta(hours=6)).count(),
        "h_6_24": pending_qs.filter(fetched_at__lt=now - timedelta(hours=6), fetched_at__gte=now - timedelta(hours=24)).count(),
        "gt_24h": pending_qs.filter(fetched_at__lt=now - timedelta(hours=24)).count(),
    }

    completed_24h = base.filter(sync_status__in=[RawJob.SyncStatus.SYNCED, RawJob.SyncStatus.SKIPPED], updated_at__gte=recent_cutoff).count()
    failed_24h = base.filter(sync_status=RawJob.SyncStatus.FAILED, updated_at__gte=recent_cutoff).count()
    drain_per_hour = round(completed_24h / 24.0, 2) if completed_24h > 0 else 0.0
    eta_hours = round(pending_total / drain_per_hour, 1) if drain_per_hour > 0 else None

    missing_jd = base.filter(has_description=False).count()
    html_heavy = base.filter(has_html_content=True).count()
    low_confidence = base.filter(Q(classification_confidence__lt=0.55) | Q(classification_confidence__isnull=True)).count()
    missing_salary = base.filter(salary_min__isnull=True, salary_max__isnull=True).count()
    missing_location = base.filter(
        Q(location_raw="") & Q(city="") & Q(state="") & Q(country="")
    ).count()
    missing_experience = base.filter(
        Q(experience_level=RawJob.ExperienceLevel.UNKNOWN)
        & Q(years_required__isnull=True)
        & Q(years_required_max__isnull=True)
    ).count()

    # Duplicate = SKIPPED in current pipeline semantics (already in pool / URL hash match)
    duplicate_total = base.filter(sync_status=RawJob.SyncStatus.SKIPPED).count()
    duplicate_recent = base.filter(sync_status=RawJob.SyncStatus.SKIPPED, updated_at__gte=recent_cutoff).count()

    blocked_companies = list(
        pending_stale_qs.values("company_name")
        .annotate(count=Count("id"), last_seen=Max("fetched_at"))
        .order_by("-count")[:10]
    )
    blocked_platforms = list(
        pending_stale_qs.values("platform_slug")
        .annotate(count=Count("id"), last_seen=Max("fetched_at"))
        .order_by("-count")[:10]
    )

    stuck_queue = list(
        pending_stale_qs.values("platform_label_id", "company_name", "platform_slug")
        .annotate(count=Count("id"), oldest=Min("fetched_at"))
        .order_by("-count")[:100]
    )

    # Platform health from per-company fetch runs in last 24h
    recent_runs = CompanyFetchRun.objects.filter(started_at__gte=recent_cutoff)
    platform_health = []
    for row in (
        recent_runs.values("label__platform__slug")
        .annotate(
            runs=Count("id"),
            success=Count("id", filter=Q(status=CompanyFetchRun.Status.SUCCESS)),
            partial=Count("id", filter=Q(status=CompanyFetchRun.Status.PARTIAL)),
            failed=Count("id", filter=Q(status=CompanyFetchRun.Status.FAILED)),
            avg_jobs_found=Avg("jobs_found"),
            avg_jobs_new=Avg("jobs_new"),
            avg_jobs_failed=Avg("jobs_failed"),
            last_run=Max("started_at"),
        )
        .order_by("-runs")
    ):
        slug = (row.get("label__platform__slug") or "unknown").strip() or "unknown"
        runs = int(row.get("runs") or 0)
        success = int(row.get("success") or 0)
        partial = int(row.get("partial") or 0)
        failed = int(row.get("failed") or 0)
        success_rate = round(((success + (partial * 0.5)) / runs) * 100, 1) if runs else 0.0
        platform_health.append(
            {
                "platform_slug": slug,
                "runs": runs,
                "success": success,
                "partial": partial,
                "failed": failed,
                "success_rate": success_rate,
                "avg_jobs_found": round(float(row.get("avg_jobs_found") or 0.0), 1),
                "avg_jobs_new": round(float(row.get("avg_jobs_new") or 0.0), 1),
                "avg_jobs_failed": round(float(row.get("avg_jobs_failed") or 0.0), 1),
                "last_run": row.get("last_run").isoformat() if row.get("last_run") else "",
            }
        )

    payload = {
        "funnel": {
            "fetched": total,
            "parsed": parsed,
            "enriched": enriched,
            "classified": classified,
            "ready": ready,
            "synced": synced,
        },
        "queue": {
            "pending_total": pending_total,
            "pending_stale": pending_stale,
            "stale_pending_hours": max(1, int(stale_pending_hours)),
            "aging": pending_aging,
            "drain_per_hour": drain_per_hour,
            "eta_hours": eta_hours,
            "completed_24h": completed_24h,
            "failed_24h": failed_24h,
        },
        "quality_debt": {
            "missing_jd": missing_jd,
            "html_heavy": html_heavy,
            "low_confidence": low_confidence,
            "missing_salary": missing_salary,
            "missing_location": missing_location,
            "missing_experience": missing_experience,
        },
        "duplicates": {
            "total": duplicate_total,
            "recent_24h": duplicate_recent,
        },
        "top_blocked": {
            "companies": blocked_companies,
            "platforms": blocked_platforms,
        },
        "stuck_queue": stuck_queue,
        "platform_health": platform_health,
        "generated_at": now.isoformat(),
    }
    cache.set(cache_key, payload, 20)
    return payload


class SuperuserRequiredMixin(LoginRequiredMixin, UserPassesTestMixin):
    def test_func(self):
        return self.request.user.is_superuser


# ── Platform Registry ──────────────────────────────────────────────────────────

class PlatformListView(SuperuserRequiredMixin, TemplateView):
    template_name = "harvest/settings_platforms.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["platforms"] = JobBoardPlatform.objects.annotate(
            company_count=Count("labels")
        ).order_by("name")
        ctx["form"] = JobBoardPlatformForm()
        ctx["active_tab"] = "platforms"
        ctx["total_platforms"] = JobBoardPlatform.objects.count()
        ctx["enabled_count"] = JobBoardPlatform.objects.filter(is_enabled=True).count()
        return ctx


class PlatformCreateView(SuperuserRequiredMixin, CreateView):
    model = JobBoardPlatform
    form_class = JobBoardPlatformForm
    template_name = "harvest/platform_form.html"
    success_url = reverse_lazy("harvest-platforms")

    def form_valid(self, form):
        messages.success(self.request, f"Platform '{form.instance.name}' created successfully.")
        return super().form_valid(form)

    def form_invalid(self, form):
        messages.error(self.request, "Please fix the errors below.")
        return super().form_invalid(form)


class PlatformUpdateView(SuperuserRequiredMixin, UpdateView):
    model = JobBoardPlatform
    form_class = JobBoardPlatformForm
    template_name = "harvest/platform_form.html"
    success_url = reverse_lazy("harvest-platforms")

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["is_edit"] = True
        return ctx

    def form_valid(self, form):
        messages.success(self.request, f"Platform '{form.instance.name}' updated.")
        return super().form_valid(form)


class PlatformDeleteView(SuperuserRequiredMixin, View):
    def post(self, request, pk):
        platform = get_object_or_404(JobBoardPlatform, pk=pk)
        name = platform.name
        platform.delete()
        messages.success(request, f"Platform '{name}' deleted.")
        return redirect("harvest-platforms")


class PlatformToggleView(SuperuserRequiredMixin, View):
    def post(self, request, pk):
        platform = get_object_or_404(JobBoardPlatform, pk=pk)
        platform.is_enabled = not platform.is_enabled
        platform.save(update_fields=["is_enabled"])
        return JsonResponse({"enabled": platform.is_enabled, "name": platform.name})


# ── Schedule Config ────────────────────────────────────────────────────────────

class ScheduleConfigView(SuperuserRequiredMixin, TemplateView):
    template_name = "harvest/settings_schedule.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["active_tab"] = "schedule"
        try:
            from django_celery_beat.models import PeriodicTask
            ctx["periodic_tasks"] = PeriodicTask.objects.filter(
                Q(name__icontains="harvest") | Q(name__icontains="detect") | Q(name__icontains="cleanup")
            ).order_by("name")
        except Exception:
            ctx["periodic_tasks"] = []
        return ctx


# ── Run Monitor ────────────────────────────────────────────────────────────────

class RunMonitorView(SuperuserRequiredMixin, ListView):
    """Phase 5: monitor now uses RawJob + PipelineEvent (HarvestRun removed)."""
    template_name = "harvest/settings_monitor.html"
    context_object_name = "runs"
    paginate_by = 30

    def get_queryset(self):
        from jobs.models import PipelineEvent
        return PipelineEvent.objects.filter(task_name="harvest.harvest_jobs").order_by("-occurred_at")

    def get_context_data(self, **kwargs):
        from .models import RawJob
        ctx = super().get_context_data(**kwargs)
        ctx["active_tab"] = "monitor"
        ctx["platforms"] = JobBoardPlatform.objects.filter(is_enabled=True)
        ctx["total_harvested"] = RawJob.objects.filter(is_active=True).count()
        ctx["pending_sync"] = RawJob.objects.filter(sync_status="PENDING").count()
        ctx["synced_count"] = RawJob.objects.filter(sync_status="SYNCED").count()
        ctx["total_runs"] = 0
        ctx["running_runs"] = 0
        return ctx


# ── Company Labels ─────────────────────────────────────────────────────────────

class CompanyLabelListView(SuperuserRequiredMixin, ListView):
    template_name = "harvest/settings_labels.html"
    context_object_name = "labels"
    paginate_by = 100

    def get_queryset(self):
        qs = CompanyPlatformLabel.objects.select_related(
            "company", "platform", "verified_by"
        ).order_by("company__name")

        platform_f = self.request.GET.get("platform", "").strip()
        if platform_f == "UNDETECTED":
            qs = qs.filter(detection_method="UNDETECTED")
        elif platform_f:
            qs = qs.filter(platform__slug=platform_f)

        confidence_f = self.request.GET.get("confidence", "").strip()
        if confidence_f:
            qs = qs.filter(confidence=confidence_f)

        method_f = self.request.GET.get("method", "").strip()
        if method_f:
            qs = qs.filter(detection_method=method_f)

        status_f = self.request.GET.get("status", "").strip()
        if status_f == "verified":
            qs = qs.filter(portal_alive=True)
        elif status_f == "down":
            qs = qs.filter(portal_alive=False)
        elif status_f == "unchecked":
            qs = qs.filter(portal_alive__isnull=True, platform__isnull=False)
        elif status_f == "no_tenant":
            qs = qs.filter(platform__isnull=False, tenant_id="")
        elif status_f == "no_ats":
            qs = qs.filter(detection_method="UNDETECTED")

        verified_f = self.request.GET.get("verified", "").strip()
        if verified_f == "yes":
            qs = qs.filter(is_verified=True)
        elif verified_f == "no":
            qs = qs.filter(is_verified=False)

        q = self.request.GET.get("q", "").strip()
        if q:
            qs = qs.filter(company__name__icontains=q)

        return qs

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["active_tab"] = "labels"
        ctx["platforms"] = JobBoardPlatform.objects.annotate(
            company_count=Count("labels")
        ).order_by("name")
        ctx["platforms_chart"] = JobBoardPlatform.objects.annotate(
            company_count=Count("labels")
        ).filter(company_count__gt=0).order_by("-company_count")
        from companies.models import Company

        ctx["stat_labeled"] = CompanyPlatformLabel.objects.exclude(
            detection_method="UNDETECTED"
        ).count()
        ctx["stat_undetected"] = CompanyPlatformLabel.objects.filter(
            detection_method="UNDETECTED"
        ).count()
        ctx["stat_unlabeled"] = Company.objects.exclude(
            platform_label__isnull=False
        ).count()
        ctx["stat_verified"] = CompanyPlatformLabel.objects.filter(is_verified=True).count()
        ctx["stat_live"] = CompanyPlatformLabel.objects.filter(portal_alive=True).count()
        ctx["stat_down"] = CompanyPlatformLabel.objects.filter(portal_alive=False).count()
        ctx["confidence_choices"] = CompanyPlatformLabel.Confidence.choices
        ctx["method_choices"] = CompanyPlatformLabel.DetectionMethod.choices
        ctx["selected_platform"] = self.request.GET.get("platform", "")
        ctx["selected_confidence"] = self.request.GET.get("confidence", "")
        ctx["selected_method"] = self.request.GET.get("method", "")
        ctx["selected_status"] = self.request.GET.get("status", "")
        ctx["selected_verified"] = self.request.GET.get("verified", "")
        ctx["q"] = self.request.GET.get("q", "")
        return ctx


class LabelVerifyView(SuperuserRequiredMixin, View):
    """Toggle verified status — returns JSON for AJAX or redirects for plain POST."""
    def post(self, request, pk):
        label = get_object_or_404(CompanyPlatformLabel, pk=pk)
        label.is_verified = not label.is_verified
        label.verified_by = request.user if label.is_verified else None
        label.verified_at = timezone.now() if label.is_verified else None
        label.save(update_fields=["is_verified", "verified_by", "verified_at"])
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return JsonResponse({"verified": label.is_verified, "pk": pk})
        return redirect(request.META.get("HTTP_REFERER") or "harvest-labels")


class LabelManualSetView(SuperuserRequiredMixin, View):
    """Set platform + optional tenant for a label — returns JSON for AJAX."""
    def post(self, request, pk):
        label = get_object_or_404(CompanyPlatformLabel, pk=pk)
        platform_id = request.POST.get("platform_id", "").strip()
        tenant_id = request.POST.get("tenant_id", "").strip()
        platform = None
        if platform_id:
            platform = get_object_or_404(JobBoardPlatform, pk=platform_id)
        label.platform = platform
        label.detection_method = "MANUAL"
        label.confidence = "HIGH"
        label.is_verified = True
        label.verified_by = request.user
        label.verified_at = timezone.now()
        if tenant_id:
            label.tenant_id = tenant_id
        label.save()
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            from .career_url import build_career_url
            url = build_career_url(platform.slug if platform else "", label.tenant_id)
            return JsonResponse({
                "ok": True,
                "pk": pk,
                "platform_name": platform.name if platform else "",
                "platform_color": platform.color_hex if platform else "#6B7280",
                "tenant_id": label.tenant_id,
                "career_url": url,
                "scrape_status": label.scrape_status,
            })
        messages.success(
            request,
            f"Set {label.company.name} → {platform.name if platform else 'None'}",
        )
        return redirect(request.META.get("HTTP_REFERER") or "harvest-labels")


class LabelUpdateTenantView(SuperuserRequiredMixin, View):
    """Inline update of tenant_id only — AJAX only."""
    def post(self, request, pk):
        label = get_object_or_404(CompanyPlatformLabel, pk=pk)
        tenant_id = request.POST.get("tenant_id", "").strip()
        label.tenant_id = tenant_id
        label.portal_alive = None   # reset health — needs re-check
        label.portal_last_verified = None
        label.save(update_fields=["tenant_id", "portal_alive", "portal_last_verified"])
        from .career_url import build_career_url
        url = build_career_url(label.platform.slug if label.platform else "", tenant_id)
        return JsonResponse({
            "ok": True,
            "pk": pk,
            "tenant_id": tenant_id,
            "career_url": url,
            "scrape_status": label.scrape_status,
        })


# ── Trigger Actions ────────────────────────────────────────────────────────────

class RunDetectNowView(SuperuserRequiredMixin, View):
    def post(self, request):
        from .tasks import detect_company_platforms_task
        task = detect_company_platforms_task.delay(
            batch_size=200,
            triggered_user_id=request.user.id,
        )
        messages.success(
            request,
            "Platform detection is running on the server. "
            f"Refresh Run Monitor to see progress (task {task.id[:8]}…). "
            "Switching tabs does not stop this job.",
        )
        return redirect_with_task_progress(
            "harvest-monitor",
            task.id,
            "Platform detection",
        )


class RunHarvestNowView(SuperuserRequiredMixin, View):
    def post(self, request):
        from .tasks import harvest_jobs_task
        platform_slug = request.POST.get("platform_slug", "").strip() or None
        task = harvest_jobs_task.delay(
            platform_slug=platform_slug,
            triggered_by="MANUAL",
            triggered_user_id=request.user.id,
        )
        label = platform_slug or "all platforms"
        messages.success(
            request,
            f"Harvest for {label} is running on the server (task {task.id[:8]}…). "
            "Refresh Run Monitor for results; switching tabs does not cancel work.",
        )
        return redirect_with_task_progress(
            "harvest-monitor",
            task.id,
            f"Harvest ({label})",
        )


class RunSyncNowView(SuperuserRequiredMixin, View):
    def post(self, request):
        from .tasks import sync_harvested_to_pool_task
        max_jobs = int(request.POST.get("max_jobs", "500") or "500")
        task = sync_harvested_to_pool_task.delay(max_jobs=max_jobs)
        messages.success(request, f"Sync to job pool started (max {max_jobs:,} jobs, Task: {task.id[:8]}...)")
        return redirect_with_task_progress("harvest-monitor", task.id, "Sync to job pool")


class RunBulkSyncView(SuperuserRequiredMixin, View):
    """POST — sync up to 20,000 pending RawJobs to the pool in one shot."""
    def post(self, request):
        from .tasks import sync_harvested_to_pool_task
        task = sync_harvested_to_pool_task.delay(max_jobs=20000)
        messages.success(
            request,
            f"Bulk sync started — up to 20,000 pending jobs → pool (Task: {task.id[:8]}…). "
            "This runs in the background. Refresh to see progress.",
        )
        return redirect_with_task_progress("harvest-rawjobs", task.id, "Bulk sync (20k jobs)")


class RunRetryFailedFetchesView(SuperuserRequiredMixin, View):
    """POST — retry FAILED company fetch runs from the last 7 days."""

    def post(self, request):
        from .tasks import retry_failed_raw_jobs_task

        task = retry_failed_raw_jobs_task.delay()
        messages.success(
            request,
            f"Retry failed fetches queued (Task: {task.id[:8]}…). "
            "This will re-run failed company fetches in the background.",
        )
        return redirect_with_task_progress("harvest-rawjobs", task.id, "Retry failed fetches")


class RunValidateRawUrlsView(SuperuserRequiredMixin, View):
    """POST — run robust link-health validation on active raw jobs."""

    def post(self, request):
        from .tasks import validate_raw_job_urls_task

        platform = (request.POST.get("platform_slug") or "").strip() or None
        recent_hours = request.POST.get("recent_hours", "").strip()
        pending_only = (request.POST.get("pending_only", "1").strip() != "0")
        max_jobs = request.POST.get("max_jobs", "").strip()

        kwargs = {
            "batch_size": 250,
            "concurrency": 24,
            "pending_only": pending_only,
        }
        if platform:
            kwargs["platform_slug"] = platform
        if recent_hours.isdigit():
            kwargs["recent_hours"] = int(recent_hours)
        else:
            kwargs["recent_hours"] = 168
        if max_jobs.isdigit():
            kwargs["max_jobs"] = int(max_jobs)
        else:
            kwargs["max_jobs"] = 8000

        task = validate_raw_job_urls_task.delay(**kwargs)
        messages.success(
            request,
            f"Link-health validation queued (Task: {task.id[:8]}…). "
            "Soft-404 pages will be marked inactive before vet sync.",
        )
        return redirect_with_task_progress("harvest-rawjobs", task.id, "Validate Raw Job URLs")


class RunSyncSelectedRawJobsView(SuperuserRequiredMixin, View):
    """POST — sync selected RawJob ids into pool."""

    def post(self, request):
        raw_ids = request.POST.get("raw_job_ids", "").strip()
        if not raw_ids:
            messages.error(request, "No rows selected for sync.")
            return redirect("harvest-rawjobs")

        parts = [p.strip() for p in raw_ids.split(",") if p.strip()]
        ids: list[int] = []
        for part in parts:
            if part.isdigit():
                ids.append(int(part))
        ids = ids[:500]
        if not ids:
            messages.error(request, "Selected ids were invalid.")
            return redirect("harvest-rawjobs")

        qs = RawJob.objects.select_related("company", "job_platform").filter(pk__in=ids)
        synced = skipped = failed = 0
        skipped_reasons: list[str] = []
        for raw_job in qs:
            try:
                _sync_rawjob_to_pool(raw_job, posted_by=request.user)
                synced += 1
            except ValueError as exc:
                skipped += 1
                reason = str(exc).strip()
                if reason and len(skipped_reasons) < 3:
                    skipped_reasons.append(reason)
            except Exception:
                failed += 1
                logger.exception("Sync selected raw job failed: raw_job_id=%s", raw_job.pk)

        msg = f"Selected sync complete — {synced} synced, {skipped} skipped, {failed} failed."
        if skipped_reasons:
            msg += " Sample blocked reasons: " + " | ".join(skipped_reasons)
        messages.success(request, msg)
        return redirect("harvest-rawjobs")


class RunCleanupNowView(SuperuserRequiredMixin, View):
    def post(self, request):
        from .tasks import cleanup_harvested_jobs_task
        task = cleanup_harvested_jobs_task.delay()
        messages.success(request, f"Cleanup started (Task: {task.id[:8]}...)")
        return redirect_with_task_progress("harvest-monitor", task.id, "Harvest cleanup")


class RunBackfillNowView(SuperuserRequiredMixin, View):
    def post(self, request):
        from .tasks import backfill_platform_labels_from_jobs_task
        task = backfill_platform_labels_from_jobs_task.delay()
        messages.success(request, f"Backfill started — scanning all job URLs to detect platforms (Task: {task.id[:8]}...)")
        return redirect_with_task_progress("harvest-labels", task.id, "Platform backfill from job URLs")


class RunVerifyPortalsView(SuperuserRequiredMixin, View):
    """Queue async HTTP health checks for all career portal URLs."""
    def post(self, request):
        from .tasks import verify_all_portals_task
        task = verify_all_portals_task.delay()
        messages.success(
            request,
            f"Portal verification started — checking all career URLs in the background (Task: {task.id[:8]}...)"
        )
        return redirect_with_task_progress("harvest-labels", task.id, "Verifying career portal health")


# ── Raw Jobs Views ─────────────────────────────────────────────────────────────

class RawJobListView(SuperuserRequiredMixin, ListView):
    model = RawJob
    template_name = "harvest/rawjobs_list.html"
    context_object_name = "jobs"
    paginate_by = 100

    def get(self, request, *args, **kwargs):
        """JSON path for infinite-scroll: ?page=N with X-Requested-With:XMLHttpRequest"""
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            from django.core.paginator import Paginator
            from django.urls import reverse
            # Fetch only the columns rendered in the list — skips description /
            # raw_payload blobs which can be 10–50 KB each and are never shown here.
            qs = self.get_queryset().only(
                "id", "company_name", "platform_slug", "title", "original_url",
                "location_raw", "is_remote", "employment_type", "experience_level",
                "salary_min", "salary_max", "salary_raw", "posted_date", "fetched_at",
                "sync_status", "has_description", "is_active",
                "department", "department_normalized", "state", "country",
                "education_required", "languages_required", "certifications",
                "licenses_required", "clearance_required", "clearance_level",
                "schedule_type", "shift_schedule", "travel_pct_min", "travel_pct_max",
                "resume_ready_score", "classification_confidence", "quality_score",
                "jd_quality_score", "raw_payload",
                "job_keywords", "title_keywords",
                "company_industry", "company_size",
                "word_count",
            )
            paginator = Paginator(qs, self.paginate_by)
            try:
                page_num = int(request.GET.get("page", 1))
                page_obj = paginator.page(page_num)
            except Exception:
                return JsonResponse({"jobs": [], "has_next": False, "total": 0})

            jobs_data = []
            for job in page_obj.object_list:
                jd_gate = evaluate_raw_job_resume_gate(job)
                jobs_data.append({
                    "id": job.pk,
                    "company_name": (job.company_name or "")[:30],
                    "platform_slug": job.platform_slug or "",
                    "title": (job.title or "")[:60],
                    "original_url": job.original_url or "",
                    "location_raw": (job.location_raw or "")[:30],
                    "is_remote": job.is_remote,
                    "employment_type": job.employment_type or "",
                    "experience_level": job.experience_level or "",
                    "salary_min": float(job.salary_min) if job.salary_min else None,
                    "salary_max": float(job.salary_max) if job.salary_max else None,
                    "salary_raw": (job.salary_raw or "")[:20],
                    "posted_date": str(job.posted_date) if job.posted_date else "",
                    "fetched_at": job.fetched_at.strftime("%b %d, %H:%M") if job.fetched_at else "",
                    "fetched_at_iso": job.fetched_at.isoformat() if job.fetched_at else "",
                    "sync_status": job.sync_status or "",
                    "has_jd": job.has_description,
                    "jd_label": "yes" if job.has_description else ("expired" if not job.is_active else "no"),
                    "department": (job.department_normalized or job.department or "")[:40],
                    "state": (job.state or "")[:48],
                    "country": (job.country or "")[:48],
                    "education_required": job.education_required or "",
                    "languages_required": (job.languages_required or [])[:3],
                    "licenses_required": (job.licenses_required or [])[:3],
                    "clearance_required": bool(job.clearance_required),
                    "clearance_level": (job.clearance_level or "")[:64],
                    "schedule_type": (job.schedule_type or "")[:32],
                    "shift_schedule": (job.shift_schedule or "")[:64],
                    "travel_pct_min": job.travel_pct_min,
                    "travel_pct_max": job.travel_pct_max,
                    "resume_ready_score": job.resume_ready_score,
                    "classification_confidence": job.classification_confidence,
                    "quality_score": job.quality_score,
                    "jd_quality_score": job.jd_quality_score,
                    "certifications": (job.certifications or [])[:3],
                    "keywords": (job.title_keywords or job.job_keywords or [])[:4],
                    "company_industry": (job.company_industry or "")[:64],
                    "company_size": (job.company_size or "")[:32],
                    "stage": job.pipeline_stage_label(),
                    "owner_pipeline": job.owner_pipeline_label(),
                    "retry_count": job.retry_count_estimate(),
                    "last_error": job.last_error_text(),
                    "detail_url": reverse("harvest-rawjob-detail", args=[job.pk]),
                    "resume_jd_usable": jd_gate.usable,
                    "resume_jd_reason_code": jd_gate.reason_code,
                    "resume_jd_reason_text": jd_gate.reason_text,
                    "word_count": jd_gate.word_count,
                })

            return JsonResponse({
                "jobs": jobs_data,
                "has_next": page_obj.has_next(),
                "next_page": page_obj.next_page_number() if page_obj.has_next() else None,
                "total": paginator.count,
                "num_pages": paginator.num_pages,
            })
        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        # No select_related — JOINs add 20x overhead on 122k rows; all displayed
        # fields (company_name, platform_slug) are denormalised directly on RawJob.
        qs = RawJob.objects.order_by("-fetched_at")

        q = self.request.GET.get("q", "").strip()
        if q:
            qs = qs.filter(
                Q(title__icontains=q)
                | Q(company_name__icontains=q)
                | Q(skills__icontains=q)
                | Q(job_keywords__icontains=q)
                | Q(title_keywords__icontains=q)
                | Q(description_clean__icontains=q)
            )

        company_id_f = self.request.GET.get("company_id", "").strip()
        if company_id_f.isdigit():
            qs = qs.filter(company_id=int(company_id_f))

        label_pk_f = self.request.GET.get("label_pk", "").strip()
        if label_pk_f.isdigit():
            qs = qs.filter(platform_label_id=int(label_pk_f))

        platform_f = self.request.GET.get("platform", "").strip()
        if platform_f:
            qs = qs.filter(platform_slug=platform_f)

        location_f = self.request.GET.get("location_type", "").strip()
        if location_f:
            qs = qs.filter(location_type=location_f)

        employment_f = self.request.GET.get("employment_type", "").strip()
        if employment_f:
            qs = qs.filter(employment_type=employment_f)

        exp_f = self.request.GET.get("experience_level", "").strip()
        if exp_f:
            qs = qs.filter(experience_level=exp_f)

        dept_f = self.request.GET.get("department", "").strip()
        if dept_f:
            qs = qs.filter(
                Q(department_normalized__icontains=dept_f)
                | Q(department__icontains=dept_f)
            )

        country_f = self.request.GET.get("country", "").strip()
        if country_f:
            qs = qs.filter(country__icontains=country_f)

        state_f = self.request.GET.get("state", "").strip()
        if state_f:
            qs = qs.filter(state__icontains=state_f)

        edu_f = self.request.GET.get("education_required", "").strip()
        if edu_f:
            qs = qs.filter(education_required=edu_f)

        years_min_f = self.request.GET.get("years_min", "").strip()
        if years_min_f.isdigit():
            qs = qs.filter(years_required__gte=int(years_min_f))

        years_max_f = self.request.GET.get("years_max", "").strip()
        if years_max_f.isdigit():
            qs = qs.filter(years_required__lte=int(years_max_f))

        salary_min_from_f = self.request.GET.get("salary_min_from", "").strip()
        try:
            if salary_min_from_f:
                qs = qs.filter(salary_min__gte=float(salary_min_from_f))
        except ValueError:
            pass

        salary_max_to_f = self.request.GET.get("salary_max_to", "").strip()
        try:
            if salary_max_to_f:
                qs = qs.filter(salary_max__lte=float(salary_max_to_f))
        except ValueError:
            pass

        clear_f = self.request.GET.get("clearance_required", "").strip()
        if clear_f == "1":
            qs = qs.filter(clearance_required=True)
        elif clear_f == "0":
            qs = qs.filter(clearance_required=False)

        clearance_level_f = self.request.GET.get("clearance_level", "").strip()
        if clearance_level_f:
            qs = qs.filter(clearance_level__icontains=clearance_level_f)

        lang_f = self.request.GET.get("language", "").strip()
        if lang_f:
            try:
                qs = qs.filter(languages_required__contains=[lang_f])
            except Exception:
                qs = qs.filter(languages_required__icontains=lang_f)

        shift_f = self.request.GET.get("shift_schedule", "").strip()
        if shift_f:
            qs = qs.filter(shift_schedule__icontains=shift_f)

        schedule_f = self.request.GET.get("schedule_type", "").strip()
        if schedule_f:
            qs = qs.filter(schedule_type__icontains=schedule_f)

        weekend_f = self.request.GET.get("weekend_required", "").strip()
        if weekend_f == "1":
            qs = qs.filter(weekend_required=True)
        elif weekend_f == "0":
            qs = qs.filter(weekend_required=False)

        travel_min_f = self.request.GET.get("travel_min", "").strip()
        if travel_min_f.isdigit():
            qs = qs.filter(travel_pct_max__gte=int(travel_min_f))

        travel_max_f = self.request.GET.get("travel_max", "").strip()
        if travel_max_f.isdigit():
            qs = qs.filter(travel_pct_min__lte=int(travel_max_f))

        license_f = self.request.GET.get("license", "").strip()
        if license_f:
            try:
                qs = qs.filter(licenses_required__contains=[license_f])
            except Exception:
                qs = qs.filter(licenses_required__icontains=license_f)

        encouraged_f = self.request.GET.get("encouraged", "").strip()
        if encouraged_f:
            try:
                qs = qs.filter(encouraged_to_apply__contains=[encouraged_f])
            except Exception:
                qs = qs.filter(encouraged_to_apply__icontains=encouraged_f)

        cert_f = self.request.GET.get("certification", "").strip()
        if cert_f:
            try:
                qs = qs.filter(certifications__contains=[cert_f])
            except Exception:
                qs = qs.filter(certifications__icontains=cert_f)

        benefit_f = self.request.GET.get("benefit", "").strip()
        if benefit_f:
            try:
                qs = qs.filter(benefits_list__contains=[benefit_f])
            except Exception:
                qs = qs.filter(benefits_list__icontains=benefit_f)

        industry_f = self.request.GET.get("company_industry", "").strip()
        if industry_f:
            qs = qs.filter(company_industry__icontains=industry_f)

        company_stage_f = self.request.GET.get("company_stage", "").strip()
        if company_stage_f:
            qs = qs.filter(company_stage__icontains=company_stage_f)

        size_f = self.request.GET.get("company_size", "").strip()
        if size_f:
            qs = qs.filter(
                Q(company_size__icontains=size_f)
                | Q(company_employee_count_band__icontains=size_f)
            )

        funding_f = self.request.GET.get("company_funding", "").strip()
        if funding_f:
            qs = qs.filter(company_funding__icontains=funding_f)

        resume_score_f = self.request.GET.get("resume_ready_min", "").strip()
        try:
            if resume_score_f:
                qs = qs.filter(resume_ready_score__gte=float(resume_score_f))
        except ValueError:
            pass

        conf_min_f = self.request.GET.get("classification_min_conf", "").strip()
        try:
            if conf_min_f:
                qs = qs.filter(classification_confidence__gte=float(conf_min_f))
        except ValueError:
            pass

        founded_from = self.request.GET.get("founded_from", "").strip()
        if founded_from.isdigit():
            qs = qs.filter(company_founding_year__gte=int(founded_from))

        founded_to = self.request.GET.get("founded_to", "").strip()
        if founded_to.isdigit():
            qs = qs.filter(company_founding_year__lte=int(founded_to))

        sync_f = self.request.GET.get("sync_status", "").strip()
        if sync_f:
            qs = qs.filter(sync_status=sync_f)

        stage_f = self.request.GET.get("stage", "").strip().upper()
        if stage_f == "FETCHED":
            qs = qs.filter(has_description=False, sync_status=RawJob.SyncStatus.PENDING)
        elif stage_f == "PARSED":
            qs = qs.filter(has_description=True, quality_score__isnull=True, jd_quality_score__isnull=True, sync_status=RawJob.SyncStatus.PENDING)
        elif stage_f == "ENRICHED":
            qs = qs.filter(Q(quality_score__isnull=False) | Q(jd_quality_score__isnull=False), classification_confidence__isnull=True, sync_status=RawJob.SyncStatus.PENDING)
        elif stage_f == "CLASSIFIED":
            qs = qs.filter(classification_confidence__gte=0.01, classification_confidence__lt=0.55, sync_status=RawJob.SyncStatus.PENDING)
        elif stage_f == "READY":
            qs = qs.filter(has_description=True, classification_confidence__gte=0.55, is_active=True, sync_status=RawJob.SyncStatus.PENDING)
        elif stage_f == "SYNCED":
            qs = qs.filter(sync_status=RawJob.SyncStatus.SYNCED)
        elif stage_f == "FAILED":
            qs = qs.filter(sync_status=RawJob.SyncStatus.FAILED)
        elif stage_f == "DUPLICATE":
            qs = qs.filter(sync_status=RawJob.SyncStatus.SKIPPED)

        remote_f = self.request.GET.get("is_remote", "").strip()
        if remote_f == "1":
            qs = qs.filter(is_remote=True)
        elif remote_f == "0":
            qs = qs.filter(is_remote=False)

        active_f = self.request.GET.get("is_active", "").strip()
        if active_f == "1":
            qs = qs.filter(is_active=True)
        elif active_f == "0":
            qs = qs.filter(is_active=False)

        jd_f = self.request.GET.get("has_jd", "").strip()
        if jd_f == "1":
            qs = qs.filter(has_description=True)
        elif jd_f == "0":
            qs = qs.filter(has_description=False)

        resume_jd_f = self.request.GET.get("resume_jd", "").strip()
        if resume_jd_f == "ready":
            qs = qs.filter(
                has_description=True,
                is_active=True,
                word_count__gte=max(1, int(getattr(settings, "RESUME_JD_MIN_WORDS", 80))),
                classification_confidence__gte=float(
                    getattr(settings, "RESUME_JD_MIN_CLASSIFICATION_CONFIDENCE", 0.35)
                ),
            )
        elif resume_jd_f == "blocked":
            min_words = max(1, int(getattr(settings, "RESUME_JD_MIN_WORDS", 80)))
            min_conf = float(getattr(settings, "RESUME_JD_MIN_CLASSIFICATION_CONFIDENCE", 0.35))
            qs = qs.filter(
                Q(has_description=False)
                | Q(is_active=False)
                | Q(word_count__lt=min_words)
                | Q(classification_confidence__lt=min_conf)
                | Q(classification_confidence__isnull=True)
            )

        # Fetched-date range — uses the indexed fetched_at column so these are
        # fast range scans instead of function-based date extractions.
        from datetime import datetime as _dt, timedelta as _td
        from django.utils.timezone import make_aware as _aware

        fetched_from = self.request.GET.get("fetched_from", "").strip()
        if fetched_from:
            try:
                qs = qs.filter(fetched_at__gte=_aware(_dt.strptime(fetched_from, "%Y-%m-%d")))
            except ValueError:
                pass

        fetched_to = self.request.GET.get("fetched_to", "").strip()
        if fetched_to:
            try:
                next_day = _dt.strptime(fetched_to, "%Y-%m-%d") + _td(days=1)
                qs = qs.filter(fetched_at__lt=_aware(next_day))
            except ValueError:
                pass

        last_hours = self.request.GET.get("last_hours", "").strip()
        if last_hours.isdigit():
            hours = max(1, min(720, int(last_hours)))
            qs = qs.filter(fetched_at__gte=timezone.now() - timedelta(hours=hours))

        pending_age_bucket = self.request.GET.get("pending_age_bucket", "").strip()
        if pending_age_bucket:
            now = timezone.now()
            qs = qs.filter(sync_status=RawJob.SyncStatus.PENDING)
            if pending_age_bucket == "lt_1h":
                qs = qs.filter(fetched_at__gte=now - timedelta(hours=1))
            elif pending_age_bucket == "h_1_6":
                qs = qs.filter(fetched_at__lt=now - timedelta(hours=1), fetched_at__gte=now - timedelta(hours=6))
            elif pending_age_bucket == "h_6_24":
                qs = qs.filter(fetched_at__lt=now - timedelta(hours=6), fetched_at__gte=now - timedelta(hours=24))
            elif pending_age_bucket == "gt_24h":
                qs = qs.filter(fetched_at__lt=now - timedelta(hours=24))

        # Posted-date range (separate from fetched_at)
        date_from = self.request.GET.get("date_from", "").strip()
        if date_from:
            qs = qs.filter(posted_date__gte=date_from)

        date_to = self.request.GET.get("date_to", "").strip()
        if date_to:
            qs = qs.filter(posted_date__lte=date_to)

        return qs

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["active_tab"] = "rawjobs"

        # Unified KPI aggregation with short TTL + invalidation on writes.
        stats = _load_rawjobs_dashboard_stats(force_refresh=False)

        ctx["total_jobs"] = stats["total"]
        ctx["active_jobs"] = stats["active"]
        ctx["remote_jobs"] = stats["remote"]
        ctx["synced_jobs"] = stats["synced"]
        ctx["pending_jobs"] = stats["pending"]
        ctx["failed_jobs"] = stats["failed"]
        ctx["new_today"] = stats["new_today"]
        ctx["missing_description_jobs"] = stats["missing_jd"]
        ctx["missing_jd_expired_jobs"] = stats["expired_missing"]

        # Platform breakdown
        ctx["platform_stats"] = (
            RawJob.objects.values("platform_slug")
            .annotate(count=Count("id"))
            .order_by("-count")
        )

        # Recent batches
        ctx["recent_batches"] = FetchBatch.objects.order_by("-created_at")[:5]

        # Platforms list for filter dropdown
        ctx["platforms"] = JobBoardPlatform.objects.filter(is_enabled=True).order_by("name")

        # Choices for filter dropdowns
        ctx["location_type_choices"] = RawJob.LocationType.choices
        ctx["employment_type_choices"] = RawJob.EmploymentType.choices
        ctx["experience_level_choices"] = RawJob.ExperienceLevel.choices
        ctx["sync_status_choices"] = RawJob.SyncStatus.choices
        ctx["education_required_choices"] = [
            (val, label) for val, label in RawJob._meta.get_field("education_required").choices if val
        ]

        # Filter state
        ctx["q"] = self.request.GET.get("q", "")
        ctx["selected_platform"] = self.request.GET.get("platform", "")
        ctx["selected_location_type"] = self.request.GET.get("location_type", "")
        ctx["selected_employment_type"] = self.request.GET.get("employment_type", "")
        ctx["selected_experience_level"] = self.request.GET.get("experience_level", "")
        ctx["selected_department"] = self.request.GET.get("department", "")
        ctx["selected_country"] = self.request.GET.get("country", "")
        ctx["selected_state"] = self.request.GET.get("state", "")
        ctx["selected_education_required"] = self.request.GET.get("education_required", "")
        ctx["selected_years_min"] = self.request.GET.get("years_min", "")
        ctx["selected_years_max"] = self.request.GET.get("years_max", "")
        ctx["selected_salary_min_from"] = self.request.GET.get("salary_min_from", "")
        ctx["selected_salary_max_to"] = self.request.GET.get("salary_max_to", "")
        ctx["selected_clearance_required"] = self.request.GET.get("clearance_required", "")
        ctx["selected_clearance_level"] = self.request.GET.get("clearance_level", "")
        ctx["selected_language"] = self.request.GET.get("language", "")
        ctx["selected_license"] = self.request.GET.get("license", "")
        ctx["selected_encouraged"] = self.request.GET.get("encouraged", "")
        ctx["selected_certification"] = self.request.GET.get("certification", "")
        ctx["selected_benefit"] = self.request.GET.get("benefit", "")
        ctx["selected_shift_schedule"] = self.request.GET.get("shift_schedule", "")
        ctx["selected_schedule_type"] = self.request.GET.get("schedule_type", "")
        ctx["selected_weekend_required"] = self.request.GET.get("weekend_required", "")
        ctx["selected_travel_min"] = self.request.GET.get("travel_min", "")
        ctx["selected_travel_max"] = self.request.GET.get("travel_max", "")
        ctx["selected_company_industry"] = self.request.GET.get("company_industry", "")
        ctx["selected_company_stage"] = self.request.GET.get("company_stage", "")
        ctx["selected_company_size"] = self.request.GET.get("company_size", "")
        ctx["selected_company_funding"] = self.request.GET.get("company_funding", "")
        ctx["selected_resume_ready_min"] = self.request.GET.get("resume_ready_min", "")
        ctx["selected_classification_min_conf"] = self.request.GET.get("classification_min_conf", "")
        ctx["selected_founded_from"] = self.request.GET.get("founded_from", "")
        ctx["selected_founded_to"] = self.request.GET.get("founded_to", "")
        ctx["selected_sync_status"] = self.request.GET.get("sync_status", "")
        ctx["selected_stage"] = self.request.GET.get("stage", "")
        ctx["selected_pending_age_bucket"] = self.request.GET.get("pending_age_bucket", "")
        ctx["selected_is_remote"] = self.request.GET.get("is_remote", "")
        ctx["selected_is_active"] = self.request.GET.get("is_active", "")
        ctx["selected_has_jd"] = self.request.GET.get("has_jd", "")
        ctx["selected_resume_jd"] = self.request.GET.get("resume_jd", "")
        ctx["selected_fetched_from"] = self.request.GET.get("fetched_from", "")
        ctx["selected_fetched_to"] = self.request.GET.get("fetched_to", "")
        ctx["selected_last_hours"] = self.request.GET.get("last_hours", "")
        ctx["selected_date_from"] = self.request.GET.get("date_from", "")
        ctx["selected_date_to"] = self.request.GET.get("date_to", "")
        ctx["selected_company_id"] = self.request.GET.get("company_id", "")
        ctx["selected_label_pk"] = self.request.GET.get("label_pk", "")
        paginator = ctx.get("paginator")
        ctx["jobs_total_filtered"] = paginator.count if paginator else 0

        # Workflow analytics for new control tabs.
        ctx["raw_insights"] = _raw_jobs_workflow_insights(stale_pending_hours=6)

        # Running batch check (for live polling)
        ctx["has_running_batch"] = FetchBatch.objects.filter(status="RUNNING").exists()

        # Cooldown for Full Crawl button (2hr from last full batch)
        COOLDOWN_HOURS = 2
        last_full_batch = (
            FetchBatch.objects.filter(status__in=["COMPLETED", "PARTIAL", "RUNNING", "CANCELLED"])
            .exclude(name__icontains="PLATFORM CHECK")
            .exclude(name__icontains="QUICK SYNC")
            .order_by("-created_at")
            .first()
        )
        ctx["last_full_batch"] = last_full_batch
        if last_full_batch:
            elapsed_sec = (timezone.now() - last_full_batch.created_at).total_seconds()
            ctx["cooldown_remaining_sec"] = max(0, int(COOLDOWN_HOURS * 3600 - elapsed_sec))
        else:
            ctx["cooldown_remaining_sec"] = 0

        return ctx


class RawJobDetailView(SuperuserRequiredMixin, DetailView):
    model = RawJob
    template_name = "harvest/rawjob_detail.html"
    context_object_name = "rawjob"  # template uses {{ rawjob.* }}

    def get_queryset(self):
        return RawJob.objects.select_related("company", "job_platform", "platform_label")

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["resume_jd_gate"] = evaluate_raw_job_resume_gate(self.object)
        return ctx


class RawJobCheckLiveStatusView(SuperuserRequiredMixin, View):
    """POST — recheck a single raw-job posting URL and update is_active immediately."""

    def post(self, request, pk):
        from .url_health import check_job_posting_live

        raw_job = get_object_or_404(RawJob, pk=pk)
        url = (raw_job.original_url or "").strip()
        if not url:
            messages.error(request, "No source URL available for this row.")
            return redirect("harvest-rawjob-detail", pk=pk)

        result = check_job_posting_live(url, platform_slug=(raw_job.platform_slug or ""))
        payload = dict(raw_job.raw_payload or {})
        payload["link_health"] = {
            "is_live": bool(result.is_live),
            "reason": result.reason,
            "status_code": int(result.status_code or 0),
            "checked_at": timezone.now().isoformat(),
            "final_url": result.final_url,
        }
        raw_job.is_active = bool(result.is_live)
        raw_job.raw_payload = payload
        raw_job.save(update_fields=["is_active", "raw_payload", "updated_at"])
        _invalidate_rawjobs_dashboard_cache()

        if result.is_live:
            messages.success(
                request,
                f"Link health check: ACTIVE ({result.reason}, HTTP {result.status_code}).",
            )
        else:
            messages.warning(
                request,
                f"Link health check: INACTIVE ({result.reason}, HTTP {result.status_code}).",
            )
        return redirect("harvest-rawjob-detail", pk=pk)


@method_decorator(never_cache, name="dispatch")
class RawJobResumeProfileView(SuperuserRequiredMixin, View):
    """JSON export used by resume generation pipeline."""

    def get(self, request, pk):
        raw_job = get_object_or_404(
            RawJob.objects.select_related("company", "job_platform", "platform_label"),
            pk=pk,
        )
        jd_gate = evaluate_raw_job_resume_gate(raw_job)
        if not jd_gate.usable:
            return JsonResponse(
                {
                    "ok": False,
                    "raw_job_id": raw_job.pk,
                    "error": "JD is not usable for resume generation.",
                    "reason_code": jd_gate.reason_code,
                    "reason_text": jd_gate.reason_text,
                    "word_count": jd_gate.word_count,
                    "min_words": jd_gate.min_words,
                },
                status=422,
            )
        return JsonResponse(
            {
                "ok": True,
                "raw_job_id": raw_job.pk,
                "profile": build_resume_job_profile(raw_job),
                "resume_jd_gate": jd_gate.asdict(),
            }
        )


class FetchBatchListView(SuperuserRequiredMixin, ListView):
    model = FetchBatch
    template_name = "harvest/rawjobs_batches.html"
    context_object_name = "batches"
    paginate_by = 20

    def get_queryset(self):
        return FetchBatch.objects.prefetch_related("company_runs").order_by("-created_at")

    def get(self, request, *args, **kwargs):
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            qs = self.get_queryset()[:50]
            batches = []
            for b in qs:
                batches.append({
                    "id": b.pk,
                    "name": b.name,
                    "status": b.status,
                    "platform_filter": b.platform_filter,
                    "total": b.total_companies,
                    "completed": b.completed_companies,
                    "failed": b.failed_companies,
                    "total_jobs_found": b.total_jobs_found,
                    "total_jobs_new": b.total_jobs_new,
                    "progress_pct": b.progress_pct,
                    "created_at": b.created_at.strftime("%Y-%m-%d %H:%M") if b.created_at else "",
                })
            return JsonResponse({"batches": batches})
        return super().get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["active_tab"] = "rawjobs"
        return ctx


class CompanyFetchStatusView(SuperuserRequiredMixin, ListView):
    template_name = "harvest/rawjobs_company_status.html"
    context_object_name = "runs"
    paginate_by = 50

    def get_queryset(self):
        qs = CompanyFetchRun.objects.select_related(
            "label__company", "label__platform", "batch"
        ).order_by("-started_at")

        status_f = self.request.GET.get("status", "").strip()
        if status_f:
            qs = qs.filter(status=status_f)

        platform_f = self.request.GET.get("platform", "").strip()
        if platform_f:
            qs = qs.filter(label__platform__slug=platform_f)

        return qs

    def get(self, request, *args, **kwargs):
        # JSON response for AJAX calls from the rawjobs_list template
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            qs = self.get_queryset()[:100]
            runs = []
            for run in qs:
                runs.append({
                    "label_pk": run.label_id,
                    "company_name": run.label.company.name if run.label and run.label.company else "",
                    "platform_slug": run.label.platform.slug if run.label and run.label.platform else "",
                    "status": run.status,
                    "jobs_found": run.jobs_found,
                    "jobs_new": run.jobs_new,
                    "started_at": run.started_at.strftime("%Y-%m-%d %H:%M") if run.started_at else "",
                })
            return JsonResponse({"runs": runs})
        return super().get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["active_tab"] = "rawjobs"
        ctx["status_choices"] = CompanyFetchRun.Status.choices
        ctx["platforms"] = JobBoardPlatform.objects.filter(is_enabled=True).order_by("name")
        ctx["selected_status"] = self.request.GET.get("status", "")
        ctx["selected_platform"] = self.request.GET.get("platform", "")
        return ctx


class TriggerCompanyFetchView(SuperuserRequiredMixin, View):
    """AJAX POST — triggers a single-company raw job fetch."""
    def post(self, request):
        from .tasks import fetch_raw_jobs_for_company_task
        label_pk = request.POST.get("label_pk", "").strip()
        if not label_pk:
            return JsonResponse({"ok": False, "error": "Missing label_pk"}, status=400)
        try:
            label_pk = int(label_pk)
        except ValueError:
            return JsonResponse({"ok": False, "error": "Invalid label_pk"}, status=400)

        task = fetch_raw_jobs_for_company_task.delay(label_pk, None, "MANUAL")
        return JsonResponse({"ok": True, "task_id": task.id})


class FetchCooldownStatusView(SuperuserRequiredMixin, View):
    """GET — JSON: cooldown status for the Full Crawl button."""
    def get(self, request):
        COOLDOWN_HOURS = 2
        last_full = (
            FetchBatch.objects.filter(
                status__in=["COMPLETED", "PARTIAL", "RUNNING", "CANCELLED"],
            )
            .exclude(name__icontains="PLATFORM CHECK")
            .exclude(name__icontains="QUICK SYNC")
            .order_by("-created_at")
            .first()
        )
        if not last_full:
            return JsonResponse({"on_cooldown": False, "remaining_sec": 0, "last_batch_at": None})

        elapsed = (timezone.now() - last_full.created_at).total_seconds()
        cooldown_sec = COOLDOWN_HOURS * 3600
        remaining = max(0, int(cooldown_sec - elapsed))
        return JsonResponse({
            "on_cooldown": remaining > 0,
            "remaining_sec": remaining,
            "last_batch_at": last_full.created_at.isoformat(),
            "last_batch_name": last_full.name or f"Batch #{last_full.pk}",
            "last_batch_status": last_full.status,
        })


class TriggerBatchFetchView(SuperuserRequiredMixin, View):
    """POST — triggers a batch raw job fetch for all or filtered companies.

    fetch_mode:
      "quick"  → incremental (since_hours=25, no fetch_all) — fast daily sync
      "full"   → full crawl (fetch_all=True, all pages) — slow, 2hr cooldown enforced
      "test"   → test mode (test_mode=1, companies_per_platform, test_max_jobs)
      ""       → filtered batch (platform_slug selector form)
    """
    COOLDOWN_HOURS = 2

    def post(self, request):
        from .tasks import fetch_raw_jobs_batch_task
        fetch_mode = request.POST.get("fetch_mode", "").strip()  # "quick" | "full" | "test" | ""
        platform_slug = request.POST.get("platform_slug", "").strip() or None
        batch_name = request.POST.get("batch_name", "").strip() or None
        test_mode = request.POST.get("test_mode", "") in ("1", "true", "True", "yes")
        test_max_jobs = int(request.POST.get("test_max_jobs", "10") or "10")
        companies_per_platform = int(request.POST.get("companies_per_platform", "1") or "1")

        # skip_platforms: comma-separated string OR multiple hidden inputs
        skip_raw = request.POST.get("skip_platforms", "").strip()
        skip_platforms = [s.strip() for s in skip_raw.split(",") if s.strip()] if skip_raw else []

        # ── Mode: Quick Sync ─────────────────────────────────────────────────
        if fetch_mode == "quick":
            ts = timezone.now().strftime("%Y-%m-%d %H:%M")
            task = fetch_raw_jobs_batch_task.delay(
                platform_slug=platform_slug or None,
                batch_name=batch_name or f"Quick Sync (25h) — {ts}",
                triggered_user_id=request.user.id,
                test_mode=False,
                fetch_all=False,        # incremental: since_hours=25 only
                min_hours_since_fetch=6,
            )
            messages.success(
                request,
                f"Quick Sync started — fetching new/updated jobs from the last 25h "
                f"(Task: {task.id[:8]}…). Much faster than a full crawl.",
            )
            return redirect_with_task_progress("harvest-rawjobs", task.id, "Quick Sync (25h)")

        # ── Mode: Full Crawl — enforce 2-hour cooldown ────────────────────────
        if fetch_mode == "full":
            last_full = (
                FetchBatch.objects.filter(status__in=["COMPLETED", "PARTIAL", "RUNNING", "CANCELLED"])
                .exclude(name__icontains="PLATFORM CHECK")
                .exclude(name__icontains="QUICK SYNC")
                .order_by("-created_at")
                .first()
            )
            if last_full:
                elapsed = (timezone.now() - last_full.created_at).total_seconds()
                cooldown_sec = self.COOLDOWN_HOURS * 3600
                remaining = max(0, int(cooldown_sec - elapsed))
                if remaining > 0:
                    mins = remaining // 60
                    secs = remaining % 60
                    messages.error(
                        request,
                        f"⏱ Full Crawl on cooldown — last batch ran {int(elapsed//60)} min ago. "
                        f"Wait {mins}m {secs}s before starting another full crawl. "
                        f"Use Quick Sync (25h) for an incremental update now.",
                    )
                    return redirect("harvest-rawjobs")
            ts = timezone.now().strftime("%Y-%m-%d %H:%M")
            task = fetch_raw_jobs_batch_task.delay(
                platform_slug=platform_slug or None,
                batch_name=batch_name or f"Full Crawl — {ts}",
                triggered_user_id=request.user.id,
                test_mode=False,
                fetch_all=True,         # full pagination — all pages, all companies
                min_hours_since_fetch=6,
            )
            messages.success(
                request,
                f"Full Crawl started — fetching ALL jobs from every platform "
                f"(Task: {task.id[:8]}…). This may take 30–60+ minutes.",
            )
            return redirect_with_task_progress("harvest-rawjobs", task.id, "Full Crawl")

        # ── Mode: Test / Platform Check ───────────────────────────────────────
        if test_mode or fetch_mode == "test":
            task = fetch_raw_jobs_batch_task.delay(
                platform_slug=platform_slug,
                batch_name=batch_name,
                triggered_user_id=request.user.id,
                test_mode=True,
                test_max_jobs=test_max_jobs,
                companies_per_platform=companies_per_platform,
                skip_platforms=skip_platforms or None,
                fetch_all=False,
            )
            skip_note = f", skip: {', '.join(skip_platforms)}" if skip_platforms else ""
            messages.success(
                request,
                f"Platform check started — {companies_per_platform} co/platform, up to {test_max_jobs} jobs each{skip_note} (Task: {task.id[:8]}…)",
            )
            return redirect_with_task_progress("harvest-rawjobs", task.id, f"Platform check ({test_max_jobs} jobs/platform)")

        # ── Mode: Filtered Batch (platform selector form) ─────────────────────
        task = fetch_raw_jobs_batch_task.delay(
            platform_slug=platform_slug,
            batch_name=batch_name,
            triggered_user_id=request.user.id,
            test_mode=False,
            skip_platforms=skip_platforms or None,
            fetch_all=True,
        )
        messages.success(
            request,
            f"Raw jobs batch fetch started"
            + (f" for platform '{platform_slug}'" if platform_slug else " for all platforms")
            + f" (Task: {task.id[:8]}...)",
        )
        return redirect_with_task_progress("harvest-rawjobs", task.id, "Raw jobs batch fetch")


class StopBatchView(SuperuserRequiredMixin, View):
    """GET — redirect to batch list. POST — cancel a running FetchBatch and revoke its tasks."""

    def get(self, request):
        """Direct browser navigation → just go to the batch list page."""
        return redirect("harvest-rawjobs")

    def post(self, request):
        from celery import current_app

        batch_id = request.POST.get("batch_id") or None
        if batch_id:
            batch = get_object_or_404(FetchBatch, pk=batch_id)
        else:
            batch = FetchBatch.objects.filter(status="RUNNING").order_by("-created_at").first()

        if not batch:
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return JsonResponse({"ok": False, "error": "No running batch found."}, status=404)
            messages.warning(request, "No running batch found.")
            return redirect("harvest-rawjobs")

        if batch.status not in ("RUNNING", "PENDING"):
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return JsonResponse({"ok": False, "error": f"Batch is already {batch.status}."})
            messages.warning(request, f"Batch #{batch.pk} is already {batch.status}.")
            return redirect("harvest-rawjobs")

        # 1. Revoke the main batch orchestration task (if it's still queued/running)
        if batch.task_id:
            current_app.control.revoke(batch.task_id, terminate=True, signal="SIGTERM")

        # 2. Revoke all PENDING/RUNNING per-company tasks for this batch
        pending_runs = CompanyFetchRun.objects.filter(
            batch=batch, status__in=["PENDING", "RUNNING"]
        ).exclude(task_id="").exclude(task_id=None)
        task_ids = list(pending_runs.values_list("task_id", flat=True))
        if task_ids:
            current_app.control.revoke(task_ids, terminate=True, signal="SIGTERM")

        # 3. Mark company runs as SKIPPED
        pending_runs.update(status="SKIPPED")

        # 4. Mark batch as CANCELLED
        batch.status = "CANCELLED"
        if not batch.completed_at:
            batch.completed_at = timezone.now()
        batch.save(update_fields=["status", "completed_at"])

        logger.info(
            "[HARVEST] Batch #%s cancelled by %s — revoked %d task(s)",
            batch.pk, request.user.username, len(task_ids) + (1 if batch.task_id else 0),
        )

        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return JsonResponse({"ok": True, "batch_id": batch.pk, "revoked": len(task_ids)})

        messages.success(request, f"Batch #{batch.pk} cancelled — {len(task_ids)} pending tasks revoked.")
        return redirect("harvest-rawjobs")


class RunEnrichExistingView(SuperuserRequiredMixin, View):
    """POST — run enrichment engine on all jobs already in DB (no HTTP)."""

    def post(self, request):
        from .tasks import enrich_existing_jobs_task

        platform_slug  = request.POST.get("platform_slug", "").strip() or None
        batch_size     = int(request.POST.get("batch_size", "2000") or "2000")
        only_unenriched = request.POST.get("only_unenriched", "1") not in ("0", "false", "False")

        task = enrich_existing_jobs_task.delay(
            batch_size=batch_size,
            platform_slug=platform_slug,
            only_unenriched=only_unenriched,
        )
        label = f"platform={platform_slug}" if platform_slug else "all platforms"
        messages.success(
            request,
            f"Enrichment started ({label}, batch={batch_size:,}, unenriched_only={only_unenriched}) — Task {task.id[:8]}…",
        )
        return redirect_with_task_progress(
            "harvest-rawjobs", task.id,
            f"Enrich existing jobs ({label})",
        )


class RunBackfillResumeContractView(SuperuserRequiredMixin, View):
    """POST — backfill expanded resume-classification contract fields."""

    def post(self, request):
        from .tasks import backfill_resume_contract_task

        batch_size = int(request.POST.get("batch_size", "1500") or "1500")
        offset = int(request.POST.get("offset", "0") or "0")
        task = backfill_resume_contract_task.delay(batch_size=batch_size, offset=offset)
        messages.success(
            request,
            f"Resume contract backfill started (batch={batch_size:,}, offset={offset:,}) — Task {task.id[:8]}…",
        )
        return redirect_with_task_progress(
            "harvest-rawjobs",
            task.id,
            "Backfill resume contract fields",
        )


class RunBackfillDescriptionsView(SuperuserRequiredMixin, View):
    """POST — launch backfill_descriptions_task to fetch JDs for jobs that have none."""

    def post(self, request):
        from .tasks import backfill_descriptions_task

        platform_slug = request.POST.get("platform_slug", "").strip() or None
        batch_size = int(request.POST.get("batch_size", "200") or "200")
        parallel_workers = int(request.POST.get("parallel_workers", "4") or "4")
        offset = int(request.POST.get("offset", "0") or "0")

        task = backfill_descriptions_task.delay(
            batch_size=batch_size,
            parallel_workers=parallel_workers,
            platform_slug=platform_slug,
            offset=offset,
        )
        label = f"platform={platform_slug}" if platform_slug else "all platforms"
        messages.success(
            request,
            f"Description backfill started ({label}, batch={batch_size}, workers={parallel_workers}) — Task {task.id[:8]}…",
        )
        return redirect_with_task_progress("harvest-rawjobs", task.id, f"Backfill descriptions ({label})")


class TaskMonitorView(SuperuserRequiredMixin, TemplateView):
    template_name = "harvest/task_monitor.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["active_tab"] = "monitor"
        return ctx


# Human-readable names for every task in the system
_TASK_LABELS = {
    "harvest.backfill_descriptions":              ("JD Backfill",         "#f97316"),
    "harvest.backfill_descriptions_chunk":        ("JD Backfill Chunk",   "#f97316"),
    "harvest.fetch_raw_jobs_batch":               ("Harvest Batch",       "#6366f1"),
    "harvest.harvest_jobs":                       ("Harvest Jobs",        "#6366f1"),
    "harvest.sync_harvested_to_pool":             ("Pool Sync",           "#10b981"),
    "harvest.detect_company_platforms":           ("Platform Detection",  "#38bdf8"),
    "harvest.verify_all_portals":                 ("Portal Verify",       "#a855f7"),
    "harvest.enrich_existing_jobs":               ("Enrichment",          "#eab308"),
    "harvest.backfill_platform_labels_from_jobs": ("Label Backfill",      "#64748b"),
    "harvest.cleanup_harvested_jobs":             ("Cleanup",             "#64748b"),
    "harvest.jarvis_ingest":                      ("Jarvis Import",       "#ec4899"),
    "harvest.retry_failed_raw_jobs":              ("Retry Failed",        "#ef4444"),
    "core.tasks.poll_email_ingest_task":          ("Email Ingest",        "#0ea5e9"),
}


class TaskMonitorAPIView(SuperuserRequiredMixin, View):
    """GET — JSON snapshot of running tasks, recent history, workers, and key stats."""

    def get(self, request):
        from celery import current_app
        from celery.result import AsyncResult

        # ── Active tasks from all workers ────────────────────────────────────
        active_tasks = []
        try:
            inspect = current_app.control.inspect(timeout=2)
            active_map = inspect.active() or {}
            for worker_name, tasks in active_map.items():
                for t in tasks:
                    task_id = t.get("id", "")
                    name = t.get("name", "")
                    label, color = _TASK_LABELS.get(name, (name.split(".")[-1].replace("_", " ").title(), "#64748b"))
                    started = t.get("time_start")

                    # Pull live progress from result backend
                    percent, message, detail = 0, "Running…", {}
                    try:
                        res = AsyncResult(task_id)
                        if res.state == "PROGRESS":
                            meta = res.info or {}
                            percent = meta.get("percent", 0)
                            message = meta.get("message", "Running…")
                            detail = meta.get("detail", {})
                    except Exception:
                        pass

                    active_tasks.append({
                        "id": task_id,
                        "name": name,
                        "label": label,
                        "color": color,
                        "worker": worker_name.split("@")[0],
                        "percent": percent,
                        "message": message[:120],
                        "started": int(started) if started else None,
                        "updated": detail.get("updated", 0),
                        "skipped": detail.get("skipped", 0),
                        "failed": detail.get("failed", 0),
                        "speed": detail.get("speed", 0),
                        "eta": detail.get("eta", ""),
                        "remaining": detail.get("remaining_global", 0),
                    })
        except Exception:
            pass

        # ── Worker health ─────────────────────────────────────────────────────
        workers = []
        try:
            stats_map = current_app.control.inspect(timeout=2).stats() or {}
            for wname, info in stats_map.items():
                pool = info.get("pool", {})
                workers.append({
                    "name": wname.split("@")[0] + "@" + wname.split("@")[1][:8],
                    "concurrency": pool.get("max-concurrency", "?"),
                    "processes": len(pool.get("processes", [])),
                    "queues": [q["name"] for q in info.get("consumer", {}).get("queues", [])],
                })
        except Exception:
            pass

        # ── Recent task history ───────────────────────────────────────────────
        recent = []
        try:
            from django_celery_results.models import TaskResult
            qs = TaskResult.objects.exclude(
                task_name="core.tasks.poll_email_ingest_task"
            ).order_by("-date_done")[:30]
            for t in qs:
                name = t.task_name or ""
                label, color = _TASK_LABELS.get(name, (name.split(".")[-1].replace("_", " ").title(), "#64748b"))
                runtime = None
                if t.date_done and t.date_created:
                    runtime = int((t.date_done - t.date_created).total_seconds())
                recent.append({
                    "id": (t.task_id or "")[:8],
                    "label": label,
                    "color": color,
                    "status": t.status or "UNKNOWN",
                    "date_done": t.date_done.strftime("%b %d %H:%M") if t.date_done else "",
                    "runtime_secs": runtime,
                })
        except Exception:
            pass

        # ── Key stats ─────────────────────────────────────────────────────────
        stats = {}
        try:
            from .models import RawJob
            from django.db.models import Count, Q
            agg = RawJob.objects.aggregate(
                total=Count("id"),
                missing_jd=Count("id", filter=Q(has_description=False)),
                pending_sync=Count("id", filter=Q(sync_status="PENDING")),
                locked=Count("id", filter=Q(jd_backfill_locked_at__isnull=False)),
            )
            stats = {
                "total_jobs": agg["total"],
                "missing_jd": agg["missing_jd"],
                "pending_sync": agg["pending_sync"],
                "backfill_in_progress": agg["locked"],
            }
        except Exception:
            pass

        return JsonResponse({
            "active": active_tasks,
            "workers": workers,
            "recent": recent,
            "stats": stats,
        })


class RawJobCompanyBreakdownView(SuperuserRequiredMixin, View):
    """GET ?filter=pending|missing_jd — company-level breakdown for a stat filter."""

    def get(self, request):
        filter_type = request.GET.get("filter", "").strip()

        if filter_type == "pending":
            qs = RawJob.objects.filter(sync_status="PENDING")
        elif filter_type == "missing_jd":
            qs = _raw_jobs_missing_jd_base_qs()
        else:
            return JsonResponse({"error": "Invalid filter. Use pending or missing_jd."}, status=400)

        companies = list(
            qs.values("company_name", "platform_slug")
            .annotate(count=Count("id"))
            .order_by("-count")[:200]
        )
        return JsonResponse({"filter": filter_type, "total": qs.count(), "companies": companies})


@method_decorator(never_cache, name="dispatch")
class RawJobStatsView(SuperuserRequiredMixin, View):
    """Raw Jobs stats page + JSON endpoint for dashboard polling."""
    def get(self, request):
        # Running batch info
        running_batch = FetchBatch.objects.filter(status="RUNNING").order_by("-created_at").first()
        running_company_fetch = CompanyFetchRun.objects.filter(
            status=CompanyFetchRun.Status.RUNNING
        ).exists()
        batch_data = None
        if running_batch:
            batch_data = {
                "id": running_batch.pk,
                "name": running_batch.name,
                "total": running_batch.total_companies,
                "completed": running_batch.completed_companies,
                "failed": running_batch.failed_companies,
                "progress_pct": running_batch.progress_pct,
                "total_jobs_found": running_batch.total_jobs_found,
                "total_jobs_new": running_batch.total_jobs_new,
            }

        # Force refresh while a batch or Jarvis company fetch is running.
        stats = _load_rawjobs_dashboard_stats(force_refresh=bool(running_batch or running_company_fetch))

        payload = {
            "total_jobs": stats["total"],
            "active_jobs": stats["active"],
            "remote_jobs": stats["remote"],
            "synced_jobs": stats["synced"],
            "pending_jobs": stats["pending"],
            "failed_jobs": stats["failed"],
            "new_today": stats["new_today"],
            "missing_description_jobs": stats["missing_jd"],
            "missing_jd_expired_jobs": stats["expired_missing"],
            "running_batch": batch_data,
            "platform_stats": list(
                RawJob.objects.values("platform_slug")
                .annotate(count=Count("id"))
                .order_by("-count")
                .values("platform_slug", "count")
            ),
            "insights": _raw_jobs_workflow_insights(stale_pending_hours=6),
            "meta": {
                "cache": "fresh" if (running_batch or running_company_fetch) else "short_ttl",
                "new_today_basis": "last_24h_fetched",
            },
        }

        fmt = (request.GET.get("format") or "").strip().lower()
        accept = (request.headers.get("Accept") or "").lower()
        is_xhr = request.headers.get("X-Requested-With") == "XMLHttpRequest"
        wants_json = (fmt == "json") or is_xhr or ("application/json" in accept)

        if wants_json:
            return JsonResponse(payload)

        context = {
            "active_tab": "rawjobs",
            "stats_payload": payload,
            "running_batch": payload.get("running_batch"),
            "platform_stats": payload.get("platform_stats", []),
            "insights": payload.get("insights", {}),
            "stats_pretty_json": json.dumps(payload, indent=2, cls=DjangoJSONEncoder),
        }
        return render(request, "harvest/rawjobs_stats.html", context)


@method_decorator(never_cache, name="dispatch")
class RawJobWorkflowInsightsView(SuperuserRequiredMixin, View):
    """JSON endpoint — queue/quality/funnel/platform-health insights for control tabs."""

    def get(self, request):
        stale_hours_raw = (request.GET.get("stale_hours") or "6").strip()
        stale_hours = int(stale_hours_raw) if stale_hours_raw.isdigit() else 6
        stale_hours = max(1, min(72, stale_hours))
        return JsonResponse(
            {
                "ok": True,
                "insights": _raw_jobs_workflow_insights(stale_pending_hours=stale_hours),
            }
        )


# ── Job Jarvis ─────────────────────────────────────────────────────────────────

@method_decorator(never_cache, name="dispatch")
class JarvisView(SuperuserRequiredMixin, TemplateView):
    """
    GET  → show paste form
    POST → queue jarvis_ingest_task, return JSON {"task_id": "..."}
    """
    template_name = "harvest/jarvis.html"

    def get_context_data(self, **kwargs):
        from django.utils import timezone
        from django.db.models import Count
        ctx = super().get_context_data(**kwargs)
        ctx["active_tab"] = "jarvis"

        jarvis_qs = RawJob.objects.filter(platform_slug="jarvis")
        today = timezone.now().date()

        # Core metrics
        ctx["jarvis_total"]   = jarvis_qs.count()
        ctx["jarvis_today"]   = jarvis_qs.filter(fetched_at__date=today).count()
        ctx["jarvis_synced"]  = jarvis_qs.filter(sync_status="SYNCED").count()
        ctx["jarvis_pending"] = jarvis_qs.filter(sync_status="PENDING").count()
        ctx["jarvis_failed"]  = jarvis_qs.filter(sync_status="FAILED").count()
        ctx["jarvis_skipped"] = jarvis_qs.filter(sync_status="SKIPPED").count()
        total = ctx["jarvis_total"] or 1
        ctx["jarvis_success_rate"] = round(ctx["jarvis_synced"] / total * 100)

        # This week
        week_start = today - timezone.timedelta(days=today.weekday())
        ctx["jarvis_this_week"] = jarvis_qs.filter(fetched_at__date__gte=week_start).count()

        # Platform breakdown from detected_ats stored in raw_payload
        # Fall back to grouping by job_platform name for those with a matched platform
        recent_all = (
            jarvis_qs
            .select_related("company", "job_platform")
            .order_by("-fetched_at")[:200]
        )
        platform_counts: dict[str, int] = {}
        for rj in recent_all:
            detected = (
                (rj.raw_payload or {}).get("jarvis_detected_ats")
                or (rj.job_platform.name if rj.job_platform else None)
                or "Unknown"
            )
            key = detected.strip().title() if detected else "Unknown"
            platform_counts[key] = platform_counts.get(key, 0) + 1
        ctx["jarvis_platform_breakdown"] = sorted(
            platform_counts.items(), key=lambda x: x[1], reverse=True
        )[:6]

        # Recent imports (more items for the new sidebar)
        ctx["recent_jarvis"] = list(
            jarvis_qs
            .select_related("company", "job_platform")
            .order_by("-fetched_at")[:15]
        )

        ctx["platforms_supported"] = [
            "Greenhouse", "Lever", "Ashby", "Workday", "Workable",
            "Dayforce", "LinkedIn", "Indeed", "SmartRecruiters", "BambooHR", "Any career page",
        ]
        return ctx

    def post(self, request, *args, **kwargs):
        from .tasks import jarvis_ingest_task
        url = request.POST.get("url", "").strip()
        if not url:
            return JsonResponse({"ok": False, "error": "Please paste a job URL."}, status=400)
        if not url.startswith(("http://", "https://")):
            return JsonResponse({"ok": False, "error": "URL must start with http:// or https://"}, status=400)

        task = jarvis_ingest_task.delay(url, request.user.id)
        return JsonResponse({"ok": True, "task_id": task.id, "url": url})


@method_decorator(never_cache, name="dispatch")
class JarvisStatusView(SuperuserRequiredMixin, View):
    """
    GET ?task_id=xxx → poll task state.

    Returns JSON:
      { state, percent, message, result }   # while running
      { state:"SUCCESS", result:{...} }     # when done
      { state:"FAILURE", error:"..." }      # on error
    """

    def get(self, request, *args, **kwargs):
        from celery.result import AsyncResult
        task_id = request.GET.get("task_id", "").strip()
        if not task_id:
            return JsonResponse({"error": "Missing task_id"}, status=400)

        res = AsyncResult(task_id)
        state = res.state  # PENDING / PROGRESS / SUCCESS / FAILURE

        if state == "PENDING":
            return JsonResponse({"state": "PENDING", "percent": 0, "message": "Queued…"})

        if state == "STARTED":
            return JsonResponse({"state": "STARTED", "percent": 12, "message": "Started…"})

        if state == "RETRY":
            return JsonResponse({"state": "RETRY", "percent": 18, "message": "Retrying…"})

        if state == "PROGRESS":
            meta = res.info or {}
            return JsonResponse({
                "state": "PROGRESS",
                "percent": meta.get("percent", 0),
                "message": meta.get("message", "Working…"),
            })

        if state == "SUCCESS":
            result = res.result or {}
            if not isinstance(result, dict):
                result = {"ok": False, "error": str(result)}
            # Fetch fresh raw_job data if saved
            raw_job_data = None
            if result.get("raw_job_id"):
                try:
                    from .harvesters import get_harvester

                    job = RawJob.objects.select_related(
                        "company",
                        "job_platform",
                        "platform_label",
                        "platform_label__platform",
                    ).get(
                        pk=result["raw_job_id"]
                    )
                    payload = job.raw_payload if isinstance(job.raw_payload, dict) else {}
                    existing_live_job = _find_existing_live_job_for_rawjob(job)
                    tenant_id = (
                        payload.get("jarvis_tenant_id")
                        or (job.platform_label.tenant_id if job.platform_label else "")
                        or ""
                    )
                    company_jobs_url = (
                        payload.get("jarvis_company_jobs_url")
                        or (job.platform_label.career_page_url if job.platform_label else "")
                        or ""
                    )
                    support_slug = (
                        payload.get("jarvis_detected_ats")
                        or (job.platform_label.platform.slug if job.platform_label and job.platform_label.platform else "")
                        or (job.job_platform.slug if job.job_platform else "")
                        or ""
                    )
                    fetch_all_supported = bool(
                        tenant_id and support_slug and get_harvester(support_slug) is not None
                    )
                    raw_job_data = {
                        "id": job.pk,
                        "title": job.title,
                        "company_name": job.company.name if job.company else job.company_name,
                        "company_id": job.company_id,
                        "company_url": f"/companies/{job.company_id}/" if job.company_id else "",
                        "location_raw": job.location_raw,
                        "is_remote": job.is_remote,
                        "location_type": job.location_type,
                        "employment_type": job.get_employment_type_display(),
                        "experience_level": job.get_experience_level_display(),
                        "department": job.department,
                        "salary_raw": job.salary_raw,
                        "salary_min": str(job.salary_min) if job.salary_min else None,
                        "salary_max": str(job.salary_max) if job.salary_max else None,
                        "salary_currency": job.salary_currency,
                        "salary_period": job.salary_period,
                        "quality_score": job.quality_score,
                        "description": (job.description or "")[:3000],
                        "platform_slug": job.platform_slug,
                        "platform_name": job.job_platform.name if job.job_platform else (payload.get("jarvis_detected_ats") or job.platform_slug).title(),
                        "detected_ats": payload.get("jarvis_detected_ats", ""),
                        "original_url": job.original_url,
                        "apply_url": job.apply_url,
                        "posted_date": job.posted_date.isoformat() if job.posted_date else "",
                        "sync_status": job.sync_status,
                        "detail_url": f"/harvest/raw-jobs/{job.pk}/",
                        "duplicate_job_id": existing_live_job.pk if existing_live_job else None,
                        "live_job_id": existing_live_job.pk if existing_live_job else None,
                        "live_job_url": f"/jobs/{existing_live_job.pk}/" if existing_live_job else "",
                        "platform_label_id": job.platform_label_id,
                        "tenant_id": tenant_id,
                        "company_jobs_url": company_jobs_url,
                        "fetch_all_supported": fetch_all_supported,
                    }
                except RawJob.DoesNotExist:
                    pass
            return JsonResponse({
                "state": "SUCCESS",
                "result": result,
                "raw_job": raw_job_data,
            })

        if state == "FAILURE":
            return JsonResponse({
                "state": "FAILURE",
                "error": str(res.result) if res.result else "Task failed",
            })

        # REVOKED or other
        return JsonResponse({"state": state})


class JarvisApproveView(SuperuserRequiredMixin, View):
    """POST { raw_job_id } -> sync RawJob to pool then approve to live in one step."""

    def post(self, request, *args, **kwargs):
        from jobs.models import Job

        raw_job_id = (request.POST.get("raw_job_id") or "").strip()
        if not raw_job_id.isdigit():
            return JsonResponse({"ok": False, "error": "Missing or invalid raw_job_id."}, status=400)

        try:
            raw_job = RawJob.objects.select_related("company", "job_platform").get(pk=int(raw_job_id))
        except RawJob.DoesNotExist:
            return JsonResponse({"ok": False, "error": "Raw job not found."}, status=404)

        try:
            job, created_new = _sync_rawjob_to_pool(raw_job, posted_by=request.user)
        except ValueError as exc:
            return JsonResponse({"ok": False, "error": str(exc)}, status=400)
        except Exception as exc:
            logger.exception("Jarvis approve sync failed for raw_job=%s", raw_job.pk)
            return JsonResponse({"ok": False, "error": f"Failed to sync job: {exc}"}, status=500)

        approved_now = False
        if job.status == Job.Status.POOL:
            job.status = Job.Status.OPEN
            job.validated_by = request.user
            job.validation_run_at = timezone.now()
            job.save(update_fields=["status", "validated_by", "validation_run_at", "updated_at"])
            approved_now = True
            try:
                from jobs.notify import notify_new_open_job_to_consultants, notify_job_pool_status

                notify_new_open_job_to_consultants(job)
                notify_job_pool_status(job, approved=True, actor=request.user)
            except Exception:
                logger.exception("Jarvis approve notifications failed for job=%s", job.pk)
            try:
                from jobs.tasks import generate_job_matches_task

                generate_job_matches_task.delay(job.pk, notify=True)
            except Exception:
                logger.exception("Jarvis approve match task dispatch failed for job=%s", job.pk)

        if job.status not in (Job.Status.OPEN, Job.Status.POOL):
            return JsonResponse(
                {
                    "ok": False,
                    "error": f"Job exists but cannot be auto-approved from status {job.status}.",
                    "job_id": job.pk,
                    "job_url": f"/jobs/{job.pk}/",
                },
                status=409,
            )

        return JsonResponse(
            {
                "ok": True,
                "raw_job_id": raw_job.pk,
                "job_id": job.pk,
                "job_url": f"/jobs/{job.pk}/",
                "created_job": created_new,
                "approved_now": approved_now,
                "job_status": job.status,
            }
        )


@method_decorator(never_cache, name="dispatch")
class JarvisFetchCompanyJobsView(SuperuserRequiredMixin, View):
    """
    POST { raw_job_id } → fetch all jobs for the detected company board.

    Uses the existing `fetch_raw_jobs_for_company_task(fetch_all=True)` pipeline
    so results are deduped/upserted into RawJob.
    """

    def post(self, request, *args, **kwargs):
        from .harvesters import get_harvester
        from .tasks import fetch_raw_jobs_for_company_task, _jarvis_ensure_company_platform_label

        raw_job_id = (request.POST.get("raw_job_id") or "").strip()
        if not raw_job_id.isdigit():
            return JsonResponse({"ok": False, "error": "Missing or invalid raw_job_id."}, status=400)

        try:
            raw_job = RawJob.objects.select_related("company", "job_platform").get(pk=int(raw_job_id))
        except RawJob.DoesNotExist:
            return JsonResponse({"ok": False, "error": "Raw job not found."}, status=404)

        if not raw_job.company_id:
            return JsonResponse({"ok": False, "error": "Company mapping is missing. Re-run Jarvis analyze."}, status=400)

        payload = raw_job.raw_payload if isinstance(raw_job.raw_payload, dict) else {}
        detected_ats = (
            payload.get("jarvis_detected_ats")
            or (raw_job.job_platform.slug if raw_job.job_platform else "")
            or ""
        )
        label, board_ctx = _jarvis_ensure_company_platform_label(
            company=raw_job.company,
            detected_ats=detected_ats,
            source_url=raw_job.original_url or payload.get("jarvis_source_url") or "",
            job_platform=raw_job.job_platform,
        )

        if not label or not label.platform:
            return JsonResponse(
                {"ok": False, "error": "Could not resolve ATS platform for this company URL."},
                status=400,
            )
        if not (label.tenant_id or "").strip():
            return JsonResponse(
                {"ok": False, "error": "Tenant could not be derived from this URL yet."},
                status=400,
            )
        if get_harvester(label.platform.slug) is None:
            return JsonResponse(
                {"ok": False, "error": f"Fetch-all is not supported for platform '{label.platform.slug}' yet."},
                status=400,
            )

        # Fetch-all for large Workday boards can exceed the default 8-minute
        # soft limit. Override per-task limits for Jarvis-triggered full crawls.
        task = fetch_raw_jobs_for_company_task.apply_async(
            kwargs={
                "label_pk": label.pk,
                "batch_id": None,
                "triggered_by": "JARVIS",
                "fetch_all": True,
            },
            soft_time_limit=1800,
            time_limit=2100,
        )

        # Keep payload enriched for UI render
        payload["jarvis_platform_label_id"] = label.pk
        payload["jarvis_tenant_id"] = board_ctx.get("tenant_id") or label.tenant_id
        payload["jarvis_company_jobs_url"] = (
            board_ctx.get("company_jobs_url")
            or label.career_page_url
            or ""
        )
        payload["jarvis_fetch_all_supported"] = True
        raw_job.raw_payload = payload
        raw_job.platform_label = label
        raw_job.save(update_fields=["raw_payload", "platform_label", "updated_at"])

        progress_qs = urlencode(
            {
                "task_id": task.id,
                "label_pk": label.pk,
                "raw_job_id": raw_job.pk,
                "company_name": raw_job.company.name if raw_job.company else (raw_job.company_name or ""),
                "platform_slug": label.platform.slug if label.platform else "",
                "company_jobs_url": payload.get("jarvis_company_jobs_url", ""),
            }
        )
        progress_url = f"{reverse('harvest-jarvis-fetch-all-progress')}?{progress_qs}"

        return JsonResponse(
            {
                "ok": True,
                "task_id": task.id,
                "label_pk": label.pk,
                "platform_slug": label.platform.slug,
                "tenant_id": payload.get("jarvis_tenant_id", ""),
                "company_jobs_url": payload.get("jarvis_company_jobs_url", ""),
                "company_id": raw_job.company_id,
                "company_name": raw_job.company.name if raw_job.company else raw_job.company_name,
                "progress_url": progress_url,
            }
        )


@method_decorator(never_cache, name="dispatch")
class JarvisFetchProgressView(SuperuserRequiredMixin, TemplateView):
    template_name = "harvest/jarvis_fetch_progress.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["active_tab"] = "jarvis"
        ctx["task_id"] = self.request.GET.get("task_id", "").strip()
        ctx["label_pk"] = self.request.GET.get("label_pk", "").strip()
        ctx["raw_job_id"] = self.request.GET.get("raw_job_id", "").strip()
        ctx["company_name"] = self.request.GET.get("company_name", "").strip()
        ctx["platform_slug"] = self.request.GET.get("platform_slug", "").strip()
        ctx["company_jobs_url"] = self.request.GET.get("company_jobs_url", "").strip()
        ctx["jarvis_url"] = reverse("harvest-jarvis")
        ctx["rawjobs_url"] = f"{reverse('harvest-rawjobs')}?_subtab=jobs"
        ctx["progress_api_url"] = reverse("harvest-jarvis-fetch-all-progress-api")
        return ctx


@method_decorator(never_cache, name="dispatch")
class JarvisFetchProgressApiView(SuperuserRequiredMixin, View):
    """Live status payload for the Jarvis fetch-all progress page."""

    def get(self, request, *args, **kwargs):
        from celery.result import AsyncResult

        task_id = request.GET.get("task_id", "").strip()
        if not task_id:
            return JsonResponse({"ok": False, "error": "Missing task_id."}, status=400)

        async_res = AsyncResult(task_id)
        celery_state = (async_res.state or "").upper()
        run = (
            CompanyFetchRun.objects.select_related("label__company", "label__platform")
            .filter(task_id=task_id)
            .order_by("-started_at")
            .first()
        )

        if run:
            state = run.status
            running = state == CompanyFetchRun.Status.RUNNING
            done = state in {
                CompanyFetchRun.Status.SUCCESS,
                CompanyFetchRun.Status.PARTIAL,
                CompanyFetchRun.Status.FAILED,
                CompanyFetchRun.Status.SKIPPED,
            }
            found = int(run.jobs_found or 0)
            processed = int(run.jobs_new + run.jobs_updated + run.jobs_duplicate + run.jobs_failed)
            if done:
                percent = 100
            elif found > 0:
                percent = max(8, min(95, int((processed / max(found, 1)) * 100)))
            elif celery_state == "STARTED":
                percent = 10
            elif celery_state == "RETRY":
                percent = 18
            elif celery_state == "PROGRESS" and isinstance(async_res.info, dict):
                percent = int(async_res.info.get("percent", 15) or 15)
            else:
                percent = 4

            if done:
                if state == CompanyFetchRun.Status.SUCCESS:
                    message = (
                        "Company fetch complete."
                        if found > 0
                        else "Company fetch complete (no jobs returned from board)."
                    )
                elif state == CompanyFetchRun.Status.PARTIAL:
                    if int(run.jobs_failed or 0) > 0:
                        message = f"Company fetch completed with partial failures ({run.jobs_failed} failed)."
                    elif run.error_message:
                        message = run.error_message
                    else:
                        message = "Company fetch completed with warnings."
                elif state == CompanyFetchRun.Status.SKIPPED:
                    message = run.error_message or "Company fetch skipped."
                else:
                    message = run.error_message or "Company fetch failed."
            else:
                if found > 0:
                    message = f"Processing jobs… {processed}/{found}"
                else:
                    message = "Discovering jobs from company board…"

            recent_qs = RawJob.objects.filter(platform_label_id=run.label_id)
            if run.started_at:
                recent_qs = recent_qs.filter(updated_at__gte=run.started_at - timedelta(seconds=3))
            recent_jobs = list(
                recent_qs.order_by("-updated_at").values(
                    "id",
                    "title",
                    "company_name",
                    "location_raw",
                    "sync_status",
                    "updated_at",
                    "original_url",
                )[:20]
            )
            jobs_payload = [
                {
                    "id": row["id"],
                    "title": row["title"] or "Untitled role",
                    "company_name": row["company_name"] or (run.label.company.name if run.label and run.label.company else ""),
                    "location_raw": row["location_raw"] or "",
                    "sync_status": row["sync_status"] or "PENDING",
                    "detail_url": f"/harvest/raw-jobs/{row['id']}/",
                    "original_url": row["original_url"] or "",
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else "",
                }
                for row in recent_jobs
            ]

            rawjobs_qs = {
                "_subtab": "jobs",
                "q": run.label.company.name if run.label and run.label.company else "",
            }
            if run.label and run.label.company_id:
                rawjobs_qs["company_id"] = str(run.label.company_id)
            if run.label and run.label.platform:
                rawjobs_qs["platform"] = run.label.platform.slug
            if run.label_id:
                rawjobs_qs["label_pk"] = str(run.label_id)
            rawjobs_url = f"{reverse('harvest-rawjobs')}?{urlencode(rawjobs_qs)}"

            return JsonResponse(
                {
                    "ok": True,
                    "task_id": task_id,
                    "celery_state": celery_state,
                    "state": state,
                    "running": running,
                    "done": done,
                    "percent": percent,
                    "message": message,
                    "company_name": run.label.company.name if run.label and run.label.company else "",
                    "platform_slug": run.label.platform.slug if run.label and run.label.platform else "",
                    "company_jobs_url": run.label.career_page_url or "",
                    "counts": {
                        "found": found,
                        "new": int(run.jobs_new),
                        "updated": int(run.jobs_updated),
                        "duplicate": int(run.jobs_duplicate),
                        "skipped": int(run.jobs_duplicate),
                        "failed": int(run.jobs_failed),
                    },
                    "run": {
                        "id": run.pk,
                        "label_pk": run.label_id,
                        "started_at": run.started_at.isoformat() if run.started_at else "",
                        "completed_at": run.completed_at.isoformat() if run.completed_at else "",
                        "error_message": run.error_message or "",
                    },
                    "recent_jobs": jobs_payload,
                    "rawjobs_url": rawjobs_url,
                }
            )

        # Fallback before run record is created.
        if celery_state in {"PENDING", "STARTED", "RETRY", "PROGRESS"}:
            percent_map = {"PENDING": 2, "STARTED": 10, "RETRY": 18}
            percent = percent_map.get(celery_state, 15)
            message = "Queued…" if celery_state == "PENDING" else "Starting company fetch…"
            if celery_state == "PROGRESS" and isinstance(async_res.info, dict):
                percent = int(async_res.info.get("percent", percent) or percent)
                message = async_res.info.get("message", message)
            return JsonResponse(
                {
                    "ok": True,
                    "task_id": task_id,
                    "state": celery_state,
                    "celery_state": celery_state,
                    "running": True,
                    "done": False,
                    "percent": max(1, min(95, percent)),
                    "message": message,
                    "counts": {"found": 0, "new": 0, "updated": 0, "duplicate": 0, "skipped": 0, "failed": 0},
                    "recent_jobs": [],
                    "rawjobs_url": f"{reverse('harvest-rawjobs')}?_subtab=jobs",
                }
            )

        if celery_state == "SUCCESS":
            result = async_res.result if isinstance(async_res.result, dict) else {}
            return JsonResponse(
                {
                    "ok": True,
                    "task_id": task_id,
                    "state": "SUCCESS",
                    "celery_state": "SUCCESS",
                    "running": False,
                    "done": True,
                    "percent": 100,
                    "message": "Company fetch complete.",
                    "counts": {
                        "found": int(result.get("jobs_found", 0) or 0),
                        "new": int(result.get("jobs_new", 0) or 0),
                        "updated": int(result.get("jobs_updated", 0) or 0),
                        "duplicate": 0,
                        "skipped": 0,
                        "failed": int(result.get("jobs_failed", 0) or 0),
                    },
                    "recent_jobs": [],
                    "rawjobs_url": f"{reverse('harvest-rawjobs')}?_subtab=jobs",
                }
            )

        err = str(async_res.result) if async_res.result else "Company fetch failed."
        return JsonResponse(
            {
                "ok": True,
                "task_id": task_id,
                "state": celery_state or "FAILURE",
                "celery_state": celery_state or "FAILURE",
                "running": False,
                "done": True,
                "percent": 100,
                "message": err,
                "counts": {"found": 0, "new": 0, "updated": 0, "duplicate": 0, "skipped": 0, "failed": 1},
                "recent_jobs": [],
                "rawjobs_url": f"{reverse('harvest-rawjobs')}?_subtab=jobs",
            }
        )


class JarvisReScrapeView(SuperuserRequiredMixin, View):
    """POST { url } → re-run Jarvis on a fresh URL without saving (preview only)."""

    def post(self, request, *args, **kwargs):
        """Synchronous quick-preview — no DB write, just extract + return JSON."""
        from .jarvis import JobJarvis
        url = request.POST.get("url", "").strip()
        if not url:
            return JsonResponse({"ok": False, "error": "Missing URL"}, status=400)

        try:
            data = JobJarvis().ingest(url)
        except Exception as exc:
            return JsonResponse({"ok": False, "error": str(exc)}, status=500)

        # Strip raw_payload (too large) from response
        data.pop("raw_payload", None)
        return JsonResponse({"ok": not bool(data.get("error")), "data": data})


# ── Setup Celery Beat Schedule ─────────────────────────────────────────────────

class SetupScheduleView(SuperuserRequiredMixin, View):
    """POST — create/update Celery Beat PeriodicTasks for daily auto-harvest."""

    def post(self, request):
        try:
            from django_celery_beat.models import CrontabSchedule, PeriodicTask
            import json as _json

            created = []
            updated = []

            # 1. Daily Quick Sync — 02:00 UTC every day (incremental, since_hours=25)
            quick_cron, _ = CrontabSchedule.objects.get_or_create(
                minute="0", hour="2", day_of_week="*",
                day_of_month="*", month_of_year="*",
            )
            _, was_created = PeriodicTask.objects.update_or_create(
                name="harvest.daily_quick_sync",
                defaults={
                    "crontab": quick_cron,
                    "task": "harvest.fetch_raw_jobs_batch",
                    "kwargs": _json.dumps({"fetch_all": False, "min_hours_since_fetch": 6, "batch_name": "Daily Quick Sync (auto)"}),
                    "enabled": True,
                    "description": "Daily incremental harvest — fetch new/updated jobs from last 25h. Runs at 02:00 UTC.",
                },
            )
            (created if was_created else updated).append("Daily Quick Sync (02:00 UTC)")

            # 2. Weekly Full Crawl — Sunday 03:00 UTC (fetch_all=True, respects 2hr cooldown on GUI only)
            full_cron, _ = CrontabSchedule.objects.get_or_create(
                minute="0", hour="3", day_of_week="0",
                day_of_month="*", month_of_year="*",
            )
            _, was_created = PeriodicTask.objects.update_or_create(
                name="harvest.weekly_full_crawl",
                defaults={
                    "crontab": full_cron,
                    "task": "harvest.fetch_raw_jobs_batch",
                    "kwargs": _json.dumps({"fetch_all": True, "min_hours_since_fetch": 0, "batch_name": "Weekly Full Crawl (auto)"}),
                    "enabled": True,
                    "description": "Weekly full crawl — paginate all jobs from all companies. Runs Sunday 03:00 UTC.",
                },
            )
            (created if was_created else updated).append("Weekly Full Crawl (Sun 03:00 UTC)")

            # 3. Daily sync to pool — 04:00 UTC (after harvest settles)
            sync_cron, _ = CrontabSchedule.objects.get_or_create(
                minute="0", hour="4", day_of_week="*",
                day_of_month="*", month_of_year="*",
            )
            _, was_created = PeriodicTask.objects.update_or_create(
                name="harvest.daily_pool_sync",
                defaults={
                    "crontab": sync_cron,
                    "task": "harvest.sync_harvested_to_pool",
                    "kwargs": _json.dumps({"max_jobs": 5000}),
                    "enabled": True,
                    "description": "Daily pool sync — promote up to 5,000 pending RawJobs to the job pool. Runs at 04:00 UTC.",
                },
            )
            (created if was_created else updated).append("Daily Pool Sync (04:00 UTC)")

            # 4. Daily JD backfill — 05:00 UTC
            backfill_cron, _ = CrontabSchedule.objects.get_or_create(
                minute="0", hour="5", day_of_week="*",
                day_of_month="*", month_of_year="*",
            )
            _, was_created = PeriodicTask.objects.update_or_create(
                name="harvest.daily_jd_backfill",
                defaults={
                    "crontab": backfill_cron,
                    "task": "harvest.backfill_descriptions",
                    "kwargs": _json.dumps({"batch_size": 200, "parallel_workers": 1}),
                    "enabled": True,
                    "description": "Daily JD backfill — fill missing descriptions. Runs at 05:00 UTC.",
                },
            )
            (created if was_created else updated).append("Daily JD Backfill (05:00 UTC)")

            msg_parts = []
            if created:
                msg_parts.append(f"Created: {', '.join(created)}")
            if updated:
                msg_parts.append(f"Updated: {', '.join(updated)}")
            messages.success(request, "Schedule configured. " + " | ".join(msg_parts))

        except ImportError:
            messages.error(request, "django-celery-beat is not installed. Add it to requirements.txt.")
        except Exception as exc:
            messages.error(request, f"Schedule setup failed: {exc}")

        return redirect("harvest-schedule")


# ── Harvest Engine Config ──────────────────────────────────────────────────────

class EngineConfigView(SuperuserRequiredMixin, View):
    """GET = show engine config GUI. POST = save + broadcast rate limit live."""
    template_name = "harvest/settings_engine.html"

    def get(self, request, *args, **kwargs):
        import os
        from django.template.response import TemplateResponse
        cfg = HarvestEngineConfig.get()

        # Detect server CPU count for the advisory note
        cpu_count = os.cpu_count() or 2
        recommended_concurrency = max(2, cpu_count)

        # Try to inspect running Celery workers
        worker_stats = {}
        try:
            from celery import current_app
            inspect = current_app.control.inspect(timeout=1.5)
            stats = inspect.stats() or {}
            for worker_name, info in stats.items():
                pool = info.get("pool", {})
                worker_stats[worker_name] = {
                    "concurrency": pool.get("max-concurrency", "?"),
                    "processes": len(pool.get("processes", [])),
                    "queues": [q["name"] for q in info.get("consumer", {}).get("queues", [])],
                }
        except Exception:
            pass

        # Cooldown info for rawjobs page (also useful on engine page)
        COOLDOWN_HOURS = 2
        last_full_batch = (
            FetchBatch.objects.filter(status__in=["COMPLETED", "PARTIAL", "RUNNING", "CANCELLED"])
            .exclude(name__icontains="PLATFORM CHECK")
            .exclude(name__icontains="QUICK SYNC")
            .order_by("-created_at")
            .first()
        )
        cooldown_remaining_sec = 0
        if last_full_batch:
            elapsed = (timezone.now() - last_full_batch.created_at).total_seconds()
            cooldown_remaining_sec = max(0, int(COOLDOWN_HOURS * 3600 - elapsed))

        ctx = {
            "cfg": cfg,
            "cpu_count": cpu_count,
            "recommended_concurrency": recommended_concurrency,
            "worker_stats": worker_stats,
            "active_tab": "engine",
            "concurrency_presets": [1, 2, 3, 4, 6, 8],
            "last_full_batch": last_full_batch,
            "cooldown_remaining_sec": cooldown_remaining_sec,
        }
        return TemplateResponse(request, self.template_name, ctx)

    def post(self, request, *args, **kwargs):
        cfg = HarvestEngineConfig.get()

        # Integer fields
        int_fields = [
            "worker_concurrency", "task_rate_limit",
            "api_stagger_ms", "scraper_stagger_ms",
            "min_hours_since_fetch", "task_soft_time_limit_secs",
            "resume_jd_min_words", "resume_jd_min_chars",
        ]
        errors = []
        for field in int_fields:
            val = request.POST.get(field, "").strip()
            if val:
                try:
                    setattr(cfg, field, int(val))
                except (ValueError, TypeError):
                    errors.append(f"{field}: must be a whole number")

        # Float fields
        float_fields = ["resume_jd_min_classification_confidence"]
        for field in float_fields:
            val = request.POST.get(field, "").strip()
            if val:
                try:
                    fval = float(val)
                    if field == "resume_jd_min_classification_confidence" and not (0.0 <= fval <= 1.0):
                        raise ValueError
                    setattr(cfg, field, fval)
                except (ValueError, TypeError):
                    errors.append(f"{field}: must be a number (0 to 1)")

        # Boolean (checkbox) fields — unchecked checkboxes send no value, so
        # we must explicitly set False when the key is absent from POST.
        bool_fields = ["auto_backfill_jd", "auto_enrich", "auto_sync_to_pool"]
        for field in bool_fields:
            setattr(cfg, field, field in request.POST)

        if errors:
            messages.error(request, " | ".join(errors))
        else:
            cfg.updated_by = request.user
            cfg.save()  # triggers Celery broadcast for rate_limit
            messages.success(
                request,
                "Engine config saved. Rate limit applied to running workers immediately. "
                "Resume JD gate thresholds apply immediately. "
                "Pipeline funnel toggles and stagger changes apply on the next batch run.",
            )

        return redirect("harvest-engine-config")
