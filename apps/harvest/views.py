import json
import logging
from urllib.parse import urlencode

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.core.cache import cache
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
from users.models import User

from .forms import JobBoardPlatformForm
from .models import (
    CompanyFetchRun,
    CompanyPlatformLabel,
    DuplicateLabel,
    DuplicateResolution,
    FetchBatch,
    HarvestEngineConfig,
    HarvestOpsRun,
    JobBoardPlatform,
    RawJob,
    RawJobDuplicatePair,
)
from .resume_profile import build_resume_job_profile
from .jd_gate import evaluate_raw_job_resume_gate
from .enrichments import infer_country_from_location
from .services.pipeline_snapshot import (
    load_rawjobs_dashboard_stats as _svc_load_rawjobs_dashboard_stats,
    raw_jobs_missing_description_count as _svc_raw_jobs_missing_description_count,
    raw_jobs_missing_jd_expired_count as _svc_raw_jobs_missing_jd_expired_count,
    raw_jobs_workflow_insights as _svc_raw_jobs_workflow_insights,
)
from .services.rawjob_query import (
    apply_rawjob_filters as _svc_apply_rawjob_filters,
    effective_classification_q as _svc_effective_classification_q,
    ready_stage_q as _svc_ready_stage_q,
    rawjob_filter_state as _svc_rawjob_filter_state,
)

logger = logging.getLogger(__name__)


_FULL_CRAWL_COOLDOWN_HOURS = 2


def _effective_classification_q(min_conf: float = 0.01) -> Q:
    """Backward-compatible wrapper to shared query service."""
    return _svc_effective_classification_q(min_conf=min_conf)


def _ready_stage_q(min_conf: float = 0.55) -> Q:
    """Backward-compatible wrapper to shared query service."""
    return _svc_ready_stage_q(min_conf=min_conf)


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
    """Backward-compatible wrapper to shared snapshot service."""
    return _svc_load_rawjobs_dashboard_stats(force_refresh=force_refresh)


def _sync_rawjob_to_pool(raw_job, *, posted_by):
    """
    Sync one RawJob into Job pool (same mapping as bulk sync task).

    Returns ``(job, created_new)``.
    """
    from django.utils import timezone as _tz
    from jobs.models import Job
    from jobs.quality import compute_quality_score
    from jobs.gating import apply_gate_result_to_job, evaluate_raw_job_gate
    from .url_health import check_job_posting_live, is_definitive_inactive

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
        if not live.is_live and is_definitive_inactive(live):
            payload = dict(raw_job.raw_payload or {})
            payload["link_health"] = {
                "is_live": False,
                "reason": live.reason,
                "status_code": live.status_code,
                "checked_at": _tz.now().isoformat(),
                "final_url": live.final_url,
                "decisive": True,
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
            country=raw_job.country or "",
            department=raw_job.department_normalized or "",
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
        from jobs.marketing_role_routing import assign_marketing_roles_to_job

        assign_marketing_roles_to_job(job, raw_job=raw_job)
        raw_job.sync_status = "SYNCED"
        raw_job.raw_payload = payload
        raw_job.save(update_fields=["sync_status", "raw_payload", "updated_at"])
    _invalidate_rawjobs_dashboard_cache()
    return job, True


def _full_crawl_cooldown_ctx() -> dict:
    """Return last_full_batch + cooldown_remaining_sec for any view that shows the Full Crawl button."""
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
        cooldown_remaining_sec = max(0, int(_FULL_CRAWL_COOLDOWN_HOURS * 3600 - elapsed))
    return {"last_full_batch": last_full_batch, "cooldown_remaining_sec": cooldown_remaining_sec}


_ALLOWED_RETURN_VIEWS = {
    "harvest-rawjobs",
    "jobs-pipeline",
    "ops-center",
    "harvest-labels",
    "harvest-schedule",
}


def _resolve_return_target(
    request,
    *,
    default_view: str = "jobs-pipeline",
    default_pipeline_tab: str = "raw",
) -> tuple[str, dict | None]:
    """
    Normalize ``return_to`` from form posts and optional ``return_tab``.

    When returning to jobs pipeline, keep tab context so long-running task actions
    do not bounce users into a different section.
    """
    return_to = (request.POST.get("return_to", "") or "").strip() or default_view
    if return_to not in _ALLOWED_RETURN_VIEWS:
        return_to = default_view
    if return_to == "harvest-rawjobs":
        return_to = "jobs-pipeline"

    extra_query: dict | None = None
    if return_to == "jobs-pipeline":
        return_tab = (request.POST.get("return_tab", "") or "").strip() or default_pipeline_tab
        extra_query = {"tab": return_tab}

    return return_to, extra_query


def raw_jobs_missing_description_count() -> int:
    return _svc_raw_jobs_missing_description_count()


def raw_jobs_missing_jd_expired_count() -> int:
    return _svc_raw_jobs_missing_jd_expired_count()


def _raw_jobs_workflow_insights(*, stale_pending_hours: int = 6) -> dict:
    return _svc_raw_jobs_workflow_insights(stale_pending_hours=stale_pending_hours)


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
            "ops-center",
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
            "ops-center",
            task.id,
            f"Harvest ({label})",
        )


class RunSyncNowView(SuperuserRequiredMixin, View):
    def post(self, request):
        from .tasks import sync_harvested_to_pool_task
        raw_max = (request.POST.get("max_jobs", "") or "").strip()
        try:
            max_jobs = int(raw_max) if raw_max else 0
        except (TypeError, ValueError):
            max_jobs = 0
        qualified_only = (request.POST.get("qualified_only", "1").strip() != "0")
        chunk_raw = (request.POST.get("chunk_size", "") or "").strip()
        try:
            chunk_size = int(chunk_raw) if chunk_raw else 500
        except (TypeError, ValueError):
            chunk_size = 500

        task = sync_harvested_to_pool_task.delay(
            max_jobs=max_jobs,
            qualified_only=qualified_only,
            chunk_size=chunk_size,
        )
        scope_txt = "all qualified pending jobs" if not max_jobs else f"up to {max_jobs:,} qualified jobs"
        if not qualified_only:
            scope_txt = "pending jobs"
            if max_jobs:
                scope_txt = f"up to {max_jobs:,} pending jobs"
        messages.success(
            request,
            f"Vet sync started ({scope_txt}, Task: {task.id[:8]}…). "
            "This scans across the backlog and updates multi-page results.",
        )
        return_to, extra_query = _resolve_return_target(
            request,
            default_view="jobs-pipeline",
            default_pipeline_tab="raw",
        )
        return redirect_with_task_progress(
            return_to,
            task.id,
            "Sync Qualified to Vet Queue",
            extra_query=extra_query,
        )


class RunBulkSyncView(SuperuserRequiredMixin, View):
    """POST — sync up to 20,000 pending RawJobs to the pool in one shot."""
    def post(self, request):
        from .tasks import sync_harvested_to_pool_task
        task = sync_harvested_to_pool_task.delay(max_jobs=20000, qualified_only=False, chunk_size=500)
        messages.success(
            request,
            f"Bulk sync started — up to 20,000 pending jobs → pool (Task: {task.id[:8]}…). "
            "This runs in the background. Refresh to see progress.",
        )
        return_to, extra_query = _resolve_return_target(request, default_view="jobs-pipeline")
        return redirect_with_task_progress(
            return_to,
            task.id,
            "Bulk sync (20k jobs)",
            extra_query=extra_query,
        )


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
        return_to, extra_query = _resolve_return_target(request, default_view="jobs-pipeline")
        return redirect_with_task_progress(
            return_to,
            task.id,
            "Retry failed fetches",
            extra_query=extra_query,
        )


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
            kwargs["max_jobs"] = 0

        task = validate_raw_job_urls_task.delay(**kwargs)
        messages.success(
            request,
            f"Link-health validation queued (Task: {task.id[:8]}…). "
            "Soft-404 pages will be marked inactive before vet sync.",
        )
        return_to, extra_query = _resolve_return_target(request, default_view="jobs-pipeline")
        return redirect_with_task_progress(
            return_to,
            task.id,
            "Validate Raw Job URLs",
            extra_query=extra_query,
        )


class RunSyncSelectedRawJobsView(SuperuserRequiredMixin, View):
    """POST — sync selected RawJob ids into pool."""

    def post(self, request):
        raw_ids = request.POST.get("raw_job_ids", "").strip()
        if not raw_ids:
            messages.error(request, "No rows selected for sync.")
            return redirect(f"{reverse('jobs-pipeline')}?tab=raw")

        parts = [p.strip() for p in raw_ids.split(",") if p.strip()]
        ids: list[int] = []
        for part in parts:
            if part.isdigit():
                ids.append(int(part))
        ids = ids[:500]
        if not ids:
            messages.error(request, "Selected ids were invalid.")
            return redirect(f"{reverse('jobs-pipeline')}?tab=raw")

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
        return redirect(f"{reverse('jobs-pipeline')}?tab=raw")


class RunCleanupNowView(SuperuserRequiredMixin, View):
    def post(self, request):
        from .tasks import cleanup_harvested_jobs_task
        task = cleanup_harvested_jobs_task.delay()
        messages.success(request, f"Cleanup started (Task: {task.id[:8]}...)")
        return redirect_with_task_progress("ops-center", task.id, "Harvest cleanup")


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
    template_name = "jobs/pipeline.html"
    context_object_name = "jobs"
    paginate_by = 100

    def get(self, request, *args, **kwargs):
        """JSON path for infinite-scroll: ?page=N with X-Requested-With:XMLHttpRequest"""
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            from django.core.paginator import Paginator
            # Fetch only the columns rendered in the list — skips description /
            # raw_payload blobs which can be 10–50 KB each and are never shown here.
            qs = self.get_queryset().only(
                "id", "company_name", "platform_slug", "title", "original_url",
                "location_raw", "is_remote", "employment_type", "experience_level",
                "salary_min", "salary_max", "salary_raw", "posted_date", "fetched_at",
                "sync_status", "has_description", "is_active",
                "job_category", "job_domain",
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
                detected_country = infer_country_from_location(
                    location_raw=job.location_raw or "",
                    state=job.state or "",
                    country=job.country or "",
                )
                jobs_data.append({
                    "id": job.pk,
                    "company_name": (job.company_name or "")[:30],
                    "platform_slug": job.platform_slug or "",
                    "title": (job.title or "")[:60],
                    "job_category": job.job_category or "",
                    "job_domain": job.job_domain or "",
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
                    "is_active": bool(job.is_active),
                    "department": (job.department_normalized or job.department or "")[:40],
                    "state": (job.state or "")[:48],
                    "country": (detected_country or "")[:48],
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
                    "link_health_reason": (((job.raw_payload or {}).get("link_health") or {}).get("reason", ""))[:140],
                    "link_health_checked_at": (((job.raw_payload or {}).get("link_health") or {}).get("checked_at", ""))[:40],
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
        # HTML Raw Jobs command center is consolidated into /jobs/pipeline/?tab=raw
        # while this endpoint remains for legacy XHR table consumers.
        query_pairs: list[tuple[str, str]] = [("tab", "raw")]
        for key, values in request.GET.lists():
            if key in {"tab", "_subtab"}:
                continue
            for value in values:
                value_s = str(value or "").strip()
                if value_s:
                    query_pairs.append((key, value_s))
        target = reverse("jobs-pipeline")
        if query_pairs:
            target = f"{target}?{urlencode(query_pairs)}"
        return redirect(target)

    def get_queryset(self):
        # No select_related — JOINs add 20x overhead on 122k rows; all displayed
        # fields (company_name, platform_slug) are denormalised directly on RawJob.
        qs = RawJob.objects.order_by("-fetched_at")
        return _svc_apply_rawjob_filters(qs, self.request.GET)

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["active_tab"] = "rawjobs"

        # Display fallback country immediately from location text, even before
        # a full backfill writes inferred country into DB rows.
        for job in ctx.get("object_list", []):
            detected_country = infer_country_from_location(
                location_raw=job.location_raw or "",
                state=job.state or "",
                country=job.country or "",
            )
            setattr(job, "country_display", detected_country)

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

        # Filter state comes from shared query-contract helper.
        ctx.update(_svc_rawjob_filter_state(self.request.GET))
        paginator = ctx.get("paginator")
        ctx["jobs_total_filtered"] = paginator.count if paginator else 0

        # Workflow analytics for new control tabs.
        ctx["raw_insights"] = _raw_jobs_workflow_insights(stale_pending_hours=6)

        # Running batch check (for live polling)
        ctx["has_running_batch"] = FetchBatch.objects.filter(status="RUNNING").exists()

        ctx.update(_full_crawl_cooldown_ctx())
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
        from .url_health import check_job_posting_live, is_definitive_inactive

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
            "decisive": bool((not result.is_live) and is_definitive_inactive(result)),
        }
        # Only flip inactive on definitive evidence to avoid transient false negatives.
        if result.is_live:
            raw_job.is_active = True
        elif is_definitive_inactive(result):
            raw_job.is_active = False
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


class FetchBatchListView(SuperuserRequiredMixin, View):
    """
    Legacy compatibility endpoint.
    - XHR: returns recent batch JSON.
    - HTML: redirects to unified Jobs Pipeline Raw tab.
    """

    def get(self, request, *args, **kwargs):
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            qs = (
                FetchBatch.objects.prefetch_related("company_runs")
                .order_by("-created_at")[:50]
            )
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
                    "run_kind": ((b.audit_payload or {}).get("queue") or {}).get("run_kind"),
                    "audit_has_completion": bool((b.audit_payload or {}).get("completion")),
                })
            return JsonResponse({"batches": batches})
        # HTML batch history is consolidated into Jobs Pipeline (tab=raw).
        return redirect(f"{reverse('jobs-pipeline')}?tab=raw")


class FetchBatchDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    """Per-batch drill-down: every company run + platform rollup (staff visibility)."""

    model = FetchBatch
    template_name = "harvest/fetch_batch_detail.html"
    context_object_name = "batch"

    def test_func(self):
        u = self.request.user
        return u.is_superuser or getattr(u, "role", None) in (
            User.Role.ADMIN,
            User.Role.EMPLOYEE,
        )

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        batch = self.object
        status_filter = (self.request.GET.get("status") or "").strip().upper()
        runs = batch.company_runs.select_related(
            "label__company",
            "label__platform",
        ).order_by("label__company__name", "pk")
        valid_status = {choice[0] for choice in CompanyFetchRun.Status.choices}
        if status_filter in valid_status:
            runs = runs.filter(status=status_filter)

        ctx["runs"] = runs
        ctx["status_filter"] = status_filter
        ctx["valid_run_statuses"] = sorted(valid_status)

        ctx["batch_platform_rows"] = (
            batch.company_runs.values("label__platform__slug")
            .annotate(
                total=Count("id"),
                ok=Count(
                    "id",
                    filter=Q(
                        status__in=[
                            CompanyFetchRun.Status.SUCCESS,
                            CompanyFetchRun.Status.PARTIAL,
                        ]
                    ),
                ),
                bad=Count(
                    "id",
                    filter=Q(
                        status__in=[
                            CompanyFetchRun.Status.FAILED,
                            CompanyFetchRun.Status.SKIPPED,
                        ]
                    ),
                ),
                running=Count("id", filter=Q(status=CompanyFetchRun.Status.RUNNING)),
            )
            .order_by("-total")
        )

        since_7d = timezone.now() - timedelta(days=7)
        ctx["platform_health_7d"] = (
            CompanyFetchRun.objects.filter(started_at__gte=since_7d)
            .values("label__platform__slug")
            .annotate(
                total=Count("id"),
                ok=Count(
                    "id",
                    filter=Q(
                        status__in=[
                            CompanyFetchRun.Status.SUCCESS,
                            CompanyFetchRun.Status.PARTIAL,
                        ]
                    ),
                ),
                bad=Count(
                    "id",
                    filter=Q(
                        status__in=[
                            CompanyFetchRun.Status.FAILED,
                            CompanyFetchRun.Status.SKIPPED,
                        ]
                    ),
                ),
            )
            .order_by("-bad")[:30]
        )

        ctx["recent_batches"] = FetchBatch.objects.order_by("-created_at")[:15]
        return ctx


class HarvestBatchActivityView(LoginRequiredMixin, UserPassesTestMixin, ListView):
    """Centralized harvest batch timeline + rolled-up metrics (Quick / Full / smoke)."""

    model = FetchBatch
    template_name = "harvest/batch_activity.html"
    context_object_name = "batches"
    paginate_by = 40

    def test_func(self):
        u = self.request.user
        return u.is_superuser or getattr(u, "role", None) in (
            User.Role.ADMIN,
            User.Role.EMPLOYEE,
        )

    def get_queryset(self):
        return FetchBatch.objects.order_by("-created_at")

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        now = timezone.now()
        since_24h = now - timedelta(hours=24)
        since_7d = now - timedelta(days=7)

        recent_24h = list(FetchBatch.objects.filter(created_at__gte=since_24h))
        recent_7d = FetchBatch.objects.filter(created_at__gte=since_7d)

        kind_counts: dict[str, int] = {}
        skipped_fresh_sum = eligible_sum = queued_sum = 0
        jobs_new_sum = jobs_found_sum = 0
        completion_batches = 0

        cr_agg = CompanyFetchRun.objects.filter(started_at__gte=since_24h).aggregate(
            ok_24h=Count(
                "id",
                filter=Q(
                    status__in=[
                        CompanyFetchRun.Status.SUCCESS,
                        CompanyFetchRun.Status.PARTIAL,
                    ]
                ),
            ),
            bad_24h=Count(
                "id",
                filter=Q(
                    status__in=[
                        CompanyFetchRun.Status.FAILED,
                        CompanyFetchRun.Status.SKIPPED,
                    ]
                ),
            ),
        )

        for b in recent_24h:
            q = (b.audit_payload or {}).get("queue") or {}
            rk = q.get("run_kind") or "unknown"
            kind_counts[rk] = kind_counts.get(rk, 0) + 1
            if q.get("eligible_labels") is not None:
                eligible_sum += int(q["eligible_labels"])
            if q.get("skipped_fresh") is not None:
                skipped_fresh_sum += int(q["skipped_fresh"])
            qc = q.get("queued_companies")
            if qc is not None:
                queued_sum += int(qc)
            c = (b.audit_payload or {}).get("completion") or {}
            if c:
                completion_batches += 1
            jobs_new_sum += int(b.total_jobs_new or 0)
            jobs_found_sum += int(b.total_jobs_found or 0)

        ctx["metrics_24h"] = {
            "batch_count": len(recent_24h),
            "run_kind_counts": dict(sorted(kind_counts.items(), key=lambda x: -x[1])),
            "eligible_labels_sum": eligible_sum,
            "skipped_fresh_sum": skipped_fresh_sum,
            "queued_companies_sum": queued_sum,
            "completion_logged_batches": completion_batches,
            "jobs_new_sum": jobs_new_sum,
            "jobs_found_sum": jobs_found_sum,
            "company_runs_ok_24h": int(cr_agg.get("ok_24h") or 0),
            "company_runs_bad_24h": int(cr_agg.get("bad_24h") or 0),
        }
        ctx["metrics_7d_batch_count"] = recent_7d.count()
        ctx["grep_hints"] = ["[HARVEST_AUDIT queue]", "[HARVEST_AUDIT done]", "[HARVEST_AUDIT ops_queue]", "[HARVEST_AUDIT ops_done]"]
        ctx["latest_fetch_batch"] = FetchBatch.objects.order_by("-created_at").first()

        ops_since = HarvestOpsRun.objects.filter(created_at__gte=since_24h)
        ctx["metrics_24h"]["ops_run_count"] = ops_since.count()
        ctx["metrics_24h"]["ops_by_operation"] = {
            row["operation"]: row["n"]
            for row in ops_since.values("operation").annotate(n=Count("id")).order_by()
        }
        ctx["recent_ops_runs"] = HarvestOpsRun.objects.order_by("-created_at")[:40]
        return ctx


class HarvestOpsRunDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    """JSON-shaped audit for a single non-batch pipeline op."""

    model = HarvestOpsRun
    template_name = "harvest/ops_run_detail.html"
    context_object_name = "ops_run"

    def test_func(self):
        u = self.request.user
        return u.is_superuser or getattr(u, "role", None) in (
            User.Role.ADMIN,
            User.Role.EMPLOYEE,
        )

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["audit_json"] = json.dumps(self.object.audit_payload or {}, indent=2, default=str)
        return ctx


class CompanyFetchStatusView(SuperuserRequiredMixin, View):
    """
    Legacy compatibility endpoint.
    - XHR: returns recent company fetch run JSON (optionally filtered).
    - HTML: redirects to unified Jobs Pipeline Raw tab.
    """

    def _get_queryset(self, request):
        qs = CompanyFetchRun.objects.select_related(
            "label__company", "label__platform", "batch"
        ).order_by("-started_at")

        status_f = request.GET.get("status", "").strip()
        if status_f:
            qs = qs.filter(status=status_f)

        platform_f = request.GET.get("platform", "").strip()
        if platform_f:
            qs = qs.filter(label__platform__slug=platform_f)

        return qs

    def get(self, request, *args, **kwargs):
        # JSON response for legacy AJAX clients
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            qs = self._get_queryset(request)[:100]
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
        # HTML company status view is consolidated into Jobs Pipeline (tab=raw).
        return redirect(f"{reverse('jobs-pipeline')}?tab=raw")


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
        cd = _full_crawl_cooldown_ctx()
        last_full = cd["last_full_batch"]
        remaining = cd["cooldown_remaining_sec"]
        if not last_full:
            return JsonResponse({"on_cooldown": False, "remaining_sec": 0, "last_batch_at": None})
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
                run_kind="quick_sync",
            )
            messages.success(
                request,
                f"Quick Sync started — fetching new/updated jobs from the last 25h "
                f"(Task: {task.id[:8]}…). Much faster than a full crawl.",
            )
            return_to, extra_query = _resolve_return_target(request, default_view="jobs-pipeline")
            return redirect_with_task_progress(
                return_to,
                task.id,
                "Quick Sync (25h)",
                extra_query=extra_query,
            )

        # ── Mode: Full Crawl — enforce 2-hour cooldown ────────────────────────
        if fetch_mode == "full":
            cd = _full_crawl_cooldown_ctx()
            last_full = cd["last_full_batch"]
            remaining = cd["cooldown_remaining_sec"]
            if last_full and remaining > 0:
                elapsed_sec = (timezone.now() - last_full.created_at).total_seconds()
                mins = remaining // 60
                secs = remaining % 60
                messages.error(
                    request,
                    f"⏱ Full Crawl on cooldown — last batch ran {int(elapsed_sec // 60)} min ago. "
                    f"Wait {mins}m {secs}s before starting another full crawl. "
                    f"Use Quick Sync (25h) for an incremental update now.",
                )
                return_to, _ = _resolve_return_target(request, default_view="jobs-pipeline")
                return redirect(return_to)
            ts = timezone.now().strftime("%Y-%m-%d %H:%M")
            task = fetch_raw_jobs_batch_task.delay(
                platform_slug=platform_slug or None,
                batch_name=batch_name or f"Full Crawl — {ts}",
                triggered_user_id=request.user.id,
                test_mode=False,
                fetch_all=True,         # full pagination — all pages, all companies
                min_hours_since_fetch=6,
                run_kind="full_crawl_platform" if platform_slug else "full_crawl_all",
            )
            messages.success(
                request,
                f"Full Crawl started — fetching ALL jobs from every platform "
                f"(Task: {task.id[:8]}…). This may take 30–60+ minutes.",
            )
            return_to, extra_query = _resolve_return_target(request, default_view="jobs-pipeline")
            return redirect_with_task_progress(
                return_to,
                task.id,
                "Full Crawl",
                extra_query=extra_query,
            )

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
                run_kind="platform_smoke",
            )
            skip_note = f", skip: {', '.join(skip_platforms)}" if skip_platforms else ""
            messages.success(
                request,
                f"Platform check started — {companies_per_platform} co/platform, up to {test_max_jobs} jobs each{skip_note} (Task: {task.id[:8]}…)",
            )
            return_to, extra_query = _resolve_return_target(request, default_view="jobs-pipeline")
            return redirect_with_task_progress(
                return_to,
                task.id,
                f"Platform check ({test_max_jobs} jobs/platform)",
                extra_query=extra_query,
            )

        # ── Mode: Filtered Batch (platform selector form) ─────────────────────
        filtered_rk = "full_crawl_platform" if platform_slug else "full_crawl_all"
        task = fetch_raw_jobs_batch_task.delay(
            platform_slug=platform_slug,
            batch_name=batch_name,
            triggered_user_id=request.user.id,
            test_mode=False,
            skip_platforms=skip_platforms or None,
            fetch_all=True,
            run_kind=filtered_rk,
        )
        messages.success(
            request,
            f"Raw jobs batch fetch started"
            + (f" for platform '{platform_slug}'" if platform_slug else " for all platforms")
            + f" (Task: {task.id[:8]}...)",
        )
        return_to, extra_query = _resolve_return_target(request, default_view="jobs-pipeline")
        return redirect_with_task_progress(
            return_to,
            task.id,
            "Raw jobs batch fetch",
            extra_query=extra_query,
        )


class StopBatchView(SuperuserRequiredMixin, View):
    """GET — redirect to batch list. POST — cancel a running FetchBatch and revoke its tasks."""

    def get(self, request):
        """Direct browser navigation → just go to the batch list page."""
        return redirect(f"{reverse('jobs-pipeline')}?tab=raw")

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
            return redirect(f"{reverse('jobs-pipeline')}?tab=raw")

        if batch.status not in ("RUNNING", "PENDING"):
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return JsonResponse({"ok": False, "error": f"Batch is already {batch.status}."})
            messages.warning(request, f"Batch #{batch.pk} is already {batch.status}.")
            return redirect(f"{reverse('jobs-pipeline')}?tab=raw")

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

        try:
            locked = FetchBatch.objects.filter(pk=batch.pk).first()
            if locked:
                ap = dict(locked.audit_payload or {})
                ap["cancelled"] = {
                    "at": timezone.now().isoformat(),
                    "by": request.user.username,
                    "revoked_child_tasks": len(task_ids),
                }
                locked.audit_payload = ap
                locked.save(update_fields=["audit_payload"])
        except Exception:
            logger.exception("StopBatchView: failed to record audit_payload cancelled")

        logger.info(
            "[HARVEST] Batch #%s cancelled by %s — revoked %d task(s)",
            batch.pk, request.user.username, len(task_ids) + (1 if batch.task_id else 0),
        )

        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return JsonResponse({"ok": True, "batch_id": batch.pk, "revoked": len(task_ids)})

        messages.success(request, f"Batch #{batch.pk} cancelled — {len(task_ids)} pending tasks revoked.")
        return redirect(f"{reverse('jobs-pipeline')}?tab=raw")


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
        return_to, extra_query = _resolve_return_target(request, default_view="jobs-pipeline")
        return redirect_with_task_progress(
            return_to,
            task.id,
            f"Enrich existing jobs ({label})",
            extra_query=extra_query,
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
        return_to, extra_query = _resolve_return_target(request, default_view="jobs-pipeline")
        return redirect_with_task_progress(
            return_to,
            task.id,
            "Backfill resume contract fields",
            extra_query=extra_query,
        )


class RunBackfillDescriptionsView(SuperuserRequiredMixin, View):
    """POST — launch backfill_descriptions_task to fetch JDs for jobs that have none."""

    def post(self, request):
        from .tasks import backfill_descriptions_task

        platform_slug = request.POST.get("platform_slug", "").strip() or None
        batch_size = int(request.POST.get("batch_size", "200") or "200")
        parallel_workers = int(request.POST.get("parallel_workers", "4") or "4")
        offset = int(request.POST.get("offset", "0") or "0")
        force_jarvis = request.POST.get("force_jarvis", "") in ("1", "true", "True")
        reset_locks = request.POST.get("reset_locks", "") in ("1", "true", "True")

        task = backfill_descriptions_task.delay(
            batch_size=batch_size,
            parallel_workers=parallel_workers,
            platform_slug=platform_slug,
            offset=offset,
            force_jarvis=force_jarvis,
            reset_locks=reset_locks,
        )
        label = f"platform={platform_slug}" if platform_slug else "all platforms"
        mode = "Deep Scan (Jarvis)" if force_jarvis else "backfill"
        messages.success(
            request,
            f"Description {mode} started ({label}, batch={batch_size}, workers={parallel_workers}) — Task {task.id[:8]}…",
        )
        return_to, extra_query = _resolve_return_target(request, default_view="jobs-pipeline")
        return redirect_with_task_progress(
            return_to,
            task.id,
            f"{'Deep Scan' if force_jarvis else 'Backfill'} descriptions ({label})",
            extra_query=extra_query,
        )


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

        # HTML stats view is consolidated into Jobs Pipeline (tab=raw).
        return redirect(f"{reverse('jobs-pipeline')}?tab=raw")


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


@method_decorator(never_cache, name="dispatch")
class BoardAnalyticsDashboardView(SuperuserRequiredMixin, View):
    """
    GET /harvest/board-analytics/ — HTML dashboard with sortable platform table.
    """

    def get(self, request):
        from .board_analytics import get_board_analytics
        try:
            window = int(request.GET.get("window_days", 30))
            window = max(1, min(90, window))
        except (ValueError, TypeError):
            window = 30

        from .board_analytics import MIN_RUNS_FOR_RISK
        data = get_board_analytics(window_days=window)
        return render(request, "harvest/board_analytics.html", {
            "data": data,
            "window_choices": [7, 14, 30, 60, 90],
            "min_runs_for_risk": MIN_RUNS_FOR_RISK,
        })


class BoardAnalyticsView(SuperuserRequiredMixin, View):
    """
    GET /harvest/api/board-analytics/?window_days=30

    Returns unified per-platform analytics as JSON.  All "blocked" and "jobs" metrics
    share the same denominator (total RawJob rows) so impossible ratios cannot occur.

    Query params:
      window_days  – run-history look-back window (default 30, max 90)
    """

    def get(self, request):
        from .board_analytics import get_board_analytics
        try:
            window = int(request.GET.get("window_days", 30))
            window = max(1, min(90, window))
        except (ValueError, TypeError):
            window = 30

        data = get_board_analytics(window_days=window)
        return JsonResponse({"ok": True, **data})


class BoardDrillDownView(SuperuserRequiredMixin, View):
    """
    GET /harvest/board-analytics/<slug>/
    Per-platform drill-down: run history, field coverage trend, recent error messages.
    """

    def get(self, request, slug: str):
        from .models import CompanyFetchRun, RawJob, JobBoardPlatform
        from .board_capabilities import get_capabilities
        from datetime import timedelta
        from django.utils import timezone
        from django.db.models import Count, Avg, Max, Q

        try:
            window = int(request.GET.get("window_days", 30))
            window = max(1, min(90, window))
        except (ValueError, TypeError):
            window = 30

        now = timezone.now()
        run_window = now - timedelta(days=window)

        # Platform meta
        platform = JobBoardPlatform.objects.filter(slug=slug).first()

        # Recent runs for this platform
        recent_runs = (
            CompanyFetchRun.objects
            .filter(started_at__gte=run_window, label__platform__slug=slug)
            .order_by("-started_at")
            .values(
                "id", "status", "error_type", "issue_code", "error_message",
                "jobs_found", "jobs_new", "jobs_updated", "jobs_duplicate",
                "started_at", "completed_at", "field_presence",
                "label__company__name",
            )[:100]
        )

        # Aggregate stats from runs
        run_agg = (
            CompanyFetchRun.objects
            .filter(started_at__gte=run_window, label__platform__slug=slug)
            .aggregate(
                total=Count("id"),
                success=Count("id", filter=Q(status="SUCCESS")),
                empty=Count("id", filter=Q(status="EMPTY")),
                failed=Count("id", filter=Q(status="FAILED")),
                partial=Count("id", filter=Q(status="PARTIAL")),
                avg_jobs=Avg("jobs_found"),
            )
        )

        # RawJob totals for this platform
        job_qs = RawJob.objects.filter(platform_slug=slug)
        job_totals = job_qs.aggregate(
            total=Count("id"),
            synced=Count("id", filter=Q(sync_status="SYNCED")),
            pending=Count("id", filter=Q(sync_status="PENDING")),
            failed=Count("id", filter=Q(sync_status="FAILED")),
            inactive=Count("id", filter=Q(is_active=False)),
            missing_jd=Count("id", filter=Q(has_description=False)),
            has_salary=Count("id", filter=Q(salary_min__isnull=False) | Q(salary_max__isnull=False)),
        )

        # Blocker breakdown
        _rj_fields = {f.name for f in RawJob._meta.get_fields()}
        blockers = {}
        if "sync_skip_reason" in _rj_fields:
            from django.db.models import Count as C
            blockers = (
                job_qs
                .exclude(sync_skip_reason="")
                .values("sync_skip_reason")
                .annotate(count=Count("id"))
                .order_by("-count")
            )
            blockers = {b["sync_skip_reason"]: b["count"] for b in blockers}

        # Most recent errors
        recent_errors = list(
            CompanyFetchRun.objects
            .filter(
                started_at__gte=run_window,
                label__platform__slug=slug,
                status__in=["FAILED", "PARTIAL"],
            )
            .exclude(error_message="")
            .order_by("-started_at")
            .values("label__company__name", "error_message", "error_type", "issue_code", "started_at")
            [:20]
        )

        caps = get_capabilities(slug)

        context = {
            "slug": slug,
            "platform": platform,
            "window_days": window,
            "window_choices": [7, 14, 30, 60, 90],
            "run_agg": run_agg,
            "recent_runs": list(recent_runs),
            "job_totals": job_totals,
            "blockers": blockers,
            "recent_errors": recent_errors,
            "capabilities": caps,
        }
        return render(request, "harvest/board_drilldown.html", context)


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

        # Platform breakdown — single aggregation query, no Python loop over model instances
        ctx["jarvis_platform_breakdown"] = [
            (row["job_platform__name"] or "Unknown", row["count"])
            for row in jarvis_qs
            .values("job_platform__name")
            .annotate(count=Count("id"))
            .order_by("-count")[:6]
        ]

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
        ctx["rawjobs_url"] = f"{reverse('jobs-pipeline')}?tab=raw"
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
                "tab": "raw",
                "q": run.label.company.name if run.label and run.label.company else "",
            }
            if run.label and run.label.company_id:
                rawjobs_qs["company_id"] = str(run.label.company_id)
            if run.label and run.label.platform:
                rawjobs_qs["platform"] = run.label.platform.slug
            if run.label_id:
                rawjobs_qs["label_pk"] = str(run.label_id)
            rawjobs_url = f"{reverse('jobs-pipeline')}?{urlencode(rawjobs_qs)}"

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
                    "rawjobs_url": f"{reverse('jobs-pipeline')}?tab=raw",
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
                    "rawjobs_url": f"{reverse('jobs-pipeline')}?tab=raw",
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
                "rawjobs_url": f"{reverse('jobs-pipeline')}?tab=raw",
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

        ctx = {
            "cfg": cfg,
            "cpu_count": cpu_count,
            "recommended_concurrency": recommended_concurrency,
            "worker_stats": worker_stats,
            "active_tab": "engine",
            "concurrency_presets": [1, 2, 3, 4, 6, 8],
            **_full_crawl_cooldown_ctx(),
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


# ── Duplicate Engine Views ────────────────────────────────────────────────────

class DuplicateListView(SuperuserRequiredMixin, TemplateView):
    template_name = "harvest/duplicates.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        from django.db.models import Count

        label_filter      = self.request.GET.get("label", "")
        resolution_filter = self.request.GET.get("resolution", "PENDING")
        search            = self.request.GET.get("q", "").strip()

        qs = RawJobDuplicatePair.objects.select_related(
            "primary", "duplicate", "resolved_by",
        )
        if resolution_filter and resolution_filter != "all":
            qs = qs.filter(resolution=resolution_filter)
        if label_filter:
            qs = qs.filter(label=label_filter)
        if search:
            qs = qs.filter(
                Q(primary__title__icontains=search)
                | Q(primary__company_name__icontains=search)
                | Q(duplicate__title__icontains=search)
            )

        # Summary stats
        stats = (
            RawJobDuplicatePair.objects.values("label")
            .annotate(cnt=Count("id"))
            .order_by("-cnt")
        )
        pending_count = RawJobDuplicatePair.objects.filter(
            resolution=DuplicateResolution.PENDING
        ).count()

        ctx.update({
            "pairs":            qs[:200],
            "total_pairs":      RawJobDuplicatePair.objects.count(),
            "pending_count":    pending_count,
            "label_stats":      {s["label"]: s["cnt"] for s in stats},
            "label_choices":    DuplicateLabel.choices,
            "resolution_choices": DuplicateResolution.choices,
            "label_filter":     label_filter,
            "resolution_filter": resolution_filter,
            "search":           search,
            "q":                search,
        })
        return ctx


class DuplicateRunView(SuperuserRequiredMixin, View):
    def post(self, request):
        from .tasks import run_duplicate_detection_task
        raw_limit = request.POST.get("limit", "5000").strip()
        try:
            limit = max(100, min(int(raw_limit), 200_000))
        except (ValueError, TypeError):
            limit = 5000
        company_slug = request.POST.get("company_slug", "").strip()
        task = run_duplicate_detection_task.delay(limit=limit, company_slug=company_slug)
        messages.success(
            request,
            f"Duplicate detection is running in the background (task {task.id[:8]}…). "
            "This may take a few minutes — refresh the page to see new pairs.",
        )
        return redirect_with_task_progress("harvest-duplicates", task.id, "Duplicate detection")


class DuplicateResolveView(SuperuserRequiredMixin, View):
    def post(self, request, pk):
        pair   = get_object_or_404(RawJobDuplicatePair, pk=pk)
        action = request.POST.get("action", "")
        notes  = request.POST.get("notes", "")

        if action == "merge":
            from .duplicate_engine import merge_pair
            result = merge_pair(pair, resolved_by=request.user)
            fields = ", ".join(result["backfilled_fields"]) or "none"
            messages.success(request, f"Merged — duplicate deactivated. Backfilled: {fields}.")

        elif action == "dismiss":
            from .duplicate_engine import dismiss_pair
            dismiss_pair(pair, resolved_by=request.user, notes=notes)
            messages.success(request, "Marked as Keep Both.")

        elif action == "confirm":
            from .duplicate_engine import confirm_pair
            confirm_pair(pair, resolved_by=request.user)
            messages.success(request, "Confirmed as duplicate.")

        elif action == "reopen":
            pair.resolution = DuplicateResolution.PENDING
            pair.resolved_at = None
            pair.resolved_by = None
            pair.save(update_fields=["resolution", "resolved_at", "resolved_by"])
            messages.info(request, "Pair reopened for review.")

        return redirect(request.POST.get("next", "harvest-duplicates"))


class DuplicateBulkResolveView(SuperuserRequiredMixin, View):
    def post(self, request):
        action  = request.POST.get("action", "")
        ids     = request.POST.getlist("pair_ids")
        next_url = request.POST.get("next") or reverse("harvest-duplicates")

        if not ids:
            messages.warning(request, "No pairs selected.")
            return redirect(next_url)

        pairs = RawJobDuplicatePair.objects.filter(pk__in=ids, resolution=DuplicateResolution.PENDING)

        if action == "bulk_merge":
            from .duplicate_engine import merge_pair
            count = 0
            failed = 0
            for pair in pairs:
                try:
                    merge_pair(pair, resolved_by=request.user)
                    count += 1
                except Exception as e:
                    failed += 1
                    logger.warning("merge_pair failed for pair %s: %s", pair.pk, e)
            if count:
                messages.success(request, f"Merged {count} pair{'s' if count != 1 else ''}.")
            if failed:
                messages.warning(request, f"{failed} pair{'s' if failed != 1 else ''} could not be merged.")

        elif action == "bulk_dismiss":
            from django.utils import timezone
            count = pairs.update(
                resolution=DuplicateResolution.DISMISSED,
                resolved_at=timezone.now(),
                resolved_by=request.user,
            )
            messages.success(request, f"Kept both for {count} pair{'s' if count != 1 else ''}.")

        return redirect(next_url)
