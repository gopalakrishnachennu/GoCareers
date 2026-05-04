import csv
import json
from collections import defaultdict
from datetime import timedelta

from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.core.serializers.json import DjangoJSONEncoder
from django.db.models import (
    Avg,
    Case,
    CharField,
    Count,
    DurationField,
    ExpressionWrapper,
    F,
    IntegerField,
    Q,
    Sum,
    Value,
    When,
)
from django.db.models.functions import Coalesce, TruncMonth
from django.http import HttpResponse
from django.utils import timezone
from django.views.generic import TemplateView, View

from core.dashboard_metrics import (
    get_consultant_performance_metrics,
    get_employee_leaderboard_metrics,
    get_submission_funnel_metrics,
    get_time_to_hire_metrics,
)
from core.feature_flags import feature_enabled_for
from harvest.models import CompanyFetchRun, CompanyPlatformLabel, HarvestOpsRun, JobBoardPlatform, RawJob
from jobs.models import Job
from submissions.models import ApplicationSubmission
from users.models import MarketingRole, User


BLOCKED_CONFIDENCE_THRESHOLD = 0.55
LOW_QUALITY_THRESHOLD = 0.45
RESUME_READY_THRESHOLD = 0.50


def _pct(part: int | float, whole: int | float) -> float:
    if not whole:
        return 0.0
    return round((float(part) / float(whole)) * 100.0, 1)


def _risk_band(score: float) -> str:
    if score >= 60:
        return "critical"
    if score >= 35:
        return "warning"
    return "healthy"


def _safe_duration_minutes(value) -> float:
    if not value:
        return 0.0
    try:
        return round(float(value.total_seconds()) / 60.0, 1)
    except Exception:
        return 0.0


def _rawjob_platform_slug_expr():
    fallback = Coalesce("job_platform__slug", "platform_label__platform__slug", Value("unknown"))
    return Case(
        When(Q(platform_slug__isnull=True) | Q(platform_slug=""), then=fallback),
        default=F("platform_slug"),
        output_field=CharField(),
    )


def _run_platform_slug_expr():
    return Coalesce("label__platform__slug", Value("unknown"))


def _platform_health_rows(since):
    raw_slug = _rawjob_platform_slug_expr()
    run_slug = _run_platform_slug_expr()
    duration_expr = ExpressionWrapper(F("completed_at") - F("started_at"), output_field=DurationField())
    low_conf_q = Q(category_confidence__lt=BLOCKED_CONFIDENCE_THRESHOLD) | (
        Q(category_confidence__isnull=True) & Q(classification_confidence__lt=BLOCKED_CONFIDENCE_THRESHOLD)
    )
    blocked_q = Q(sync_status=RawJob.SyncStatus.PENDING) & (
        Q(is_active=False)
        | Q(has_description=False)
        | low_conf_q
        | Q(resume_ready_score__lt=RESUME_READY_THRESHOLD)
        | Q(sync_status=RawJob.SyncStatus.FAILED)
    )

    raw_qs = RawJob.objects.all()
    run_qs = CompanyFetchRun.objects.exclude(label__platform__isnull=True)
    if since:
        raw_qs = raw_qs.filter(fetched_at__gte=since)
        run_qs = run_qs.filter(started_at__gte=since)

    raw_rows = (
        raw_qs
        .annotate(metric_slug=raw_slug)
        .values("metric_slug")
        .annotate(
            total_jobs=Count("id"),
            active_jobs=Count("id", filter=Q(is_active=True)),
            inactive_jobs=Count("id", filter=Q(is_active=False)),
            missing_jd=Count("id", filter=Q(has_description=False)),
            failed_sync=Count("id", filter=Q(sync_status=RawJob.SyncStatus.FAILED)),
            pending_sync=Count("id", filter=Q(sync_status=RawJob.SyncStatus.PENDING)),
            synced_jobs=Count("id", filter=Q(sync_status=RawJob.SyncStatus.SYNCED)),
            skipped_jobs=Count("id", filter=Q(sync_status=RawJob.SyncStatus.SKIPPED)),
            duplicate_jobs=Count("id", filter=Q(sync_status=RawJob.SyncStatus.DUPLICATE)),
            blocked_jobs=Count("id", filter=blocked_q),
            low_confidence_jobs=Count("id", filter=low_conf_q),
            low_quality_jobs=Count("id", filter=Q(quality_score__lt=LOW_QUALITY_THRESHOLD)),
            low_jd_quality_jobs=Count("id", filter=Q(jd_quality_score__lt=LOW_QUALITY_THRESHOLD)),
            low_resume_ready_jobs=Count("id", filter=Q(resume_ready_score__lt=RESUME_READY_THRESHOLD)),
            requirements_jobs=Count("id", filter=~Q(requirements="")),
            responsibilities_jobs=Count("id", filter=~Q(responsibilities="")),
            department_jobs=Count("id", filter=(~Q(department="") | ~Q(department_normalized=""))),
            geo_jobs=Count(
                "id",
                filter=(~Q(location_raw="") | ~Q(city="") | ~Q(state="") | ~Q(country=""))
            ),
            salary_jobs=Count(
                "id",
                filter=(
                    Q(salary_min__isnull=False)
                    | Q(salary_max__isnull=False)
                    | ~Q(salary_raw="")
                ),
            ),
            category_jobs=Count("id", filter=~Q(job_category="")),
            education_jobs=Count(
                "id",
                filter=(~Q(education_required="") | ~Q(vendor_degree_level="")),
            ),
            schedule_jobs=Count(
                "id",
                filter=(~Q(schedule_type="") | ~Q(shift_schedule="") | ~Q(vendor_job_schedule="")),
            ),
            location_block_jobs=Count("id", filter=~Q(vendor_location_block="")),
        )
        .order_by()
    )

    run_rows = (
        run_qs
        .annotate(metric_slug=run_slug)
        .values("metric_slug")
        .annotate(
            total_runs=Count("id"),
            success_runs=Count("id", filter=Q(status=CompanyFetchRun.Status.SUCCESS)),
            partial_runs=Count("id", filter=Q(status=CompanyFetchRun.Status.PARTIAL)),
            failed_runs=Count("id", filter=Q(status=CompanyFetchRun.Status.FAILED)),
            skipped_runs=Count("id", filter=Q(status=CompanyFetchRun.Status.SKIPPED)),
            timeout_runs=Count("id", filter=Q(error_type=CompanyFetchRun.ErrorType.TIMEOUT)),
            http_error_runs=Count("id", filter=Q(error_type=CompanyFetchRun.ErrorType.HTTP_ERROR)),
            parse_error_runs=Count("id", filter=Q(error_type=CompanyFetchRun.ErrorType.PARSE_ERROR)),
            no_tenant_runs=Count("id", filter=Q(error_type=CompanyFetchRun.ErrorType.NO_TENANT)),
            platform_error_runs=Count("id", filter=Q(error_type=CompanyFetchRun.ErrorType.PLATFORM_ERROR)),
            rate_limited_runs=Count("id", filter=Q(error_type=CompanyFetchRun.ErrorType.RATE_LIMITED)),
            zero_yield_success_runs=Count(
                "id",
                filter=(
                    Q(status__in=[CompanyFetchRun.Status.SUCCESS, CompanyFetchRun.Status.PARTIAL])
                    & Q(jobs_found=0)
                    & (Q(error_message="") | Q(error_message__isnull=True))
                ),
            ),
            total_jobs_found=Coalesce(Sum("jobs_found"), 0),
            total_jobs_available=Coalesce(Sum("jobs_total_available"), 0),
            total_jobs_new=Coalesce(Sum("jobs_new"), 0),
            total_jobs_updated=Coalesce(Sum("jobs_updated"), 0),
            total_jobs_duplicate=Coalesce(Sum("jobs_duplicate"), 0),
            total_jobs_failed=Coalesce(Sum("jobs_failed"), 0),
            total_pages_fetched=Coalesce(Sum("pages_fetched"), 0),
            avg_duration=Avg(duration_expr),
        )
        .order_by()
    )

    label_rows = (
        CompanyPlatformLabel.objects
        .exclude(platform__isnull=True)
        .order_by()
        .values("platform__slug")
        .annotate(
            companies_tracked=Count("id"),
            verified_labels=Count("id", filter=Q(is_verified=True)),
            portal_live=Count("id", filter=Q(portal_alive=True)),
            portal_down=Count("id", filter=Q(portal_alive=False)),
            portal_unknown=Count("id", filter=Q(portal_alive__isnull=True)),
            low_conf_labels=Count("id", filter=Q(confidence=CompanyPlatformLabel.Confidence.LOW)),
            medium_conf_labels=Count("id", filter=Q(confidence=CompanyPlatformLabel.Confidence.MEDIUM)),
            no_tenant_labels=Count("id", filter=Q(tenant_id="")),
            undetected_labels=Count("id", filter=Q(detection_method=CompanyPlatformLabel.DetectionMethod.UNDETECTED)),
        )
    )

    registry = {
        p.slug: {
            "slug": p.slug,
            "platform_name": p.name,
            "color_hex": p.color_hex or "#64748b",
            "api_type": p.api_type,
        }
        for p in JobBoardPlatform.objects.all()
    }

    merged: dict[str, dict] = defaultdict(dict)
    for slug, meta in registry.items():
        merged[slug].update(meta)

    for row in raw_rows:
        slug = row.pop("metric_slug") or "unknown"
        merged[slug].update(row)

    for row in run_rows:
        slug = row.pop("metric_slug") or "unknown"
        merged[slug].update(row)

    for row in label_rows:
        slug = row.pop("platform__slug") or "unknown"
        merged[slug].update(row)

    rows = []
    for slug, row in merged.items():
        total_jobs = int(row.get("total_jobs") or 0)
        total_runs = int(row.get("total_runs") or 0)
        companies_tracked = int(row.get("companies_tracked") or 0)
        success_like_runs = int(row.get("success_runs") or 0) + int(row.get("partial_runs") or 0)
        missing_jd_rate = _pct(row.get("missing_jd") or 0, total_jobs)
        inactive_rate = _pct(row.get("inactive_jobs") or 0, total_jobs)
        blocked_rate = _pct(row.get("blocked_jobs") or 0, total_jobs)
        low_confidence_rate = _pct(row.get("low_confidence_jobs") or 0, total_jobs)
        fetch_fail_rate = _pct(row.get("failed_runs") or 0, total_runs)
        fetch_partial_rate = _pct(row.get("partial_runs") or 0, total_runs)
        portal_down_rate = _pct(row.get("portal_down") or 0, companies_tracked)
        zero_yield_rate = _pct(row.get("zero_yield_success_runs") or 0, success_like_runs)
        risk_score = round(
            (missing_jd_rate * 0.22)
            + (inactive_rate * 0.15)
            + (fetch_fail_rate * 0.18)
            + (fetch_partial_rate * 0.08)
            + (portal_down_rate * 0.10)
            + (zero_yield_rate * 0.10)
            + (blocked_rate * 0.10)
            + (low_confidence_rate * 0.07),
            1,
        )
        row["risk_score"] = risk_score
        row["risk_band"] = _risk_band(risk_score)
        row["missing_jd_rate"] = missing_jd_rate
        row["inactive_rate"] = inactive_rate
        row["blocked_rate"] = blocked_rate
        row["low_confidence_rate"] = low_confidence_rate
        row["fetch_fail_rate"] = fetch_fail_rate
        row["fetch_partial_rate"] = fetch_partial_rate
        row["portal_down_rate"] = portal_down_rate
        row["zero_yield_rate"] = zero_yield_rate
        row["jd_coverage_pct"] = _pct(total_jobs - int(row.get("missing_jd") or 0), total_jobs)
        row["requirements_coverage_pct"] = _pct(row.get("requirements_jobs") or 0, total_jobs)
        row["responsibilities_coverage_pct"] = _pct(row.get("responsibilities_jobs") or 0, total_jobs)
        row["department_coverage_pct"] = _pct(row.get("department_jobs") or 0, total_jobs)
        row["geo_coverage_pct"] = _pct(row.get("geo_jobs") or 0, total_jobs)
        row["salary_coverage_pct"] = _pct(row.get("salary_jobs") or 0, total_jobs)
        row["category_coverage_pct"] = _pct(row.get("category_jobs") or 0, total_jobs)
        row["education_coverage_pct"] = _pct(row.get("education_jobs") or 0, total_jobs)
        row["schedule_coverage_pct"] = _pct(row.get("schedule_jobs") or 0, total_jobs)
        row["avg_duration_minutes"] = _safe_duration_minutes(row.get("avg_duration"))
        row["platform_name"] = row.get("platform_name") or slug.replace("_", " ").title()
        row["slug"] = slug
        rows.append(row)

    rows.sort(
        key=lambda item: (
            -(item.get("risk_score") or 0),
            -(item.get("total_jobs") or 0),
            item.get("platform_name") or "",
        )
    )
    return rows


def _date_range_context(request):
    range_param = request.GET.get("range", "all")
    since = None
    if range_param == "7":
        since = timezone.now() - timedelta(days=7)
        label = "Last 7 days"
    elif range_param == "30":
        since = timezone.now() - timedelta(days=30)
        label = "Last 30 days"
    else:
        range_param = "all"
        label = "All time"
    return since, range_param, label


class EmployeeRequiredMixin(UserPassesTestMixin):
    def test_func(self):
        u = self.request.user
        if not (u.is_superuser or u.role in (User.Role.EMPLOYEE, User.Role.ADMIN)):
            return False
        return feature_enabled_for(u, "employee_analytics")


class AnalyticsDashboardView(LoginRequiredMixin, EmployeeRequiredMixin, TemplateView):
    template_name = "analytics/dashboard.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        since, range_param, range_label = _date_range_context(self.request)
        context["date_range"] = range_param
        context["date_range_label"] = range_label

        job_qs = Job.objects.all()
        app_qs = ApplicationSubmission.objects.all()
        if since:
            job_qs = job_qs.filter(created_at__gte=since)
            app_qs = app_qs.filter(created_at__gte=since)

        context["total_jobs"] = job_qs.count()
        context["active_jobs"] = job_qs.filter(status=Job.Status.OPEN).count()
        context["total_consultants"] = User.objects.filter(role=User.Role.CONSULTANT).count()
        context["total_applications"] = app_qs.count()

        app_status_qs = app_qs.values("status").annotate(count=Count("status")).order_by()
        status_counts = {row["status"]: row["count"] for row in app_status_qs}
        total_apps = context["total_applications"]
        context["interview_rate"] = int((status_counts.get(ApplicationSubmission.Status.INTERVIEW, 0) / total_apps) * 100) if total_apps else 0
        context["offer_rate"] = int((status_counts.get(ApplicationSubmission.Status.OFFER, 0) / total_apps) * 100) if total_apps else 0
        context["rejection_rate"] = int((status_counts.get(ApplicationSubmission.Status.REJECTED, 0) / total_apps) * 100) if total_apps else 0

        jobs_over_time = (
            job_qs.annotate(month=TruncMonth("created_at"))
            .values("month")
            .annotate(count=Count("id"))
            .order_by("month")
        )
        apps_over_time = (
            app_qs.annotate(month=TruncMonth("created_at"))
            .values("month")
            .annotate(count=Count("id"))
            .order_by("month")
        )
        context["jobs_time_labels"] = json.dumps([item["month"].strftime("%b %Y") for item in jobs_over_time], cls=DjangoJSONEncoder)
        context["jobs_time_data"] = json.dumps([item["count"] for item in jobs_over_time], cls=DjangoJSONEncoder)
        context["apps_time_labels"] = json.dumps([item["month"].strftime("%b %Y") for item in apps_over_time], cls=DjangoJSONEncoder)
        context["apps_time_data"] = json.dumps([item["count"] for item in apps_over_time], cls=DjangoJSONEncoder)
        context["app_status_labels"] = [item["status"] for item in app_status_qs]
        context["app_status_data"] = [item["count"] for item in app_status_qs]

        if since:
            context["top_employees"] = (
                User.objects.filter(role=User.Role.EMPLOYEE)
                .annotate(job_count=Count("posted_jobs", filter=Q(posted_jobs__created_at__gte=since)))
                .order_by("-job_count")[:5]
            )
            context["top_consultants"] = (
                User.objects.filter(role=User.Role.CONSULTANT, consultant_profile__isnull=False)
                .annotate(
                    submission_count=Count(
                        "consultant_profile__submissions",
                        filter=Q(consultant_profile__submissions__created_at__gte=since),
                    )
                )
                .order_by("-submission_count")[:5]
            )
            role_qs = MarketingRole.objects.all().annotate(
                total=Count("jobs__submissions", filter=Q(jobs__submissions__created_at__gte=since)),
                applied=Count(
                    "jobs__submissions",
                    filter=Q(
                        jobs__submissions__created_at__gte=since,
                        jobs__submissions__status=ApplicationSubmission.Status.APPLIED,
                    ),
                ),
                interview=Count(
                    "jobs__submissions",
                    filter=Q(
                        jobs__submissions__created_at__gte=since,
                        jobs__submissions__status=ApplicationSubmission.Status.INTERVIEW,
                    ),
                ),
                offer=Count(
                    "jobs__submissions",
                    filter=Q(
                        jobs__submissions__created_at__gte=since,
                        jobs__submissions__status=ApplicationSubmission.Status.OFFER,
                    ),
                ),
                rejected=Count(
                    "jobs__submissions",
                    filter=Q(
                        jobs__submissions__created_at__gte=since,
                        jobs__submissions__status=ApplicationSubmission.Status.REJECTED,
                    ),
                ),
            )
        else:
            context["top_employees"] = (
                User.objects.filter(role=User.Role.EMPLOYEE)
                .annotate(job_count=Count("posted_jobs"))
                .order_by("-job_count")[:5]
            )
            context["top_consultants"] = (
                User.objects.filter(role=User.Role.CONSULTANT, consultant_profile__isnull=False)
                .annotate(submission_count=Count("consultant_profile__submissions"))
                .order_by("-submission_count")[:5]
            )
            role_qs = MarketingRole.objects.all().annotate(
                total=Count("jobs__submissions"),
                applied=Count("jobs__submissions", filter=Q(jobs__submissions__status=ApplicationSubmission.Status.APPLIED)),
                interview=Count("jobs__submissions", filter=Q(jobs__submissions__status=ApplicationSubmission.Status.INTERVIEW)),
                offer=Count("jobs__submissions", filter=Q(jobs__submissions__status=ApplicationSubmission.Status.OFFER)),
                rejected=Count("jobs__submissions", filter=Q(jobs__submissions__status=ApplicationSubmission.Status.REJECTED)),
            )
        context["role_funnel"] = [
            {
                "name": role.name,
                "total": role.total,
                "applied": role.applied,
                "interview": role.interview,
                "offer": role.offer,
                "rejected": role.rejected,
            }
            for role in role_qs
            if role.total
        ]

        context.update(get_submission_funnel_metrics())
        context.update(get_time_to_hire_metrics())
        context.update(get_employee_leaderboard_metrics())
        context.update(get_consultant_performance_metrics())

        funnel_global = context.get("funnel_global") or {}
        context["funnel_global_json"] = {
            "labels": ["Resumes generated", "Submitted", "Interview+", "Offers", "Rejected"],
            "values": [
                funnel_global.get("resumes") or 0,
                funnel_global.get("submitted") or 0,
                funnel_global.get("interview") or 0,
                funnel_global.get("hired") or 0,
                funnel_global.get("rejected") or 0,
            ],
        }

        leaderboard = context.get("employee_leaderboard") or []
        context["leaderboard_chart_json"] = {
            "labels": [(row["user"].get_full_name() or row["user"].username)[:24] for row in leaderboard[:12]],
            "sub_to_interview": [row.get("sub_to_interview_rate_pct") or 0 for row in leaderboard[:12]],
            "hires": [row.get("hires") or 0 for row in leaderboard[:12]],
        }

        board_rows = _platform_health_rows(since)
        context["board_health_rows"] = board_rows

        data_rows = [
            row for row in board_rows
            if row.get("total_jobs") or row.get("total_runs") or row.get("companies_tracked")
        ]
        total_raw_jobs = sum(int(row.get("total_jobs") or 0) for row in data_rows)
        total_fetch_runs = sum(int(row.get("total_runs") or 0) for row in data_rows)
        total_missing_jd = sum(int(row.get("missing_jd") or 0) for row in data_rows)
        total_inactive_jobs = sum(int(row.get("inactive_jobs") or 0) for row in data_rows)
        total_blocked_jobs = sum(int(row.get("blocked_jobs") or 0) for row in data_rows)
        total_portal_down = sum(int(row.get("portal_down") or 0) for row in data_rows)
        total_zero_yield = sum(int(row.get("zero_yield_success_runs") or 0) for row in data_rows)
        total_failed_runs = sum(int(row.get("failed_runs") or 0) for row in data_rows)
        total_partial_runs = sum(int(row.get("partial_runs") or 0) for row in data_rows)
        total_low_conf = sum(int(row.get("low_confidence_jobs") or 0) for row in data_rows)
        total_companies_tracked = sum(int(row.get("companies_tracked") or 0) for row in data_rows)
        avg_risk_score = round(
            sum((row.get("risk_score") or 0) for row in data_rows)
            / max(1, len(data_rows)),
            1,
        )
        high_risk_boards = [row for row in data_rows if row.get("risk_score", 0) >= 60]
        worst_board = data_rows[0] if data_rows else None

        context["harvest_summary"] = {
            "platforms_tracked": len(data_rows),
            "companies_tracked": total_companies_tracked,
            "raw_jobs": total_raw_jobs,
            "fetch_runs": total_fetch_runs,
            "missing_jd": total_missing_jd,
            "missing_jd_rate": _pct(total_missing_jd, total_raw_jobs),
            "inactive_jobs": total_inactive_jobs,
            "inactive_rate": _pct(total_inactive_jobs, total_raw_jobs),
            "blocked_jobs": total_blocked_jobs,
            "blocked_rate": _pct(total_blocked_jobs, total_raw_jobs),
            "portal_down": total_portal_down,
            "failed_runs": total_failed_runs,
            "failed_run_rate": _pct(total_failed_runs, total_fetch_runs),
            "partial_runs": total_partial_runs,
            "partial_run_rate": _pct(total_partial_runs, total_fetch_runs),
            "zero_yield_success_runs": total_zero_yield,
            "low_confidence_jobs": total_low_conf,
            "low_confidence_rate": _pct(total_low_conf, total_raw_jobs),
            "avg_risk_score": avg_risk_score,
            "high_risk_boards": len(high_risk_boards),
            "worst_board_name": worst_board["platform_name"] if worst_board else "—",
            "worst_board_score": worst_board["risk_score"] if worst_board else 0,
        }

        top_risk_rows = data_rows[:8]
        top_volume_rows = sorted(data_rows, key=lambda item: (-(item.get("total_jobs") or 0), item.get("platform_name") or ""))[:8]

        context["board_risk_chart_json"] = {
            "labels": [row["platform_name"] for row in top_risk_rows],
            "risk": [row["risk_score"] for row in top_risk_rows],
            "missing_jd": [row["missing_jd_rate"] for row in top_risk_rows],
            "inactive": [row["inactive_rate"] for row in top_risk_rows],
            "failed_runs": [row["fetch_fail_rate"] for row in top_risk_rows],
            "zero_yield": [row["zero_yield_rate"] for row in top_risk_rows],
        }
        context["board_volume_chart_json"] = {
            "labels": [row["platform_name"] for row in top_volume_rows],
            "jobs": [row.get("total_jobs") or 0 for row in top_volume_rows],
            "runs": [row.get("total_runs") or 0 for row in top_volume_rows],
            "blocked": [row.get("blocked_jobs") or 0 for row in top_volume_rows],
        }
        context["coverage_chart_json"] = {
            "labels": [row["platform_name"] for row in top_volume_rows],
            "jd": [row["jd_coverage_pct"] for row in top_volume_rows],
            "geo": [row["geo_coverage_pct"] for row in top_volume_rows],
            "department": [row["department_coverage_pct"] for row in top_volume_rows],
            "salary": [row["salary_coverage_pct"] for row in top_volume_rows],
            "schedule": [row["schedule_coverage_pct"] for row in top_volume_rows],
            "education": [row["education_coverage_pct"] for row in top_volume_rows],
        }
        context["failure_mix_chart_json"] = {
            "labels": ["Failed", "Partial", "Timeout", "HTTP", "Parse", "No tenant", "Rate limited", "Platform"],
            "values": [
                total_failed_runs,
                total_partial_runs,
                sum(int(row.get("timeout_runs") or 0) for row in board_rows),
                sum(int(row.get("http_error_runs") or 0) for row in board_rows),
                sum(int(row.get("parse_error_runs") or 0) for row in board_rows),
                sum(int(row.get("no_tenant_runs") or 0) for row in board_rows),
                sum(int(row.get("rate_limited_runs") or 0) for row in board_rows),
                sum(int(row.get("platform_error_runs") or 0) for row in board_rows),
            ],
        }
        context["blocker_breakdown_chart_json"] = {
            "labels": ["Missing JD", "Inactive links", "Low confidence", "Blocked jobs", "Zero-yield success"],
            "values": [
                total_missing_jd,
                total_inactive_jobs,
                total_low_conf,
                total_blocked_jobs,
                total_zero_yield,
            ],
        }

        ops_qs = HarvestOpsRun.objects.all()
        if since:
            ops_qs = ops_qs.filter(created_at__gte=since)
        ops_summary = (
            ops_qs.values("operation", "status")
            .annotate(count=Count("id"))
            .order_by()
        )
        ops_map: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for row in ops_summary:
            ops_map[row["operation"]][row["status"]] = row["count"]
        context["ops_health_rows"] = [
            {
                "operation": dict(HarvestOpsRun.Operation.choices).get(operation, operation),
                "success": status_map.get(HarvestOpsRun.Status.SUCCESS, 0),
                "partial": status_map.get(HarvestOpsRun.Status.PARTIAL, 0),
                "failed": status_map.get(HarvestOpsRun.Status.FAILED, 0),
                "running": status_map.get(HarvestOpsRun.Status.RUNNING, 0),
                "skipped": status_map.get(HarvestOpsRun.Status.SKIPPED, 0),
            }
            for operation, status_map in ops_map.items()
        ]

        return context


class AnalyticsExportCSVView(LoginRequiredMixin, EmployeeRequiredMixin, View):
    def get(self, request, *args, **kwargs):
        since, range_param, range_label = _date_range_context(request)
        board_rows = _platform_health_rows(since)

        job_qs = Job.objects.all()
        app_qs = ApplicationSubmission.objects.all()
        if since:
            job_qs = job_qs.filter(created_at__gte=since)
            app_qs = app_qs.filter(created_at__gte=since)

        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = 'attachment; filename="analytics.csv"'
        writer = csv.writer(response)

        writer.writerow(["Section", "Key", "Value"])
        writer.writerow(["Summary", "Date range", range_label])
        writer.writerow(["Summary", "Total jobs", job_qs.count()])
        writer.writerow(["Summary", "Active jobs", job_qs.filter(status=Job.Status.OPEN).count()])
        writer.writerow(["Summary", "Total consultants", User.objects.filter(role=User.Role.CONSULTANT).count()])
        writer.writerow(["Summary", "Total applications", app_qs.count()])

        writer.writerow([])
        writer.writerow([
            "Job board health",
            "Platform",
            "Risk score",
            "Companies tracked",
            "Portal down",
            "Total jobs",
            "Missing JD",
            "Missing JD %",
            "Inactive jobs",
            "Inactive %",
            "Blocked jobs",
            "Blocked %",
            "Fetch runs",
            "Failed runs",
            "Failed runs %",
            "Partial runs",
            "Zero-yield success runs",
            "JD coverage %",
            "Department coverage %",
            "Geo coverage %",
            "Salary coverage %",
            "Schedule coverage %",
            "Education coverage %",
        ])
        for row in board_rows:
            if not (row.get("total_jobs") or row.get("total_runs") or row.get("companies_tracked")):
                continue
            writer.writerow([
                "Job board health",
                row.get("platform_name") or "",
                row.get("risk_score") or 0,
                row.get("companies_tracked") or 0,
                row.get("portal_down") or 0,
                row.get("total_jobs") or 0,
                row.get("missing_jd") or 0,
                row.get("missing_jd_rate") or 0,
                row.get("inactive_jobs") or 0,
                row.get("inactive_rate") or 0,
                row.get("blocked_jobs") or 0,
                row.get("blocked_rate") or 0,
                row.get("total_runs") or 0,
                row.get("failed_runs") or 0,
                row.get("fetch_fail_rate") or 0,
                row.get("partial_runs") or 0,
                row.get("zero_yield_success_runs") or 0,
                row.get("jd_coverage_pct") or 0,
                row.get("department_coverage_pct") or 0,
                row.get("geo_coverage_pct") or 0,
                row.get("salary_coverage_pct") or 0,
                row.get("schedule_coverage_pct") or 0,
                row.get("education_coverage_pct") or 0,
            ])

        writer.writerow([])
        writer.writerow(["Applications status", "Status code", "Count"])
        for row in app_qs.values("status").annotate(count=Count("status")).order_by():
            writer.writerow(["Applications status", row["status"], row["count"]])

        writer.writerow([])
        writer.writerow(["Marketing role funnel", "Role", "Total", "Applied", "Interview", "Offer", "Rejected"])
        role_qs = MarketingRole.objects.all().annotate(
            total=Count("jobs__submissions"),
            applied=Count("jobs__submissions", filter=Q(jobs__submissions__status=ApplicationSubmission.Status.APPLIED)),
            interview=Count("jobs__submissions", filter=Q(jobs__submissions__status=ApplicationSubmission.Status.INTERVIEW)),
            offer=Count("jobs__submissions", filter=Q(jobs__submissions__status=ApplicationSubmission.Status.OFFER)),
            rejected=Count("jobs__submissions", filter=Q(jobs__submissions__status=ApplicationSubmission.Status.REJECTED)),
        )
        for role in role_qs:
            if not role.total:
                continue
            writer.writerow(["Marketing role funnel", role.name, role.total, role.applied, role.interview, role.offer, role.rejected])

        return response
