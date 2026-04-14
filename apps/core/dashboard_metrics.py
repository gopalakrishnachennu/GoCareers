"""
Reusable analytics metrics shared by Admin Dashboard and /analytics/.

Extracted from AdminDashboardView so charts stay consistent without duplicating queries.
"""

from django.db.models import Count, Q

from jobs.models import Job
from resumes.models import ResumeDraft
from submissions.models import ApplicationSubmission
from users.models import User, ConsultantProfile


def get_submission_funnel_metrics():
    """Submission funnel: resumes → submitted → interview → hired + per-employee slices."""
    AS = ApplicationSubmission
    resumes_generated = ResumeDraft.objects.count()
    submitted = AS.objects.filter(
        status__in=[
            AS.Status.APPLIED,
            AS.Status.INTERVIEW,
            AS.Status.OFFER,
            AS.Status.REJECTED,
            AS.Status.WITHDRAWN,
        ]
    ).count()
    interview_stage = AS.objects.filter(status__in=[AS.Status.INTERVIEW, AS.Status.OFFER]).count()
    hired = AS.objects.filter(status=AS.Status.OFFER).count()
    rejected = AS.objects.filter(status=AS.Status.REJECTED).count()

    def pct(prev, curr):
        if not prev:
            return None
        return round((curr / prev) * 100)

    funnel_global = {
        "resumes": resumes_generated,
        "submitted": submitted,
        "interview": interview_stage,
        "hired": hired,
        "rejected": rejected,
        "drop_off_resumes_to_submitted": (resumes_generated - submitted) if resumes_generated else 0,
        "drop_off_submitted_to_interview": (submitted - interview_stage) if submitted else 0,
        "drop_off_interview_to_hired": (interview_stage - hired) if interview_stage else 0,
        "conv_resumes_to_submitted_pct": pct(resumes_generated, submitted),
        "conv_submitted_to_interview_pct": pct(submitted, interview_stage),
        "conv_interview_to_hired_pct": pct(interview_stage, hired),
        "rejection_rate_submitted_pct": pct(submitted, rejected),
    }

    employees = User.objects.filter(role=User.Role.EMPLOYEE).select_related("employee_profile")
    employee_funnels = []
    for emp in employees:
        job_ids = Job.objects.filter(posted_by=emp).values_list("id", flat=True)
        if not job_ids:
            employee_funnels.append(
                {
                    "user": emp,
                    "resumes": 0,
                    "submitted": 0,
                    "interview": 0,
                    "hired": 0,
                    "rejected": 0,
                    "drop_off_resumes_to_submitted": 0,
                    "drop_off_submitted_to_interview": 0,
                    "drop_off_interview_to_hired": 0,
                    "conv_resumes_to_submitted_pct": None,
                    "conv_submitted_to_interview_pct": None,
                    "conv_interview_to_hired_pct": None,
                    "rejection_rate_submitted_pct": None,
                }
            )
            continue
        r = ResumeDraft.objects.filter(job_id__in=job_ids).count()
        sub = AS.objects.filter(
            job_id__in=job_ids,
            status__in=[
                AS.Status.APPLIED,
                AS.Status.INTERVIEW,
                AS.Status.OFFER,
                AS.Status.REJECTED,
                AS.Status.WITHDRAWN,
            ],
        ).count()
        intr = AS.objects.filter(job_id__in=job_ids, status__in=[AS.Status.INTERVIEW, AS.Status.OFFER]).count()
        h = AS.objects.filter(job_id__in=job_ids, status=AS.Status.OFFER).count()
        rej = AS.objects.filter(job_id__in=job_ids, status=AS.Status.REJECTED).count()
        employee_funnels.append(
            {
                "user": emp,
                "resumes": r,
                "submitted": sub,
                "interview": intr,
                "hired": h,
                "rejected": rej,
                "drop_off_resumes_to_submitted": (r - sub) if r else 0,
                "drop_off_submitted_to_interview": (sub - intr) if sub else 0,
                "drop_off_interview_to_hired": (intr - h) if intr else 0,
                "conv_resumes_to_submitted_pct": pct(r, sub),
                "conv_submitted_to_interview_pct": pct(sub, intr),
                "conv_interview_to_hired_pct": pct(intr, h),
                "rejection_rate_submitted_pct": pct(sub, rej),
            }
        )

    return {"funnel_global": funnel_global, "funnel_by_employee": employee_funnels}


def get_time_to_hire_metrics():
    """Average days per pipeline stage and bottleneck label."""
    try:
        from interviews_app.models import Interview
    except ImportError:
        Interview = None

    AS = ApplicationSubmission
    submissions_with_submit = AS.objects.filter(submitted_at__isnull=False).select_related("consultant", "job")
    stage1_days_list = []
    for sub in submissions_with_submit:
        first_draft = (
            ResumeDraft.objects.filter(consultant=sub.consultant, job=sub.job).order_by("created_at").first()
        )
        if first_draft and sub.submitted_at:
            delta = sub.submitted_at - first_draft.created_at
            if delta.total_seconds() >= 0:
                stage1_days_list.append(delta.total_seconds() / 86400)
    avg_draft_to_submit = round(sum(stage1_days_list) / len(stage1_days_list), 1) if stage1_days_list else None

    stage2_days_list = []
    if Interview is not None:
        for sub in AS.objects.filter(
            status__in=[AS.Status.INTERVIEW, AS.Status.OFFER],
            submitted_at__isnull=False,
        ):
            first_int = Interview.objects.filter(submission=sub).order_by("scheduled_at").first()
            if first_int and sub.submitted_at:
                delta = first_int.scheduled_at - sub.submitted_at
                secs = getattr(delta, "total_seconds", lambda: delta.days * 86400 + delta.seconds)()
                if secs >= 0:
                    stage2_days_list.append(secs / 86400)
        avg_submit_to_interview = (
            round(sum(stage2_days_list) / len(stage2_days_list), 1) if stage2_days_list else None
        )
    else:
        avg_submit_to_interview = None

    offer_subs = AS.objects.filter(status=AS.Status.OFFER, submitted_at__isnull=False)
    stage3_days_list = []
    for sub in offer_subs:
        delta = sub.updated_at - sub.submitted_at
        if delta.total_seconds() >= 0:
            stage3_days_list.append(delta.total_seconds() / 86400)
    avg_submit_to_offer = round(sum(stage3_days_list) / len(stage3_days_list), 1) if stage3_days_list else None

    stages = [
        ("Draft → Submit", avg_draft_to_submit),
        ("Submit → Interview", avg_submit_to_interview),
        ("Submit → Offer", avg_submit_to_offer),
    ]
    valid_stages = [(n, d) for n, d in stages if d is not None]
    bottleneck = max(valid_stages, key=lambda x: x[1])[0] if valid_stages else None

    return {
        "time_to_hire_avg_draft_to_submit": avg_draft_to_submit,
        "time_to_hire_avg_submit_to_interview": avg_submit_to_interview,
        "time_to_hire_avg_submit_to_offer": avg_submit_to_offer,
        "time_to_hire_stages": stages,
        "time_to_hire_bottleneck": bottleneck,
    }


def get_employee_leaderboard_metrics():
    """Recruiter leaderboard: submissions → interviews → hires."""
    AS = ApplicationSubmission
    employees = User.objects.filter(role=User.Role.EMPLOYEE).select_related("employee_profile")
    rows = []
    for emp in employees:
        job_ids = list(Job.objects.filter(posted_by=emp).values_list("id", flat=True))
        if not job_ids:
            rows.append(
                {
                    "user": emp,
                    "submissions": 0,
                    "interviews": 0,
                    "hires": 0,
                    "sub_to_interview_rate_pct": None,
                    "rejections": 0,
                    "rejection_rate_pct": None,
                }
            )
            continue
        sub = AS.objects.filter(
            job_id__in=job_ids,
            status__in=[
                AS.Status.APPLIED,
                AS.Status.INTERVIEW,
                AS.Status.OFFER,
                AS.Status.REJECTED,
                AS.Status.WITHDRAWN,
            ],
        ).count()
        intr = AS.objects.filter(job_id__in=job_ids, status__in=[AS.Status.INTERVIEW, AS.Status.OFFER]).count()
        h = AS.objects.filter(job_id__in=job_ids, status=AS.Status.OFFER).count()
        rej = AS.objects.filter(job_id__in=job_ids, status=AS.Status.REJECTED).count()
        rate = round((intr / sub) * 100) if sub else None
        rej_rate = round((rej / sub) * 100) if sub else None
        rows.append(
            {
                "user": emp,
                "submissions": sub,
                "interviews": intr,
                "hires": h,
                "rejections": rej,
                "sub_to_interview_rate_pct": rate,
                "rejection_rate_pct": rej_rate,
            }
        )
    rows.sort(
        key=lambda x: (
            x["sub_to_interview_rate_pct"] is None,
            -(x["sub_to_interview_rate_pct"] or 0),
            -x["hires"],
            -x["interviews"],
        )
    )
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return {"employee_leaderboard": rows}


def get_consultant_performance_metrics():
    """Consultant performance rows (same logic as admin dashboard)."""
    AS = ApplicationSubmission
    consultants = ConsultantProfile.objects.select_related("user").annotate(
        total_sub=Count("submissions"),
        interview_count=Count(
            "submissions",
            filter=Q(submissions__status__in=[AS.Status.INTERVIEW, AS.Status.OFFER]),
        ),
        offer_count=Count("submissions", filter=Q(submissions__status=AS.Status.OFFER)),
        rejected_count=Count("submissions", filter=Q(submissions__status=AS.Status.REJECTED)),
    )
    consultant_perf = []
    for c in consultants:
        total = c.total_sub
        if total == 0:
            consultant_perf.append(
                {
                    "consultant": c,
                    "total_submissions": 0,
                    "interview_rate_pct": None,
                    "hire_rate_pct": None,
                    "rejected_count": c.rejected_count,
                    "rejection_rate_pct": None,
                    "avg_response_days": None,
                    "performance_score": None,
                }
            )
            continue
        interview_rate = round((c.interview_count / total) * 100)
        hire_rate = round((c.offer_count / total) * 100)
        rejection_rate = round((c.rejected_count / total) * 100) if total else None
        subs_with_response = list(
            AS.objects.filter(
                consultant=c,
                submitted_at__isnull=False,
                status__in=[
                    AS.Status.INTERVIEW,
                    AS.Status.OFFER,
                    AS.Status.REJECTED,
                ],
            ).values_list("submitted_at", "updated_at")
        )
        if subs_with_response:
            days_list = [(u - s).total_seconds() / 86400 for s, u in subs_with_response if u and s]
            avg_days = sum(days_list) / len(days_list) if days_list else None
        else:
            avg_days = None
        score = min(interview_rate * 0.4, 40) + min(hire_rate * 0.5, 50)
        if avg_days is not None and avg_days <= 14:
            score += 10
        performance_score = min(100, round(score))
        consultant_perf.append(
            {
                "consultant": c,
                "total_submissions": total,
                "interview_rate_pct": interview_rate,
                "hire_rate_pct": hire_rate,
                "rejected_count": c.rejected_count,
                "rejection_rate_pct": rejection_rate,
                "avg_response_days": round(avg_days, 1) if avg_days is not None else None,
                "performance_score": performance_score,
            }
        )
    consultant_perf.sort(key=lambda x: (x["performance_score"] is None, -(x["performance_score"] or 0)))
    return {"consultant_performance": consultant_perf}
