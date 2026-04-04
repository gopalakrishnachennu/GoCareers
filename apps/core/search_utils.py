"""Shared global search query logic for full page + HTMX partial."""

from django.db.models import Q

from companies.models import Company
from jobs.models import Job
from submissions.models import ApplicationSubmission
from users.models import ConsultantProfile, User


def build_global_search_context(request, q: str) -> dict:
    q = (q or "").strip()
    ctx = {
        "query": q,
        "jobs": [],
        "consultants": [],
        "companies": [],
        "submissions": [],
        "total_results": 0,
    }
    if len(q) < 2:
        return ctx

    ctx["jobs"] = list(
        Job.objects.filter(
            Q(title__icontains=q)
            | Q(company__icontains=q)
            | Q(description__icontains=q)
            | Q(location__icontains=q)
        ).order_by("-created_at")[:10]
    )

    ctx["consultants"] = list(
        ConsultantProfile.objects.filter(
            Q(user__first_name__icontains=q)
            | Q(user__last_name__icontains=q)
            | Q(user__username__icontains=q)
            | Q(bio__icontains=q)
        )
        .select_related("user")
        .order_by("user__first_name")[:10]
    )

    ctx["companies"] = list(
        Company.objects.filter(
            Q(name__icontains=q) | Q(domain__icontains=q) | Q(industry__icontains=q)
        ).order_by("name")[:10]
    )

    user = request.user
    sub_qs = ApplicationSubmission.objects.select_related("job", "consultant__user")
    if user.is_authenticated and user.role == User.Role.CONSULTANT:
        sub_qs = sub_qs.filter(consultant=user.consultant_profile)
    ctx["submissions"] = list(
        sub_qs.filter(
            Q(job__title__icontains=q)
            | Q(job__company__icontains=q)
            | Q(consultant__user__first_name__icontains=q)
            | Q(consultant__user__last_name__icontains=q)
        ).order_by("-created_at")[:10]
    )

    ctx["total_results"] = (
        len(ctx["jobs"])
        + len(ctx["consultants"])
        + len(ctx["companies"])
        + len(ctx["submissions"])
    )
    return ctx
