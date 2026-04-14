"""
Public (unauthenticated) job board: OPEN roles for marketing and demo.
Internal job URLs still require login; this uses a separate URL namespace.
"""

from django.contrib import messages
from django.contrib.auth.views import redirect_to_login
from django.http import Http404
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse
from django.views.generic import DetailView, ListView, View

from users.models import User

from .models import Job


def _public_job_queryset():
    return Job.objects.filter(status=Job.Status.OPEN, is_archived=False).select_related("posted_by", "company_obj")


class PublicJobListView(ListView):
    """SEO-friendly list of open roles (no login)."""

    model = Job
    template_name = "jobs/public_job_list.html"
    context_object_name = "jobs"
    paginate_by = 20

    def get_queryset(self):
        from django.db.models import Q

        qs = _public_job_queryset().order_by("-created_at")
        q = (self.request.GET.get("q") or "").strip()
        if q:
            qs = qs.filter(Q(title__icontains=q) | Q(company__icontains=q) | Q(location__icontains=q))
        return qs


class PublicJobDetailView(DetailView):
    model = Job
    template_name = "jobs/public_job_detail.html"
    context_object_name = "job"

    def get_object(self, queryset=None):
        obj = super().get_object(queryset)
        if obj.status != Job.Status.OPEN or obj.is_archived:
            raise Http404()
        return obj

    def get_queryset(self):
        return _public_job_queryset()


class PublicJobApplyView(View):
    """
    Entry point for "Apply" from the public board.
    - Anonymous → login with next=/jobs/<pk>/ (internal job page after auth).
    - Consultant → internal job detail (full description + quick submit).
    - Other roles → internal job detail with info message.
    """

    def get(self, request, pk):
        job = get_object_or_404(Job, pk=pk)
        if job.status != Job.Status.OPEN or job.is_archived:
            raise Http404()
        next_url = reverse("job-detail", kwargs={"pk": job.pk})
        if not request.user.is_authenticated:
            return redirect_to_login(next_url)
        u = request.user
        if u.role == User.Role.CONSULTANT:
            messages.info(
                request,
                "Review the full job and use Quick submit or your consultant workflow to apply.",
            )
            return redirect(next_url)
        messages.info(request, "Sign in as a consultant to submit applications through the portal.")
        return redirect(next_url)
