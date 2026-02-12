from django.urls import reverse_lazy, reverse
from django.views.generic import DetailView, View
from django.views import View as BaseView
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from .models import ResumeDraft
from .forms import DraftGenerateForm
from .services import LLMService, DocxService
from users.models import ConsultantProfile


class AdminOrEmployeeMixin(LoginRequiredMixin, UserPassesTestMixin):
    """Only Admins and Employees can access draft features."""
    def test_func(self):
        u = self.request.user
        return u.is_superuser or u.role in ('ADMIN', 'EMPLOYEE')


class DraftGenerateView(AdminOrEmployeeMixin, BaseView):
    """POST: Generate a new resume draft for a consultant + job."""

    def post(self, request, pk):
        consultant_profile = get_object_or_404(ConsultantProfile, user__pk=pk)
        form = DraftGenerateForm(request.POST)

        if not form.is_valid():
            messages.error(request, "Please select a valid job.")
            return redirect('consultant-detail', pk=pk)

        job = form.cleaned_data['job']

        # Create draft record in PROCESSING state
        draft = ResumeDraft(
            consultant=consultant_profile,
            job=job,
            status=ResumeDraft.Status.PROCESSING,
            created_by=request.user,
        )
        draft.save()

        # Generate content
        llm = LLMService()
        content, tokens, error = llm.generate_resume_content(job, consultant_profile)

        if error:
            draft.status = ResumeDraft.Status.ERROR
            draft.error_message = error
            draft.save(skip_version=True)
            messages.error(request, f"Draft generation failed: {error}")
        else:
            draft.status = ResumeDraft.Status.DRAFT
            draft.content = content
            draft.tokens_used = tokens
            draft.save(skip_version=True)
            messages.success(
                request,
                f"Resume draft v{draft.version} generated for {consultant_profile.user.get_full_name() or consultant_profile.user.username}!"
            )

        return redirect('consultant-detail', pk=pk)


class DraftDetailView(AdminOrEmployeeMixin, DetailView):
    """View a single draft's generated content."""
    model = ResumeDraft
    template_name = 'resumes/draft_detail.html'
    context_object_name = 'draft'


class DraftDownloadView(AdminOrEmployeeMixin, BaseView):
    """Download a draft as .docx."""

    def get(self, request, pk):
        draft = get_object_or_404(ResumeDraft, pk=pk)

        if not draft.content:
            messages.error(request, "This draft has no content to download.")
            return redirect('draft-detail', pk=pk)

        docx_service = DocxService()
        buffer = docx_service.create_docx(draft.content)

        filename = f"resume_{draft.consultant.user.username}_{draft.job.title.replace(' ', '_')}_v{draft.version}.docx"

        response = HttpResponse(
            buffer.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response


class DraftPromoteView(AdminOrEmployeeMixin, BaseView):
    """Promote a draft to FINAL status. Only one FINAL per consultant+job."""

    def test_func(self):
        u = self.request.user
        return u.is_superuser or u.role == 'ADMIN'

    def post(self, request, pk):
        draft = get_object_or_404(ResumeDraft, pk=pk)

        # Demote any existing FINAL for this consultant+job
        ResumeDraft.objects.filter(
            consultant=draft.consultant, job=draft.job, status=ResumeDraft.Status.FINAL
        ).update(status=ResumeDraft.Status.DRAFT)

        draft.status = ResumeDraft.Status.FINAL
        draft.save(skip_version=True)
        messages.success(request, f"Draft v{draft.version} promoted to FINAL.")
        return redirect('consultant-detail', pk=draft.consultant.user.pk)


class DraftDeleteView(AdminOrEmployeeMixin, BaseView):
    """Delete a draft. Admin only."""

    def test_func(self):
        u = self.request.user
        return u.is_superuser or u.role == 'ADMIN'

    def post(self, request, pk):
        draft = get_object_or_404(ResumeDraft, pk=pk)
        consultant_pk = draft.consultant.user.pk
        draft.delete()
        messages.success(request, "Draft deleted.")
        return redirect('consultant-detail', pk=consultant_pk)


# ─── Legacy views (kept for backward compat) ─────────────────────────
class ResumeCreateView(AdminOrEmployeeMixin, BaseView):
    """Legacy resume creation — redirects to consultant list."""
    def get(self, request):
        return redirect('consultant-list')


class ResumeDetailView(AdminOrEmployeeMixin, DetailView):
    """Legacy — redirects to new draft detail."""
    model = ResumeDraft
    template_name = 'resumes/draft_detail.html'
    context_object_name = 'draft'


class ResumeDownloadView(DraftDownloadView):
    """Legacy alias."""
    pass
