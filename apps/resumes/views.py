from django.urls import reverse_lazy, reverse
from django.views.generic import DetailView, View
from django.views import View as BaseView
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from .models import ResumeDraft
from .forms import DraftGenerateForm
from .services import LLMService, DocxService, build_input_summary, get_system_prompt_text
from users.models import ConsultantProfile
from jobs.models import Job
from core.models import LLMConfig
from prompts_app.models import Prompt


class AdminOrEmployeeMixin(LoginRequiredMixin, UserPassesTestMixin):
    """Only Admins and Employees can access draft features."""
    def test_func(self):
        u = self.request.user
        return u.is_superuser or u.role in ('ADMIN', 'EMPLOYEE')


class DraftAccessMixin(LoginRequiredMixin, UserPassesTestMixin):
    """Admins/Employees or the owning consultant can view/download drafts."""
    def test_func(self):
        u = self.request.user
        if u.is_superuser or u.role in ('ADMIN', 'EMPLOYEE'):
            return True
        if u.role == 'CONSULTANT' and hasattr(u, 'consultant_profile'):
            draft_id = self.kwargs.get('pk')
            return ResumeDraft.objects.filter(pk=draft_id, consultant=u.consultant_profile).exists()
        return False


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
        llm = LLMService()
        user_prompt = llm._build_prompt(job, consultant_profile)
        draft.llm_system_prompt = get_system_prompt_text(job, consultant_profile)
        draft.llm_user_prompt = user_prompt
        draft.llm_input_summary = build_input_summary(job, consultant_profile)
        draft.save()

        # Generate content
        content, tokens, error = llm.generate_resume_content(job, consultant_profile, actor=request.user)

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


class DraftGenerateAllView(AdminOrEmployeeMixin, BaseView):
    """POST: Generate drafts for all eligible jobs for a consultant."""

    def post(self, request, pk):
        consultant_profile = get_object_or_404(ConsultantProfile, user__pk=pk)
        roles = consultant_profile.marketing_roles.all()

        if not roles:
            messages.error(request, "This consultant has no marketing roles assigned.")
            return redirect('consultant-detail', pk=pk)

        # Use the same queryset as the dropdown (OPEN + consultant marketing roles)
        form = DraftGenerateForm()
        form.fields['job'].queryset = Job.objects.filter(
            status='OPEN',
            marketing_roles__in=roles
        ).distinct()
        eligible_jobs = form.fields['job'].queryset

        existing_job_ids = ResumeDraft.objects.filter(consultant=consultant_profile).values_list('job_id', flat=True)
        jobs_to_generate = eligible_jobs.exclude(id__in=existing_job_ids)

        if not jobs_to_generate.exists():
            messages.info(request, "All eligible jobs already have drafts.")
            return redirect('consultant-detail', pk=pk)

        llm = LLMService()
        created_count = 0
        error_count = 0

        for job in jobs_to_generate:
            draft = ResumeDraft(
                consultant=consultant_profile,
                job=job,
                status=ResumeDraft.Status.PROCESSING,
                created_by=request.user,
            )
            user_prompt = llm._build_prompt(job, consultant_profile)
            draft.llm_system_prompt = get_system_prompt_text(job, consultant_profile)
            draft.llm_user_prompt = user_prompt
            draft.llm_input_summary = build_input_summary(job, consultant_profile)
            draft.save()

            content, tokens, error = llm.generate_resume_content(job, consultant_profile, actor=request.user)
            if error:
                draft.status = ResumeDraft.Status.ERROR
                draft.error_message = error
                draft.save(skip_version=True)
                error_count += 1
            else:
                draft.status = ResumeDraft.Status.DRAFT
                draft.content = content
                draft.tokens_used = tokens
                draft.save(skip_version=True)
                created_count += 1

        if created_count:
            messages.success(request, f"Generated {created_count} drafts.")
        if error_count:
            messages.warning(request, f"{error_count} drafts failed to generate.")

        return redirect('consultant-detail', pk=pk)


class DraftDetailView(DraftAccessMixin, DetailView):
    """View a single draft's generated content."""
    model = ResumeDraft
    template_name = 'resumes/draft_detail.html'
    context_object_name = 'draft'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        draft = context['draft']
        llm = LLMService()
        user_prompt = draft.llm_user_prompt or llm._build_prompt(draft.job, draft.consultant)
        system_prompt = draft.llm_system_prompt or get_system_prompt_text(draft.job, draft.consultant)
        context['llm_system_prompt'] = system_prompt
        context['llm_user_prompt'] = user_prompt
        config = LLMConfig.load()
        context['prompt_options'] = Prompt.objects.filter(is_active=True).order_by('name')
        context['selected_prompt_id'] = config.active_prompt_id
        context['selected_prompt_name'] = config.active_prompt.name if config.active_prompt else None
        context['llm_input_summary'] = draft.llm_input_summary or build_input_summary(draft.job, draft.consultant)
        return context


class DraftSetPromptView(AdminOrEmployeeMixin, BaseView):
    """Set the active prompt used for LLM generation (global)."""

    def post(self, request, pk):
        prompt_id = request.POST.get('prompt_id')
        config = LLMConfig.load()

        if not prompt_id:
            config.active_prompt = None
            config.save()
            messages.success(request, "Prompt selection cleared.")
            return redirect('draft-detail', pk=pk)

        prompt = get_object_or_404(Prompt, pk=prompt_id, is_active=True)
        config.active_prompt = prompt
        config.save()
        messages.success(request, f"Prompt set to: {prompt.name}")
        return redirect('draft-detail', pk=pk)


class DraftDownloadView(DraftAccessMixin, BaseView):
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
        return u.is_superuser or u.role in ('ADMIN', 'EMPLOYEE')

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
