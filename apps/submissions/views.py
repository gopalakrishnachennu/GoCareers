from django.shortcuts import get_object_or_404, redirect
from django.views.generic import CreateView, ListView, UpdateView, View, DetailView
from django.db.models import Q
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.urls import reverse_lazy
from django.contrib import messages
from django.utils import timezone
from .models import ApplicationSubmission
from .forms import ApplicationSubmissionForm, SubmissionResponseForm
from resumes.models import Resume, ResumeDraft
from users.models import User
from config.constants import (
    PAGINATION_SUBMISSIONS, MAX_UPLOAD_SIZE, MAX_UPLOAD_SIZE_MB,
    MSG_SUBMISSION_SUCCESS, MSG_SUBMISSION_MISMATCH, MSG_SUBMISSION_SELF_ONLY, MSG_FILE_TOO_LARGE,
)

class SubmissionCreateView(LoginRequiredMixin, CreateView):
    model = ApplicationSubmission
    form_class = ApplicationSubmissionForm
    template_name = 'submissions/submission_form.html'
    success_url = reverse_lazy('submission-list')

    def get_initial(self):
        initial = super().get_initial()
        resume_id = self.request.GET.get('resume_id')
        if resume_id:
            resume = get_object_or_404(Resume, pk=resume_id)
            initial['resume'] = resume
            initial['job'] = resume.job
            initial['consultant'] = resume.consultant
        return initial

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        resume_id = self.request.GET.get('resume_id')
        if resume_id:
            context['resume'] = get_object_or_404(Resume, pk=resume_id)
        return context

    def form_valid(self, form):
        job = form.cleaned_data['job']
        consultant = form.cleaned_data['consultant']
        resume = form.cleaned_data['resume']
        
        # 1. Consistency Check
        if not resume or resume.job != job or resume.consultant != consultant:
            messages.error(self.request, MSG_SUBMISSION_MISMATCH)
            return self.form_invalid(form)
            
        # 2. Permission Check
        if self.request.user.role == 'CONSULTANT' and consultant.user != self.request.user:
            messages.error(self.request, MSG_SUBMISSION_SELF_ONLY)
            return self.form_invalid(form)

        # 3. File Validation (Basic)
        proof_file = form.cleaned_data.get('proof_file')
        if proof_file:
            if proof_file.size > MAX_UPLOAD_SIZE:
                form.add_error('proof_file', MSG_FILE_TOO_LARGE.format(max_mb=MAX_UPLOAD_SIZE_MB))
                return self.form_invalid(form)
            if not form.instance.submitted_at:
                form.instance.submitted_at = timezone.now()

        form.instance.submitted_by = self.request.user
        if proof_file and form.instance.status == ApplicationSubmission.Status.IN_PROGRESS:
            form.instance.status = ApplicationSubmission.Status.APPLIED
        messages.success(self.request, MSG_SUBMISSION_SUCCESS)
        return super().form_valid(form)

class SubmissionListView(LoginRequiredMixin, ListView):
    model = ApplicationSubmission
    template_name = 'submissions/submission_list.html'
    context_object_name = 'submissions'
    paginate_by = 10

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['status_choices'] = ApplicationSubmission.Status.choices
        return context

    def get_queryset(self):
        qs = super().get_queryset()
        user = self.request.user
        status = self.request.GET.get('status')
        search = self.request.GET.get('search')
        if user.role == User.Role.CONSULTANT:
            # Consultant sees their own submissions
            qs = qs.filter(consultant=user.consultant_profile)
        elif user.role == User.Role.EMPLOYEE or user.is_superuser:
            # Employee sees all
            qs = qs
        else:
            return qs.none()

        if status:
            qs = qs.filter(status=status)
        if search:
            qs = qs.filter(Q(job__title__icontains=search) | Q(job__company__icontains=search))
        return qs

class SubmissionUpdateView(LoginRequiredMixin, UserPassesTestMixin, UpdateView):
    model = ApplicationSubmission
    fields = ['status', 'notes', 'proof_file']
    template_name = 'submissions/submission_form.html'
    success_url = reverse_lazy('submission-list')
    
    def test_func(self):
        obj = self.get_object()
        return self.request.user == obj.consultant.user or self.request.user.role == User.Role.EMPLOYEE or self.request.user.is_superuser
    template_name = 'submissions/submission_form.html'
    success_url = reverse_lazy('submission-list')

    def form_valid(self, form):
        proof_file = form.cleaned_data.get('proof_file')
        if proof_file and not form.instance.submitted_at:
            form.instance.submitted_at = timezone.now()
        messages.success(self.request, "Submission updated successfully!")
        return super().form_valid(form)


class SubmissionDetailView(LoginRequiredMixin, UserPassesTestMixin, DetailView):
    model = ApplicationSubmission
    template_name = 'submissions/submission_detail.html'
    context_object_name = 'submission'

    def test_func(self):
        obj = self.get_object()
        u = self.request.user
        if u.is_superuser or u.role == User.Role.EMPLOYEE:
            return True
        return u.role == User.Role.CONSULTANT and hasattr(u, 'consultant_profile') and obj.consultant == u.consultant_profile

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['response_form'] = SubmissionResponseForm()
        return context

    def post(self, request, *args, **kwargs):
        self.object = self.get_object()
        form = SubmissionResponseForm(request.POST)
        if form.is_valid():
            response = form.save(commit=False)
            response.submission = self.object
            response.created_by = request.user
            response.save()
            messages.success(request, "Response added.")
            return redirect('submission-detail', pk=self.object.pk)
        context = self.get_context_data()
        context['response_form'] = form
        return self.render_to_response(context)


class SubmissionClaimView(LoginRequiredMixin, UserPassesTestMixin, View):
    """Create an IN_PROGRESS submission for a draft (claim job)."""

    def test_func(self):
        u = self.request.user
        return u.is_superuser or u.role in (User.Role.ADMIN, User.Role.EMPLOYEE)

    def post(self, request, draft_id):
        draft = get_object_or_404(ResumeDraft, pk=draft_id)
        consultant = draft.consultant

        submission, created = ApplicationSubmission.objects.get_or_create(
            job=draft.job,
            consultant=consultant,
            defaults={
                'resume': draft,
                'status': ApplicationSubmission.Status.IN_PROGRESS,
                'submitted_by': request.user,
            },
        )

        if not created:
            if submission.status != ApplicationSubmission.Status.IN_PROGRESS:
                messages.warning(
                    request,
                    f"Submission already exists for {draft.job.title} and is marked as {submission.get_status_display()}."
                )
            else:
                if submission.resume != draft:
                    submission.resume = draft
                    submission.save(update_fields=['resume', 'updated_at'])
                messages.info(request, f"{draft.job.title} is already claimed.")
        else:
            messages.success(request, f"Claimed {draft.job.title} for application.")

        return redirect('consultant-detail', pk=consultant.user.pk)
