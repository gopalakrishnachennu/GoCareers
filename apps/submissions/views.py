from django.shortcuts import get_object_or_404, redirect
from django.views.generic import CreateView, ListView, UpdateView
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.urls import reverse_lazy
from django.contrib import messages
from .models import ApplicationSubmission
from .forms import ApplicationSubmissionForm
from resumes.models import Resume
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
        if resume.job != job or resume.consultant != consultant:
            messages.error(self.request, MSG_SUBMISSION_MISMATCH)
            return self.form_invalid(form)
            
        # 2. Permission Check
        if self.request.user.role == 'CONSULTANT' and consultant != self.request.user:
            messages.error(self.request, MSG_SUBMISSION_SELF_ONLY)
            return self.form_invalid(form)

        # 3. File Validation (Basic)
        proof_file = form.cleaned_data.get('proof_file')
        if proof_file:
            if proof_file.size > MAX_UPLOAD_SIZE:
                form.add_error('proof_file', MSG_FILE_TOO_LARGE.format(max_mb=MAX_UPLOAD_SIZE_MB))
                return self.form_invalid(form)

        form.instance.submitted_by = self.request.user
        messages.success(self.request, MSG_SUBMISSION_SUCCESS)
        return super().form_valid(form)

class SubmissionListView(LoginRequiredMixin, ListView):
    model = ApplicationSubmission
    template_name = 'submissions/submission_list.html'
    context_object_name = 'submissions'
    paginate_by = 10

    def get_queryset(self):
        qs = super().get_queryset()
        user = self.request.user
        if user.role == User.Role.CONSULTANT:
            # Consultant sees their own submissions
            return qs.filter(consultant=user)
        elif user.role == User.Role.EMPLOYEE or user.is_superuser:
            # Employee sees all
            return qs
        return qs.none()

class SubmissionUpdateView(LoginRequiredMixin, UserPassesTestMixin, UpdateView):
    model = ApplicationSubmission
    fields = ['status', 'notes', 'proof_file']
    template_name = 'submissions/submission_form.html'
    success_url = reverse_lazy('submission-list')
    
    def test_func(self):
        obj = self.get_object()
        return self.request.user == obj.consultant or self.request.user.role == User.Role.EMPLOYEE or self.request.user.is_superuser
    template_name = 'submissions/submission_form.html'
    success_url = reverse_lazy('submission-list')

    def form_valid(self, form):
        messages.success(self.request, "Submission updated successfully!")
        return super().form_valid(form)
