from django.shortcuts import render, redirect
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.db.models import Count, Q
from users.models import User
from jobs.models import Job
from submissions.models import ApplicationSubmission


def home(request):
    """Smart redirect: send each role to their own dashboard."""
    if not request.user.is_authenticated:
        return render(request, 'home.html')

    role = request.user.role
    if request.user.is_superuser or role == 'ADMIN':
        return redirect('admin-dashboard')
    elif role == 'EMPLOYEE':
        return redirect('employee-dashboard')
    elif role == 'CONSULTANT':
        return redirect('consultant-dashboard')
    return render(request, 'home.html')


class AdminDashboardView(LoginRequiredMixin, UserPassesTestMixin, TemplateView):
    template_name = 'core/admin_dashboard.html'

    def test_func(self):
        return self.request.user.is_superuser or self.request.user.role == 'ADMIN'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['total_jobs'] = Job.objects.count()
        context['active_jobs'] = Job.objects.filter(status='OPEN').count()
        context['total_consultants'] = User.objects.filter(role=User.Role.CONSULTANT).count()
        context['total_employees'] = User.objects.filter(role=User.Role.EMPLOYEE).count()
        context['total_applications'] = ApplicationSubmission.objects.count()
        context['recent_jobs'] = Job.objects.order_by('-created_at')[:5]
        context['recent_applications'] = ApplicationSubmission.objects.select_related('job', 'consultant').order_by('-created_at')[:5]
        return context


class EmployeeDashboardView(LoginRequiredMixin, UserPassesTestMixin, TemplateView):
    template_name = 'core/employee_dashboard.html'

    def test_func(self):
        u = self.request.user
        return u.role == User.Role.EMPLOYEE or u.is_superuser or u.role == 'ADMIN'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        user = self.request.user
        my_jobs = Job.objects.filter(posted_by=user)
        context['my_jobs_count'] = my_jobs.count()
        context['my_open_jobs'] = my_jobs.filter(status='OPEN').count()
        my_job_ids = my_jobs.values_list('id', flat=True)
        apps_for_my_jobs = ApplicationSubmission.objects.filter(job_id__in=my_job_ids)
        context['total_apps_received'] = apps_for_my_jobs.count()
        context['pending_apps'] = apps_for_my_jobs.filter(status='APPLIED').count()
        context['recent_my_jobs'] = my_jobs.order_by('-created_at')[:5]
        context['recent_apps'] = apps_for_my_jobs.select_related('job', 'consultant').order_by('-created_at')[:5]
        context['all_open_jobs'] = Job.objects.filter(status='OPEN').count()
        return context
