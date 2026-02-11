from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.db.models import Count
from django.db.models.functions import TruncMonth
from jobs.models import Job
from submissions.models import ApplicationSubmission
from users.models import User
import json
from django.core.serializers.json import DjangoJSONEncoder

class EmployeeRequiredMixin(UserPassesTestMixin):
    def test_func(self):
        return self.request.user.role == User.Role.EMPLOYEE or self.request.user.is_superuser

class AnalyticsDashboardView(LoginRequiredMixin, EmployeeRequiredMixin, TemplateView):
    template_name = 'analytics/dashboard.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # 1. Key Metrics
        context['total_jobs'] = Job.objects.count()
        context['active_jobs'] = Job.objects.filter(status='OPEN').count()
        context['total_consultants'] = User.objects.filter(role=User.Role.CONSULTANT).count()
        context['total_applications'] = ApplicationSubmission.objects.count()

        # 2. Applications by Status (Pie Chart)
        app_status_data = ApplicationSubmission.objects.values('status').annotate(count=Count('status'))
        context['app_status_labels'] = json.dumps([item['status'] for item in app_status_data], cls=DjangoJSONEncoder)
        context['app_status_data'] = json.dumps([item['count'] for item in app_status_data], cls=DjangoJSONEncoder)

        # 3. Jobs Posted Over Time (Line Chart)
        jobs_over_time = Job.objects.annotate(month=TruncMonth('created_at')).values('month').annotate(count=Count('id')).order_by('month')
        context['jobs_time_labels'] = json.dumps([item['month'].strftime('%b %Y') for item in jobs_over_time], cls=DjangoJSONEncoder)
        context['jobs_time_data'] = json.dumps([item['count'] for item in jobs_over_time], cls=DjangoJSONEncoder)

        return context
