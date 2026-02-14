from django.shortcuts import render, redirect
from django.views.generic import TemplateView, UpdateView, View
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.db.models import Count, Q, Sum
from django.urls import reverse_lazy
from django.contrib import messages
from django.utils import timezone
from datetime import timedelta
from users.models import User
from jobs.models import Job
from submissions.models import ApplicationSubmission
from .models import PlatformConfig, LLMConfig, LLMUsageLog
from .forms import PlatformConfigForm, LLMConfigForm
from .monitor import SystemMonitor
from .security import decrypt_value
from .llm_services import list_openai_models, sort_models_by_cost, get_cost_info
from .llm_pricing import PRICING_PER_1M

class AdminRequiredMixin(LoginRequiredMixin, UserPassesTestMixin):
    def test_func(self):
        return self.request.user.is_superuser or self.request.user.role == 'ADMIN'

class SystemStatusView(AdminRequiredMixin, TemplateView):
    template_name = 'settings/system_status.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        monitor = SystemMonitor()
        context['health_check'] = monitor.check_all()
        return context

class PlatformConfigView(AdminRequiredMixin, UpdateView):
    model = PlatformConfig
    form_class = PlatformConfigForm
    template_name = 'settings/platform_config.html'
    success_url = reverse_lazy('platform-config')

    def get_object(self, queryset=None):
        return PlatformConfig.load()

    def form_valid(self, form):
        messages.success(self.request, "Platform configuration updated successfully.")
        return super().form_valid(form)


class LLMConfigView(AdminRequiredMixin, View):
    template_name = 'settings/llm_config.html'

    def _build_model_choices(self, api_key: str):
        models = []
        if api_key:
            try:
                models = list_openai_models(api_key)
            except Exception as exc:
                self._model_error = str(exc)
        if not models:
            models = list(PRICING_PER_1M.keys())
        models = sort_models_by_cost(models)
        choices = []
        for m in models:
            info = get_cost_info(m)
            if info:
                label = f"{m} — ${info['input']}/$ {info['output']} per 1M"
                label = label.replace('$ ', '$')
            else:
                label = f"{m} — cost unknown"
            choices.append((m, label))
        return choices

    def get(self, request):
        config = LLMConfig.load()
        api_key = decrypt_value(config.encrypted_api_key)
        form = LLMConfigForm(instance=config)
        form.fields['active_model'].choices = self._build_model_choices(api_key)

        context = self._build_metrics_context()
        context.update({
            'form': form,
            'api_key_masked': (api_key[:4] + '…' + api_key[-4:]) if api_key else '',
            'model_error': getattr(self, '_model_error', ''),
        })
        return render(request, self.template_name, context)

    def post(self, request):
        config = LLMConfig.load()
        api_key = decrypt_value(config.encrypted_api_key)
        api_key_for_models = request.POST.get('api_key') or api_key
        form = LLMConfigForm(request.POST, instance=config)
        form.fields['active_model'].choices = self._build_model_choices(api_key_for_models)

        action = request.POST.get('action')
        if action == 'test_key':
            test_key = api_key_for_models
            if not test_key:
                messages.error(request, "Please enter an API key to test.")
            else:
                try:
                    _ = list_openai_models(test_key)
                    messages.success(request, "API key is valid. Models fetched successfully.")
                except Exception as exc:
                    messages.error(request, f"API key test failed: {exc}")
            context = self._build_metrics_context()
            context.update({
                'form': form,
                'api_key_masked': (api_key[:4] + '…' + api_key[-4:]) if api_key else '',
            })
            return render(request, self.template_name, context)

        if form.is_valid():
            form.save()
            messages.success(request, "LLM configuration updated successfully.")
            return redirect('llm-config')

        context = self._build_metrics_context()
        context.update({
            'form': form,
            'api_key_masked': (api_key[:4] + '…' + api_key[-4:]) if api_key else '',
        })
        return render(request, self.template_name, context)

    def _build_metrics_context(self):
        now = timezone.now()
        start_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start_week = now - timedelta(days=7)
        start_day = now - timedelta(days=1)

        logs = LLMUsageLog.objects.all()
        total_calls = logs.count()
        success_calls = logs.filter(success=True).count()
        failed_calls = logs.filter(success=False).count()
        total_tokens = logs.aggregate(total=Sum('total_tokens'))['total'] or 0
        total_cost = logs.aggregate(total=Sum('cost_total'))['total'] or 0
        total_latency = logs.aggregate(total=Sum('latency_ms'))['total'] or 0
        avg_latency = int(total_latency / total_calls) if total_calls else 0

        return {
            'llm_config': LLMConfig.load(),
            'total_calls': total_calls,
            'success_calls': success_calls,
            'failed_calls': failed_calls,
            'total_tokens': total_tokens,
            'total_cost': total_cost,
            'avg_latency': avg_latency,
            'calls_today': logs.filter(created_at__gte=start_day).count(),
            'calls_week': logs.filter(created_at__gte=start_week).count(),
            'calls_month': logs.filter(created_at__gte=start_month).count(),
            'recent_logs': logs.order_by('-created_at')[:20],
        }


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
