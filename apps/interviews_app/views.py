import calendar
from datetime import date

from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, CreateView, UpdateView, TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.urls import reverse_lazy
from django.utils import timezone
from django import forms

from .models import Interview
from submissions.models import ApplicationSubmission
from users.models import User


class ConsultantOnlyMixin(LoginRequiredMixin, UserPassesTestMixin):
    def test_func(self):
        return self.request.user.role == User.Role.CONSULTANT


class InterviewForm(forms.ModelForm):
    class Meta:
        model = Interview
        fields = ['submission', 'job_title', 'company', 'location', 'round', 'scheduled_at', 'status', 'notes']
        widgets = {
            'scheduled_at': forms.DateTimeInput(attrs={'type': 'datetime-local'}),
        }

    def __init__(self, *args, **kwargs):
        consultant = kwargs.pop('consultant', None)
        super().__init__(*args, **kwargs)
        if consultant is not None:
            self.fields['submission'].queryset = ApplicationSubmission.objects.filter(consultant=consultant)


class InterviewListView(ConsultantOnlyMixin, ListView):
    model = Interview
    template_name = 'interviews/interview_list.html'
    context_object_name = 'interviews'

    def get_queryset(self):
        profile = self.request.user.consultant_profile
        return Interview.objects.filter(consultant=profile).select_related('submission')


class InterviewCreateView(ConsultantOnlyMixin, CreateView):
    model = Interview
    form_class = InterviewForm
    template_name = 'interviews/interview_form.html'
    success_url = reverse_lazy('interview-list')

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['consultant'] = self.request.user.consultant_profile
        return kwargs

    def form_valid(self, form):
        interview = form.save(commit=False)
        interview.consultant = self.request.user.consultant_profile
        if interview.submission:
            interview.job_title = interview.submission.job.title
            interview.company = interview.submission.job.company
        interview.save()
        return redirect(self.success_url)


class InterviewUpdateView(ConsultantOnlyMixin, UpdateView):
    model = Interview
    form_class = InterviewForm
    template_name = 'interviews/interview_form.html'
    success_url = reverse_lazy('interview-list')

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['consultant'] = self.request.user.consultant_profile
        return kwargs


class InterviewCalendarView(ConsultantOnlyMixin, TemplateView):
    template_name = 'interviews/interview_calendar.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        profile = self.request.user.consultant_profile

        month = int(self.request.GET.get('month', timezone.now().month))
        year = int(self.request.GET.get('year', timezone.now().year))

        cal = calendar.Calendar(firstweekday=0)
        weeks = []
        for week in cal.monthdatescalendar(year, month):
            week_days = []
            for day in week:
                day_interviews = Interview.objects.filter(
                    consultant=profile,
                    scheduled_at__date=day
                ).order_by('scheduled_at')
                week_days.append({'date': day, 'items': day_interviews})
            weeks.append(week_days)

        context['month'] = month
        context['year'] = year
        context['month_name'] = calendar.month_name[month]
        context['weeks'] = weeks
        context['today'] = date.today()
        return context
