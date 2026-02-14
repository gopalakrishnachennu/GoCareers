from django.shortcuts import render, redirect, get_object_or_404
from django.views.generic import ListView, DetailView, CreateView, UpdateView, DeleteView, View
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.urls import reverse_lazy
from django.contrib import messages
from django.db.models import Q
import csv
import io
from .models import Job
from .forms import JobForm, JobBulkUploadForm
from users.models import User, MarketingRole

class EmployeeRequiredMixin(UserPassesTestMixin):
    def test_func(self):
        return self.request.user.role == User.Role.EMPLOYEE or self.request.user.is_superuser

class JobListView(LoginRequiredMixin, ListView):
    model = Job
    template_name = 'jobs/job_list.html'
    context_object_name = 'jobs'
    paginate_by = 10

    def get_queryset(self):
        qs = super().get_queryset()
        status = self.request.GET.get('status')
        search_query = self.request.GET.get('search')
        role_filter = self.request.GET.get('role')
        
        if status and status in dict(Job.Status.choices):
            qs = qs.filter(status=status)
        
        if search_query:
            qs = qs.filter(
                Q(title__icontains=search_query) | 
                Q(company__icontains=search_query) |
                Q(description__icontains=search_query)
            )
        if role_filter:
            qs = qs.filter(marketing_role__slug=role_filter)
        return qs

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['marketing_roles'] = MarketingRole.objects.all()
        context['selected_role'] = self.request.GET.get('role', '')
        return context

    def get_template_names(self):
        if self.request.headers.get('HX-Request'):
            return ['jobs/_job_list_partial.html']
        return super().get_template_names()

class JobDetailView(LoginRequiredMixin, DetailView):
    model = Job
    template_name = 'jobs/job_detail.html'
    context_object_name = 'job'

class JobCreateView(LoginRequiredMixin, EmployeeRequiredMixin, CreateView):
    model = Job
    form_class = JobForm
    template_name = 'jobs/job_form.html'
    success_url = reverse_lazy('job-list')

    def form_valid(self, form):
        form.instance.posted_by = self.request.user
        messages.success(self.request, "Job posted successfully!")
        return super().form_valid(form)

class JobUpdateView(LoginRequiredMixin, EmployeeRequiredMixin, UpdateView):
    model = Job
    form_class = JobForm
    template_name = 'jobs/job_form.html'
    
    def get_queryset(self):
        qs = super().get_queryset()
        if self.request.user.is_superuser:
            return qs
        return qs.filter(posted_by=self.request.user)

    def get_success_url(self):
        messages.success(self.request, "Job updated successfully!")
        return reverse_lazy('job-detail', kwargs={'pk': self.object.pk})

class JobDeleteView(LoginRequiredMixin, EmployeeRequiredMixin, DeleteView):
    model = Job
    template_name = 'jobs/job_confirm_delete.html'
    success_url = reverse_lazy('job-list')

    def get_queryset(self):
        qs = super().get_queryset()
        if self.request.user.is_superuser:
            return qs
        return qs.filter(posted_by=self.request.user)
    
    def delete(self, request, *args, **kwargs):
        messages.success(self.request, "Job deleted successfully!")
        return super().delete(request, *args, **kwargs)

from django.db import transaction
import logging

logger = logging.getLogger(__name__)

from .services import JobService

class JobDuplicateView(LoginRequiredMixin, EmployeeRequiredMixin, View):
    def post(self, request, pk):
        job = JobService.clone_job(pk, request.user)
        if job:
            messages.success(request, f"Job duplicated successfully as '{job.title}'")
            return redirect('job-update', pk=job.pk)
        else:
            messages.error(request, "Failed to duplicate job.")
            return redirect('job-list')

class JobBulkUploadView(LoginRequiredMixin, EmployeeRequiredMixin, View):
    def get(self, request):
        form = JobBulkUploadForm()
        return render(request, 'jobs/job_bulk_upload.html', {'form': form})

    def post(self, request):
        form = JobBulkUploadForm(request.POST, request.FILES)
        if form.is_valid():
            csv_file = request.FILES['csv_file']
            
            # 1. File validation
            if not csv_file.name.endswith('.csv'):
                messages.error(request, "Please upload a CSV file.")
                return render(request, 'jobs/job_bulk_upload.html', {'form': form})
            
            if csv_file.multiple_chunks():
                 messages.error(request, "Uploaded file is too large (%.2f MB)." % (csv_file.size / (1000 * 1000),))
                 return render(request, 'jobs/job_bulk_upload.html', {'form': form})

            try:
                decoded_file = csv_file.read().decode('utf-8')
            except UnicodeDecodeError:
                messages.error(request, "File encoding error. Please ensure the file is UTF-8 encoded.")
                return render(request, 'jobs/job_bulk_upload.html', {'form': form})

            io_string = io.StringIO(decoded_file)
            reader = csv.DictReader(io_string)
            
            # 2. Header validation
            required_headers = {'title', 'company', 'location', 'description'}
            if not reader.fieldnames or not required_headers.issubset(set(reader.fieldnames)):
                messages.error(request, f"Missing required columns. Found: {reader.fieldnames}. Required: {required_headers}")
                return render(request, 'jobs/job_bulk_upload.html', {'form': form})

            jobs_created = 0
            errors = []
            
            try:
                with transaction.atomic():
                    for i, row in enumerate(reader, start=1):
                        title = row.get('title', '').strip()
                        company = row.get('company', '').strip()
                        
                        if not title or not company:
                            errors.append(f"Row {i}: Missing title or company.")
                            continue
                            
                        Job.objects.create(
                            title=title,
                            company=company,
                            location=row.get('location', '').strip(),
                            description=row.get('description', '').strip(),
                            requirements=row.get('requirements', ''),
                            salary_range=row.get('salary_range', ''),
                            posted_by=request.user,
                            status='OPEN' # Default to OPEN
                        )
                        jobs_created += 1
                    
                    if errors:
                        # deciding whether to rollback or partial success. 
                        # For bulk, usually all or nothing is safer, or at least warn.
                        # user didn't specify. Let's rollback if ANY error for safety? 
                        # Or maybe just report errors. Let's report errors but keep successes for now 
                        # unless it's critical. Actually, `transaction.atomic` wraps the block. 
                        # If we don't raise exception, it commits.
                        # User wants robustness. Let's valid rows go through but warn about others?
                        pass 

            except Exception as e:
                logger.error(f"Bulk upload error: {e}")
                messages.error(request, "An unexpected error occurred during processing.")
                return render(request, 'jobs/job_bulk_upload.html', {'form': form})

            if jobs_created > 0:
                messages.success(request, f"Successfully uploaded {jobs_created} jobs!")
            
            if errors:
                messages.warning(request, f"Some rows were skipped: {'; '.join(errors[:5])}...")

            return redirect('job-list')
        
        return render(request, 'jobs/job_bulk_upload.html', {'form': form})
