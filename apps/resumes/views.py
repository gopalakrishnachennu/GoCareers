from django.urls import reverse_lazy
from django.views.generic import CreateView, DetailView, View
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib import messages
from .models import Resume
from .forms import ResumeGenerationForm
from .services import LLMService

class ResumeCreateView(LoginRequiredMixin, CreateView):
    model = Resume
    form_class = ResumeGenerationForm
    template_name = 'resumes/resume_form.html'

    def form_valid(self, form):
        job = form.cleaned_data['job']
        consultant = form.cleaned_data['consultant']
        
        # 1. Job Status Validation
        if job.status != 'OPEN':
            messages.error(self.request, "Cannot generate resume for a closed or draft job.")
            return self.form_invalid(form)

        # 2. Permission Check
        if self.request.user.role == 'CONSULTANT' and consultant != self.request.user:
             messages.error(self.request, "You can only generate resumes for yourself.")
             return self.form_invalid(form)

        # 3. Duplicate Check
        existing_resume = Resume.objects.filter(job=job, consultant=consultant).first()
        if existing_resume:
            messages.info(self.request, "A resume for this job already exists.")
            return HttpResponseRedirect(reverse_lazy('resume-detail', kwargs={'pk': existing_resume.pk}))
        
        # Generate resume content using LLMService
        try:
            llm_service = LLMService()
            generated_content = llm_service.generate_resume_content(job, consultant)
            
            if not generated_content or generated_content.startswith("Error"):
                messages.error(self.request, f"Failed to generate resume: {generated_content}")
                return self.form_invalid(form)
                
            form.instance.generated_content = generated_content
            messages.success(self.request, "Resume generated successfully!")
            return super().form_valid(form)
        except Exception as e:
            messages.error(self.request, f"An error occurred during generation: {e}")
            return self.form_invalid(form)

    def get_success_url(self):
        return reverse_lazy('resume-detail', kwargs={'pk': self.object.pk})

class ResumeDetailView(LoginRequiredMixin, DetailView):
    model = Resume
    template_name = 'resumes/resume_detail.html'
    context_object_name = 'resume'

class ResumeDownloadView(LoginRequiredMixin, View):
    def get(self, request, pk):
        resume = get_object_or_404(Resume, pk=pk)
        
        # Create DOCX on th fly (or retrieve if saved)
        # For now, we generate on the fly using DocxService
        from .services import DocxService
        docx_service = DocxService()
        buffer = docx_service.create_docx(resume.generated_content)
        
        response = HttpResponse(
            buffer.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )
        response['Content-Disposition'] = f'attachment; filename=resume_{resume.id}.docx'
        return response
