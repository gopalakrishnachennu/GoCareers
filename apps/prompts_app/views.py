from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.urls import reverse_lazy, reverse
from django.contrib import messages

from .models import Prompt
from .forms import PromptForm, PromptTestForm
from .models import PromptTestRun
from resumes.services import LLMService
from users.models import User


class AdminRequiredMixin(LoginRequiredMixin, UserPassesTestMixin):
    def test_func(self):
        return self.request.user.is_superuser or self.request.user.role == User.Role.ADMIN


class PromptListView(AdminRequiredMixin, ListView):
    model = Prompt
    template_name = 'prompts/prompt_list.html'
    context_object_name = 'prompts'

    def get_queryset(self):
        qs = super().get_queryset()
        q = self.request.GET.get('q')
        if q:
            qs = qs.filter(name__icontains=q)
        return qs


class PromptCreateView(AdminRequiredMixin, CreateView):
    model = Prompt
    form_class = PromptForm
    template_name = 'prompts/prompt_form.html'
    success_url = reverse_lazy('prompt-list')

    def form_valid(self, form):
        form.instance.created_by = self.request.user
        messages.success(self.request, 'Prompt created.')
        return super().form_valid(form)

    def get_success_url(self):
        return reverse('prompt-list')


class PromptUpdateView(AdminRequiredMixin, UpdateView):
    model = Prompt
    form_class = PromptForm
    template_name = 'prompts/prompt_form.html'

    def get_success_url(self):
        messages.success(self.request, 'Prompt updated.')
        return reverse('prompt-list')


class PromptDeleteView(AdminRequiredMixin, DeleteView):
    model = Prompt
    success_url = reverse_lazy('prompt-list')
    template_name = 'prompts/prompt_confirm_delete.html'

    def delete(self, request, *args, **kwargs):
        messages.success(request, 'Prompt deleted.')
        return super().delete(request, *args, **kwargs)


def prompt_detail(request, pk):
    if not (request.user.is_superuser or request.user.role == User.Role.ADMIN):
        return redirect('home')
    prompt = get_object_or_404(Prompt, pk=pk)
    return render(request, 'prompts/prompt_detail.html', {'prompt': prompt})


def prompt_test(request):
    if not (request.user.is_superuser or request.user.role == User.Role.ADMIN):
        return redirect('home')

    test_run = None
    if request.method == 'POST':
        form = PromptTestForm(request.POST)
        if form.is_valid():
            prompt = form.cleaned_data['prompt']
            job = form.cleaned_data['job']
            consultant = form.cleaned_data['consultant']

            # Render prompt
            rendered = prompt.template_text.format(
                job_title=job.title,
                company=job.company,
                job_description=job.description,
                consultant_name=consultant.user.get_full_name() or consultant.user.username,
                consultant_bio=consultant.bio or "Not provided.",
                consultant_skills=", ".join(consultant.skills) if consultant.skills else "Not provided.",
                experience_summary="",
                certifications="",
            )

            # Generate preview (use current LLM settings)
            llm = LLMService()
            output, tokens, error = llm.generate_resume_content(job, consultant, actor=request.user)
            preview = output if output else (error or "No output")

            test_run = PromptTestRun.objects.create(
                prompt=prompt,
                job=job,
                consultant=consultant,
                rendered_prompt=rendered,
                output_preview=preview,
                tokens_used=tokens or 0,
                cost=0,
                created_by=request.user,
            )
    else:
        form = PromptTestForm()

    return render(request, 'prompts/prompt_test.html', {'form': form, 'test_run': test_run})
