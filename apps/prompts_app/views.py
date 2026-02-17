from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.urls import reverse_lazy, reverse
from django.contrib import messages

from .models import Prompt
from .forms import PromptForm
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

