from django.db import models
from django.conf import settings
from users.models import ConsultantProfile
from jobs.models import Job


class Prompt(models.Model):
    name = models.CharField(max_length=200)
    description = models.TextField(blank=True)
    system_text = models.TextField(blank=True, default="")
    template_text = models.TextField(blank=True, default="")
    temperature = models.DecimalField(max_digits=3, decimal_places=2, default=0.70)
    max_output_tokens = models.PositiveIntegerField(default=2000)
    is_active = models.BooleanField(default=True)
    is_default = models.BooleanField(default=False)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-updated_at']

    def __str__(self):
        return self.name


class PromptTestRun(models.Model):
    prompt = models.ForeignKey(Prompt, on_delete=models.CASCADE, related_name='test_runs', null=True, blank=True)
    job = models.ForeignKey(Job, on_delete=models.SET_NULL, null=True, blank=True)
    consultant = models.ForeignKey(ConsultantProfile, on_delete=models.SET_NULL, null=True, blank=True)
    rendered_prompt = models.TextField(blank=True)
    output_preview = models.TextField(blank=True)
    tokens_used = models.PositiveIntegerField(default=0)
    cost = models.DecimalField(max_digits=10, decimal_places=6, default=0)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']
