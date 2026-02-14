from django.db import models
from django.core.cache import cache
from django.conf import settings

from resumes.models import PromptTemplate
from users.models import User, ConsultantProfile
from jobs.models import Job

class PlatformConfig(models.Model):
    """
    Singleton model to store global platform configuration.
    """
    # Branding
    site_name = models.CharField(max_length=100, default="EduConsult")
    site_tagline = models.CharField(max_length=200, default="Connecting Experts with Opportunities", blank=True)
    logo_url = models.URLField(blank=True, help_text="External URL to logo image")

    # SEO
    meta_description = models.TextField(blank=True, help_text="Default meta description for SEO")
    meta_keywords = models.CharField(max_length=255, blank=True, help_text="Comma-separated keywords")

    # Contact
    contact_email = models.EmailField(default="support@educonsult.com")
    support_phone = models.CharField(max_length=20, blank=True)
    address = models.TextField(blank=True)

    # Feature Flags
    enable_consultant_registration = models.BooleanField(default=True, help_text="Allow new consultants to register")
    enable_job_applications = models.BooleanField(default=True, help_text="Allow consultants to apply for jobs")
    enable_public_consultant_view = models.BooleanField(default=True, help_text="Allow guests to view consultant profiles")

    # System
    maintenance_mode = models.BooleanField(default=False)
    maintenance_message = models.TextField(default="We are currently performing scheduled maintenance. Please check back later.")
    session_timeout_minutes = models.IntegerField(default=60, help_text="Session expiry time in minutes")
    max_upload_size_mb = models.IntegerField(default=5, help_text="Max file upload size in MB")

    # Social Media
    twitter_url = models.URLField(blank=True)
    linkedin_url = models.URLField(blank=True)
    github_url = models.URLField(blank=True)

    # Legal
    tos_url = models.URLField(blank=True, verbose_name="Terms of Service URL")
    privacy_policy_url = models.URLField(blank=True, verbose_name="Privacy Policy URL")

    def __str__(self):
        return "Platform Configuration"

    def save(self, *args, **kwargs):
        self.pk = 1  # Singleton: always ID 1
        super(PlatformConfig, self).save(*args, **kwargs)
        cache.delete('platform_config')  # Invalidate cache on save

    def delete(self, *args, **kwargs):
        pass  # Prevent deletion

    @classmethod
    def load(cls):
        """
        Load the singleton instance. Create if not exists.
        """
        if cache.get('platform_config') is None:
            obj, created = cls.objects.get_or_create(pk=1)
            cache.set('platform_config', obj)
        return cache.get('platform_config')


class AuditLog(models.Model):
    """
    Logs critical actions performed by users for compliance and tracking.
    """
    actor = models.ForeignKey('users.User', on_delete=models.SET_NULL, null=True, related_name='audit_logs')
    action = models.CharField(max_length=255)
    target_model = models.CharField(max_length=100, blank=True)
    target_id = models.CharField(max_length=100, blank=True)
    details = models.JSONField(default=dict, blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-timestamp']

    def __str__(self):
        return f"{self.actor} - {self.action} at {self.timestamp}"


class LLMConfig(models.Model):
    """
    Singleton model to store LLM configuration and API credentials.
    """
    encrypted_api_key = models.TextField(blank=True, help_text="Encrypted OpenAI API key")
    active_model = models.CharField(max_length=100, default="gpt-4o-mini")
    system_prompt = models.TextField(blank=True)
    prompt_template = models.ForeignKey(
        PromptTemplate, on_delete=models.SET_NULL, null=True, blank=True
    )
    active_prompt = models.ForeignKey(
        'prompts_app.Prompt', on_delete=models.SET_NULL, null=True, blank=True
    )
    temperature = models.DecimalField(max_digits=3, decimal_places=2, default=0.70)
    max_output_tokens = models.PositiveIntegerField(default=2000)

    monthly_token_cap = models.PositiveIntegerField(default=0, help_text="0 means no cap")
    generation_enabled = models.BooleanField(default=True)
    auto_disable_on_cap = models.BooleanField(default=True)

    data_pipelines_connected = models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return "LLM Configuration"

    def save(self, *args, **kwargs):
        is_new = self.pk is None
        self.pk = 1  # Singleton: always ID 1
        if not is_new:
            LLMConfigVersion.objects.create(
                config=self,
                active_model=self.active_model,
                system_prompt=self.system_prompt,
                prompt_template=self.prompt_template,
                temperature=self.temperature,
                max_output_tokens=self.max_output_tokens,
            )
        super().save(*args, **kwargs)
        cache.delete('llm_config')

    def delete(self, *args, **kwargs):
        pass

    @classmethod
    def load(cls):
        if cache.get('llm_config') is None:
            obj, _ = cls.objects.get_or_create(pk=1)
            cache.set('llm_config', obj)
        return cache.get('llm_config')


class LLMConfigVersion(models.Model):
    config = models.ForeignKey(LLMConfig, on_delete=models.CASCADE, related_name='versions')
    active_model = models.CharField(max_length=100)
    system_prompt = models.TextField(blank=True)
    prompt_template = models.ForeignKey(PromptTemplate, on_delete=models.SET_NULL, null=True, blank=True)
    temperature = models.DecimalField(max_digits=3, decimal_places=2)
    max_output_tokens = models.PositiveIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']


class LLMUsageLog(models.Model):
    request_type = models.CharField(max_length=50, default='resume_generation')
    model_name = models.CharField(max_length=100)
    prompt_tokens = models.PositiveIntegerField(default=0)
    completion_tokens = models.PositiveIntegerField(default=0)
    total_tokens = models.PositiveIntegerField(default=0)
    cost_input = models.DecimalField(max_digits=10, decimal_places=6, default=0)
    cost_output = models.DecimalField(max_digits=10, decimal_places=6, default=0)
    cost_total = models.DecimalField(max_digits=10, decimal_places=6, default=0)
    latency_ms = models.PositiveIntegerField(default=0)
    success = models.BooleanField(default=True)
    error_message = models.TextField(blank=True)

    job = models.ForeignKey(Job, on_delete=models.SET_NULL, null=True, blank=True)
    consultant = models.ForeignKey(ConsultantProfile, on_delete=models.SET_NULL, null=True, blank=True)
    actor = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']
