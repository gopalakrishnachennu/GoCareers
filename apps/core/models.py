from django.db import models
from django.core.cache import cache

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
