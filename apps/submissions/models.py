from django.db import models
from django.conf import settings
from jobs.models import Job
from resumes.models import Resume
from users.models import ConsultantProfile
from django.utils.translation import gettext_lazy as _

class ApplicationSubmission(models.Model):
    class Status(models.TextChoices):
        APPLIED = 'APPLIED', _('Applied')
        INTERVIEW = 'INTERVIEW', _('Interview Scheduled')
        OFFER = 'OFFER', _('Offer Received')
        REJECTED = 'REJECTED', _('Rejected')
        WITHDRAWN = 'WITHDRAWN', _('Withdrawn')

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='submissions')
    consultant = models.ForeignKey(ConsultantProfile, on_delete=models.CASCADE, related_name='submissions')
    resume = models.ForeignKey(Resume, on_delete=models.SET_NULL, null=True, blank=True, related_name='submissions')
    
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.APPLIED
    )
    
    submitted_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name='submitted_applications'
    )
    
    proof_file = models.ImageField(upload_to='submission_proofs/', blank=True, null=True, help_text="Upload screenshot or email confirmation")
    notes = models.TextField(blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.consultant.user.username} applied to {self.job.title}"
    
    class Meta:
        ordering = ['-created_at']
        unique_together = ('job', 'consultant')  # Prevent double applications? Maybe not if re-applying later.
