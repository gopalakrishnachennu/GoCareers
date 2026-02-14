from django.db import models
from django.conf import settings
from django.core.validators import FileExtensionValidator
from jobs.models import Job
from resumes.models import Resume
from users.models import ConsultantProfile
from django.utils.translation import gettext_lazy as _
from config.constants.limits import ALLOWED_UPLOAD_EXTENSIONS

class ApplicationSubmission(models.Model):
    class Status(models.TextChoices):
        IN_PROGRESS = 'IN_PROGRESS', _('In Progress')
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
    
    proof_file = models.FileField(
        upload_to='submission_proofs/',
        blank=True,
        null=True,
        help_text="Upload screenshot, image, or PDF confirmation",
        validators=[FileExtensionValidator(allowed_extensions=ALLOWED_UPLOAD_EXTENSIONS)],
    )
    notes = models.TextField(blank=True)
    submitted_at = models.DateTimeField(blank=True, null=True, help_text="When proof of submission was uploaded")
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.consultant.user.username} applied to {self.job.title}"
    
    class Meta:
        ordering = ['-created_at']
        unique_together = ('job', 'consultant')  # Prevent double applications? Maybe not if re-applying later.


class SubmissionResponse(models.Model):
    class ResponseType(models.TextChoices):
        EMAIL = 'EMAIL', _('Email')
        CALL = 'CALL', _('Call')
        PORTAL = 'PORTAL', _('Portal')
        OTHER = 'OTHER', _('Other')

    class Status(models.TextChoices):
        RECEIVED = 'RECEIVED', _('Received')
        FOLLOW_UP = 'FOLLOW_UP', _('Follow Up')
        CLOSED = 'CLOSED', _('Closed')

    submission = models.ForeignKey(
        ApplicationSubmission, on_delete=models.CASCADE, related_name='responses'
    )
    response_type = models.CharField(max_length=20, choices=ResponseType.choices, default=ResponseType.EMAIL)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.RECEIVED)
    notes = models.TextField(blank=True)
    responded_at = models.DateTimeField()
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name='submission_responses'
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-responded_at']
