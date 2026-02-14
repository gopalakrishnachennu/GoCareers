from django.db import models
from django.utils.translation import gettext_lazy as _
from submissions.models import ApplicationSubmission
from users.models import ConsultantProfile


class Interview(models.Model):
    class Status(models.TextChoices):
        SCHEDULED = 'SCHEDULED', _('Scheduled')
        RESCHEDULED = 'RESCHEDULED', _('Rescheduled')
        COMPLETED = 'COMPLETED', _('Completed')
        CANCELLED = 'CANCELLED', _('Cancelled')

    class Round(models.TextChoices):
        SCREENING = 'SCREENING', _('Screening')
        TECHNICAL = 'TECHNICAL', _('Technical')
        MANAGERIAL = 'MANAGERIAL', _('Managerial')
        HR = 'HR', _('HR')
        OTHER = 'OTHER', _('Other')

    submission = models.ForeignKey(
        ApplicationSubmission, on_delete=models.CASCADE, related_name='interviews'
    )
    consultant = models.ForeignKey(
        ConsultantProfile, on_delete=models.CASCADE, related_name='interviews'
    )
    job_title = models.CharField(max_length=200)
    company = models.CharField(max_length=200)
    location = models.CharField(max_length=200, blank=True)
    round = models.CharField(max_length=20, choices=Round.choices, default=Round.SCREENING)
    scheduled_at = models.DateTimeField()
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.SCHEDULED)
    notes = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-scheduled_at']

    def __str__(self):
        return f"{self.job_title} @ {self.company} ({self.get_status_display()})"
