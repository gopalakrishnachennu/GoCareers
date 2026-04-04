from django.conf import settings
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
    video_link = models.URLField(
        blank=True,
        help_text=_("Zoom, Meet, Teams, or other meeting URL."),
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-scheduled_at']

    def __str__(self):
        return f"{self.job_title} @ {self.company} ({self.get_status_display()})"


class InterviewFeedback(models.Model):
    """Post-interview scorecard (Phase 3)."""

    class Recommendation(models.TextChoices):
        STRONG_YES = 'STRONG_YES', _('Strong yes')
        YES = 'YES', _('Yes')
        MAYBE = 'MAYBE', _('Maybe')
        NO = 'NO', _('No')
        STRONG_NO = 'STRONG_NO', _('Strong no')

    interview = models.ForeignKey(
        Interview,
        on_delete=models.CASCADE,
        related_name='feedbacks',
    )
    author = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name='interview_feedbacks',
    )
    overall_rating = models.PositiveSmallIntegerField(
        default=3,
        help_text=_("1–5 overall."),
    )
    technical_rating = models.PositiveSmallIntegerField(null=True, blank=True, help_text=_("1–5 optional."))
    communication_rating = models.PositiveSmallIntegerField(null=True, blank=True, help_text=_("1–5 optional."))
    strengths = models.TextField(blank=True)
    concerns = models.TextField(blank=True)
    recommendation = models.CharField(
        max_length=20,
        choices=Recommendation.choices,
        default=Recommendation.MAYBE,
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"Feedback on {self.interview_id} by {self.author_id}"
