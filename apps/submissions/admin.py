from django.contrib import admin
from .models import ApplicationSubmission, SubmissionResponse

@admin.register(ApplicationSubmission)
class ApplicationSubmissionAdmin(admin.ModelAdmin):
    list_display = ('job', 'consultant', 'status', 'submitted_by', 'created_at')
    list_filter = ('status', 'created_at')


@admin.register(SubmissionResponse)
class SubmissionResponseAdmin(admin.ModelAdmin):
    list_display = ('submission', 'response_type', 'status', 'responded_at', 'created_by')
    list_filter = ('status', 'response_type', 'responded_at')
