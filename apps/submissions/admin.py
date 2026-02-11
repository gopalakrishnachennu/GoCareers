from django.contrib import admin
from .models import ApplicationSubmission

@admin.register(ApplicationSubmission)
class ApplicationSubmissionAdmin(admin.ModelAdmin):
    list_display = ('job', 'consultant', 'status', 'submitted_by', 'created_at')
    list_filter = ('status', 'created_at')
