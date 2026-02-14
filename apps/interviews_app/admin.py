from django.contrib import admin
from .models import Interview


@admin.register(Interview)
class InterviewAdmin(admin.ModelAdmin):
    list_display = ('submission', 'consultant', 'job_title', 'company', 'round', 'status', 'scheduled_at')
    list_filter = ('status', 'round', 'scheduled_at')
    search_fields = ('job_title', 'company', 'consultant__user__username')
