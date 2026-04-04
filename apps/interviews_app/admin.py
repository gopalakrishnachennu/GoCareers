from django.contrib import admin
from .models import Interview, InterviewFeedback


@admin.register(Interview)
class InterviewAdmin(admin.ModelAdmin):
    list_display = ('submission', 'consultant', 'job_title', 'company', 'round', 'status', 'scheduled_at')
    list_filter = ('status', 'round', 'scheduled_at')
    search_fields = ('job_title', 'company', 'consultant__user__username')


@admin.register(InterviewFeedback)
class InterviewFeedbackAdmin(admin.ModelAdmin):
    list_display = ('interview', 'author', 'overall_rating', 'recommendation', 'created_at')
    list_filter = ('recommendation',)
