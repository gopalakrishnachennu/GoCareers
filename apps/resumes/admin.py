from django.contrib import admin
from .models import ResumeDraft


@admin.register(ResumeDraft)
class ResumeDraftAdmin(admin.ModelAdmin):
    list_display = ('consultant', 'job', 'version', 'status', 'ats_score', 'tokens_used', 'created_by', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('consultant__user__username', 'job__title')
    readonly_fields = ('version', 'tokens_used', 'created_at')
