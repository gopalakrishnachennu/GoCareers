from django.contrib import admin
from .models import PromptTemplate, ResumeDraft


@admin.register(PromptTemplate)
class PromptTemplateAdmin(admin.ModelAdmin):
    list_display = ('name', 'is_active', 'created_at')


@admin.register(ResumeDraft)
class ResumeDraftAdmin(admin.ModelAdmin):
    list_display = ('consultant', 'job', 'version', 'status', 'tokens_used', 'created_by', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('consultant__user__username', 'job__title')
    readonly_fields = ('version', 'tokens_used', 'created_at')
