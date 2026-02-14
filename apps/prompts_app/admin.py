from django.contrib import admin
from .models import Prompt, PromptTestRun


@admin.register(Prompt)
class PromptAdmin(admin.ModelAdmin):
    list_display = ('name', 'is_active', 'is_default', 'updated_at')
    list_filter = ('is_active', 'is_default')
    search_fields = ('name',)

@admin.register(PromptTestRun)
class PromptTestRunAdmin(admin.ModelAdmin):
    list_display = ('prompt', 'job', 'consultant', 'tokens_used', 'cost', 'created_at')
    list_filter = ('created_at',)
