from django.contrib import admin
from .models import PromptTemplate, Resume

@admin.register(PromptTemplate)
class PromptTemplateAdmin(admin.ModelAdmin):
    list_display = ('name', 'is_active', 'created_at')

@admin.register(Resume)
class ResumeAdmin(admin.ModelAdmin):
    list_display = ('consultant', 'job', 'created_at')
    list_filter = ('created_at',)
