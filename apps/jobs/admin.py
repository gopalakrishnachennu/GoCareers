from django.contrib import admin
from .models import Job

@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = ('title', 'company', 'location', 'posted_by', 'status', 'created_at')
    list_filter = ('status', 'job_type', 'created_at')
    search_fields = ('title', 'company', 'description')
    date_hierarchy = 'created_at'#
