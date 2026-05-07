from django.contrib import admin
from django.db.models import Count

from .models import CompanyPlatformLabel, JobBoardPlatform, LocationCache, RawJob, PlatformEngineConfig


@admin.register(JobBoardPlatform)
class JobBoardPlatformAdmin(admin.ModelAdmin):
    list_display = ["name", "slug", "api_type", "company_count", "is_enabled", "last_harvested_at"]
    list_filter = ["api_type", "is_enabled"]
    search_fields = ["name", "slug"]
    prepopulated_fields = {"slug": ("name",)}

    def get_queryset(self, request):
        return super().get_queryset(request).annotate(_company_count=Count("labels"))

    @admin.display(description="Companies", ordering="_company_count")
    def company_count(self, obj):
        return obj._company_count


@admin.register(CompanyPlatformLabel)
class CompanyPlatformLabelAdmin(admin.ModelAdmin):
    list_display = ["company", "platform", "confidence", "detection_method", "is_verified", "last_checked_at"]
    list_filter = ["platform", "confidence", "detection_method", "is_verified"]
    search_fields = ["company__name"]
    raw_id_fields = ["company"]
    readonly_fields = ["detected_at", "last_checked_at", "verified_at"]


@admin.register(RawJob)
class RawJobAdmin(admin.ModelAdmin):
    list_display = [
        "title", "company_name", "platform_slug",
        "country_code", "scope_status", "is_priority",
        "job_domain", "job_category",
        "sync_status", "employment_type", "fetched_at", "is_active",
    ]
    list_filter = [
        "platform_slug", "sync_status", "employment_type",
        "job_domain", "country_code", "scope_status", "is_priority", "is_active",
    ]
    search_fields = ["title", "company_name", "url_hash", "job_domain", "location_raw"]
    raw_id_fields = ["company"]
    readonly_fields = ["url_hash", "fetched_at", "updated_at", "job_domain", "domain_version", "last_scope_evaluated_at"]


@admin.register(LocationCache)
class LocationCacheAdmin(admin.ModelAdmin):
    list_display = [
        "normalized_text", "country_code", "region_code", "city",
        "confidence", "source", "provider", "status", "looked_up_at",
    ]
    list_filter = ["country_code", "source", "provider", "status"]
    search_fields = ["raw_text", "normalized_text", "city", "region_name", "country_name"]
    readonly_fields = ["created_at", "looked_up_at"]


@admin.register(PlatformEngineConfig)
class PlatformEngineConfigAdmin(admin.ModelAdmin):
    list_display = ["platform", "auto_backfill", "backfill_priority", "fetch_cadence_hours", "inter_request_delay_ms", "is_active"]
    list_filter = ["auto_backfill", "is_active"]
