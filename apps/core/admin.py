from django.contrib import admin
from .models import PlatformConfig, LLMConfig, LLMConfigVersion, LLMUsageLog, AuditLog


@admin.register(PlatformConfig)
class PlatformConfigAdmin(admin.ModelAdmin):
    pass


@admin.register(LLMConfig)
class LLMConfigAdmin(admin.ModelAdmin):
    list_display = ('active_model', 'generation_enabled', 'monthly_token_cap', 'updated_at')


@admin.register(LLMConfigVersion)
class LLMConfigVersionAdmin(admin.ModelAdmin):
    list_display = ('active_model', 'created_at')


@admin.register(LLMUsageLog)
class LLMUsageLogAdmin(admin.ModelAdmin):
    list_display = ('model_name', 'total_tokens', 'cost_total', 'latency_ms', 'success', 'created_at')
    list_filter = ('model_name', 'success', 'created_at')


@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    list_display = ('actor', 'action', 'target_model', 'target_id', 'timestamp')
