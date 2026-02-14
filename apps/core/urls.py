from django.urls import path
from .views import home, PlatformConfigView, SystemStatusView, LLMConfigView

urlpatterns = [
    path('', home, name='home'),
    path('setup/', PlatformConfigView.as_view(), name='platform-config'),
    path('status/', SystemStatusView.as_view(), name='system-status'),
    path('llm/', LLMConfigView.as_view(), name='llm-config'),
]
