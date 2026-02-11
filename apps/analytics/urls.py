from django.urls import path
from .views import AnalyticsDashboardView

urlpatterns = [
    path('', AnalyticsDashboardView.as_view(), name='analytics-dashboard'),
]
