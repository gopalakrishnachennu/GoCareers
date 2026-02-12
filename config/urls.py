from django.contrib import admin
from django.urls import path, include

from core.views import home, AdminDashboardView, EmployeeDashboardView
from config.impersonate_views import start_impersonate, stop_impersonate

urlpatterns = [
    path('admin/', admin.site.urls),
    path("__reload__/", include("django_browser_reload.urls")),
    path("accounts/", include("django.contrib.auth.urls")),
    path("jobs/", include("jobs.urls")),
    path("resumes/", include("resumes.urls")),
    path("submissions/", include("submissions.urls")),
    path("messages/", include("messaging.urls")),
    path("consultants/", include("users.urls")),
    path("employees/", include("users.urls_employees")),
    path("analytics/", include("analytics.urls")),
    path("core/", include("core.urls")),
    path("admin-dashboard/", AdminDashboardView.as_view(), name="admin-dashboard"),
    path("employee-dashboard/", EmployeeDashboardView.as_view(), name="employee-dashboard"),
    path("impersonate/<int:user_id>/", start_impersonate, name="start-impersonate"),
    path("impersonate/stop/", stop_impersonate, name="stop-impersonate"),
    path("", home, name="home"),
]
