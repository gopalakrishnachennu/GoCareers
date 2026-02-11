from django.urls import path
from .views import ResumeCreateView, ResumeDetailView, ResumeDownloadView

urlpatterns = [
    path('new/', ResumeCreateView.as_view(), name='resume-create'),
    path('<int:pk>/', ResumeDetailView.as_view(), name='resume-detail'),
    path('<int:pk>/download/', ResumeDownloadView.as_view(), name='resume-download'),
]
