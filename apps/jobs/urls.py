from django.urls import path
from .views import JobListView, JobDetailView, JobCreateView, JobUpdateView, JobDeleteView, JobBulkUploadView, JobDuplicateView

urlpatterns = [
    path('', JobListView.as_view(), name='job-list'),
    path('new/', JobCreateView.as_view(), name='job-create'),
    path('bulk-upload/', JobBulkUploadView.as_view(), name='job-bulk-upload'),
    path('<int:pk>/', JobDetailView.as_view(), name='job-detail'),
    path('<int:pk>/edit/', JobUpdateView.as_view(), name='job-update'),
    path('<int:pk>/duplicate/', JobDuplicateView.as_view(), name='job-duplicate'),
    path('<int:pk>/delete/', JobDeleteView.as_view(), name='job-delete'),
]
