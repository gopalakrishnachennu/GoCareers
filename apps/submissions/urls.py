from django.urls import path
from .views import SubmissionCreateView, SubmissionListView, SubmissionUpdateView

urlpatterns = [
    path('', SubmissionListView.as_view(), name='submission-list'),
    path('log/', SubmissionCreateView.as_view(), name='submission-create'),
    path('<int:pk>/update/', SubmissionUpdateView.as_view(), name='submission-update'),
]
