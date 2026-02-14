from django.urls import path
from .views import SubmissionCreateView, SubmissionListView, SubmissionUpdateView, SubmissionClaimView, SubmissionDetailView

urlpatterns = [
    path('', SubmissionListView.as_view(), name='submission-list'),
    path('log/', SubmissionCreateView.as_view(), name='submission-create'),
    path('<int:pk>/update/', SubmissionUpdateView.as_view(), name='submission-update'),
    path('<int:pk>/', SubmissionDetailView.as_view(), name='submission-detail'),
    path('claim/<int:draft_id>/', SubmissionClaimView.as_view(), name='submission-claim'),
]
