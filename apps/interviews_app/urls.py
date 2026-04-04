from django.urls import path
from .views import (
    InterviewListView,
    InterviewExportCSVView,
    InterviewCreateView,
    InterviewUpdateView,
    InterviewDetailView,
    InterviewFeedbackCreateView,
    InterviewCalendarView,
    InterviewICSExportView,
    InterviewCalendarICSFeedView,
)

urlpatterns = [
    path('', InterviewListView.as_view(), name='interview-list'),
    path('export/', InterviewExportCSVView.as_view(), name='interview-export-csv'),
    path('calendar.ics', InterviewCalendarICSFeedView.as_view(), name='interview-ics-feed'),
    path('add/', InterviewCreateView.as_view(), name='interview-add'),
    path('<int:pk>/event.ics', InterviewICSExportView.as_view(), name='interview-ics'),
    path('<int:pk>/feedback/', InterviewFeedbackCreateView.as_view(), name='interview-feedback-add'),
    path('<int:pk>/', InterviewDetailView.as_view(), name='interview-detail'),
    path('<int:pk>/edit/', InterviewUpdateView.as_view(), name='interview-edit'),
    path('calendar/', InterviewCalendarView.as_view(), name='interview-calendar'),
]
