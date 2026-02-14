from django.urls import path
from .views import InterviewListView, InterviewCreateView, InterviewUpdateView, InterviewCalendarView

urlpatterns = [
    path('', InterviewListView.as_view(), name='interview-list'),
    path('add/', InterviewCreateView.as_view(), name='interview-add'),
    path('<int:pk>/edit/', InterviewUpdateView.as_view(), name='interview-edit'),
    path('calendar/', InterviewCalendarView.as_view(), name='interview-calendar'),
]
