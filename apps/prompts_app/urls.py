from django.urls import path
from .views import (
    PromptListView, PromptCreateView, PromptUpdateView, PromptDeleteView,
    prompt_detail,
)

urlpatterns = [
    path('', PromptListView.as_view(), name='prompt-list'),
    path('add/', PromptCreateView.as_view(), name='prompt-add'),
    path('<int:pk>/', prompt_detail, name='prompt-detail'),
    path('<int:pk>/edit/', PromptUpdateView.as_view(), name='prompt-edit'),
    path('<int:pk>/delete/', PromptDeleteView.as_view(), name='prompt-delete'),
]
