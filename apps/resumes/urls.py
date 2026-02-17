from django.urls import path
from .views import (
    ResumeCreateView, ResumeDetailView, ResumeDownloadView,
    DraftDetailView, DraftDownloadView, DraftPromoteView, DraftDeleteView,
    DraftSetPromptView, DraftRegenerateView, LLMInputPreferenceSaveView,
    ResumeTemplateListView, ResumeTemplateCreateView, ResumeTemplateUpdateView,
    ResumeTemplatePackListView, ResumeTemplatePackCreateView, ResumeTemplatePackUpdateView,
    DraftRegenerateSectionView,
)

urlpatterns = [
    # Legacy resume URLs
    path('new/', ResumeCreateView.as_view(), name='resume-create'),

    # Draft URLs
    path('drafts/<int:pk>/', DraftDetailView.as_view(), name='draft-detail'),
    path('drafts/<int:pk>/set-prompt/', DraftSetPromptView.as_view(), name='draft-set-prompt'),
    path('drafts/<int:pk>/regenerate/', DraftRegenerateView.as_view(), name='draft-regenerate'),
    path('drafts/<int:pk>/regenerate-section/', DraftRegenerateSectionView.as_view(), name='draft-regenerate-section'),
    path('drafts/<int:pk>/save-input-defaults/', LLMInputPreferenceSaveView.as_view(), name='llm-input-defaults'),
    path('drafts/<int:pk>/download/', DraftDownloadView.as_view(), name='draft-download'),
    path('drafts/<int:pk>/promote/', DraftPromoteView.as_view(), name='draft-promote'),
    path('drafts/<int:pk>/delete/', DraftDeleteView.as_view(), name='draft-delete'),

    # Templates
    path('templates/', ResumeTemplateListView.as_view(), name='resume-template-list'),
    path('templates/new/', ResumeTemplateCreateView.as_view(), name='resume-template-create'),
    path('templates/<int:pk>/edit/', ResumeTemplateUpdateView.as_view(), name='resume-template-edit'),
    path('template-packs/', ResumeTemplatePackListView.as_view(), name='resume-template-pack-list'),
    path('template-packs/new/', ResumeTemplatePackCreateView.as_view(), name='resume-template-pack-create'),
    path('template-packs/<int:pk>/edit/', ResumeTemplatePackUpdateView.as_view(), name='resume-template-pack-edit'),

    # Legacy
    path('<int:pk>/', ResumeDetailView.as_view(), name='resume-detail'),
    path('<int:pk>/download/', ResumeDownloadView.as_view(), name='resume-download'),
]
