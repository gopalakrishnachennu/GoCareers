import json
from django.db.models.signals import post_migrate
from django.dispatch import receiver

from .models import ResumeTemplate


DEFAULT_TEMPLATES = [
    {
        "name": "Standard ATS",
        "description": "Clean ATS layout with summary, skills, experience, education.",
        "layout": {
            "sections": [
                {"type": "header", "fixed": True},
                {"type": "summary", "ai": True},
                {"type": "skills", "ai": True},
                {"type": "experience", "fixed": ["title", "company", "dates"], "ai": ["responsibilities"]},
                {"type": "education", "fixed": True},
            ]
        },
    },
    {
        "name": "Experience First",
        "description": "Experience-focused layout; summary is short.",
        "layout": {
            "sections": [
                {"type": "header", "fixed": True},
                {"type": "experience", "fixed": ["title", "company", "dates"], "ai": ["responsibilities"]},
                {"type": "skills", "ai": True},
                {"type": "summary", "ai": True},
                {"type": "education", "fixed": True},
            ]
        },
    },
]


@receiver(post_migrate)
def create_default_templates(sender, **kwargs):
    if sender.name != 'resumes':
        return
    for t in DEFAULT_TEMPLATES:
        ResumeTemplate.objects.get_or_create(
            name=t["name"],
            defaults={
                "description": t["description"],
                "layout": t["layout"],
                "is_active": True,
            },
        )

    # Create a default pack that includes all templates
    from .models import ResumeTemplatePack
    pack, _ = ResumeTemplatePack.objects.get_or_create(
        name="Default Pack",
        defaults={
            "description": "All active templates (fallback pack).",
            "is_active": True,
        },
    )
    pack.templates.set(ResumeTemplate.objects.filter(is_active=True))
