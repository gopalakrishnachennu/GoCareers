from django import forms
from .models import ResumeDraft, ResumeTemplate, ResumeTemplatePack
from users.models import ConsultantProfile
from jobs.models import Job


class DraftGenerateForm(forms.Form):
    """Simple form: select a job to generate a draft for. Consultant comes from URL."""
    job = forms.ModelChoiceField(
        queryset=Job.objects.filter(status='OPEN'),
        label="Select Job",
        empty_label="— Choose an open job —",
    )


# Legacy form kept for backward compat
class ResumeGenerationForm(forms.ModelForm):
    job = forms.ModelChoiceField(queryset=Job.objects.filter(status='OPEN'))
    consultant = forms.ModelChoiceField(queryset=ConsultantProfile.objects.all())

    class Meta:
        model = ResumeDraft
        fields = ['job', 'consultant']


class ResumeTemplateForm(forms.ModelForm):
    class Meta:
        model = ResumeTemplate
        fields = ['name', 'description', 'layout', 'is_active', 'marketing_roles']
        widgets = {
            'description': forms.Textarea(attrs={'rows': 3}),
            'layout': forms.Textarea(attrs={'rows': 12}),
        }


class ResumeTemplatePackForm(forms.ModelForm):
    class Meta:
        model = ResumeTemplatePack
        fields = ['name', 'description', 'is_active', 'marketing_roles', 'templates']
        widgets = {
            'description': forms.Textarea(attrs={'rows': 3}),
        }
