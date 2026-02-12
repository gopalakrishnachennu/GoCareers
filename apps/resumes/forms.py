from django import forms
from .models import ResumeDraft
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
