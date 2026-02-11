from django import forms
from .models import Resume
from users.models import ConsultantProfile
from jobs.models import Job

class ResumeGenerationForm(forms.ModelForm):
    job = forms.ModelChoiceField(queryset=Job.objects.filter(status='OPEN'))
    consultant = forms.ModelChoiceField(queryset=ConsultantProfile.objects.all())

    class Meta:
        model = Resume
        fields = ['job', 'consultant']
