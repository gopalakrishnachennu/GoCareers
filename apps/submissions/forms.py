from django import forms
from .models import ApplicationSubmission, SubmissionResponse

class ApplicationSubmissionForm(forms.ModelForm):
    class Meta:
        model = ApplicationSubmission
        fields = ['job', 'consultant', 'resume', 'status', 'proof_file', 'notes']
        widgets = {
            'job': forms.HiddenInput(),
            'consultant': forms.HiddenInput(),
            'resume': forms.HiddenInput(),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Make status optional for initial submission, defaults to model default
        self.fields['status'].required = False


class SubmissionResponseForm(forms.ModelForm):
    class Meta:
        model = SubmissionResponse
        fields = ['response_type', 'status', 'responded_at', 'notes']
        widgets = {
            'responded_at': forms.DateTimeInput(attrs={'type': 'datetime-local'}),
        }
