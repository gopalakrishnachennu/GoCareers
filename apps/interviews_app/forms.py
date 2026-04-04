from django import forms

from .models import InterviewFeedback


class InterviewFeedbackForm(forms.ModelForm):
    class Meta:
        model = InterviewFeedback
        fields = [
            'overall_rating',
            'technical_rating',
            'communication_rating',
            'strengths',
            'concerns',
            'recommendation',
        ]
        widgets = {
            'strengths': forms.Textarea(attrs={'rows': 3, 'class': 'w-full border border-gray-300 rounded-lg px-3 py-2 text-sm'}),
            'concerns': forms.Textarea(attrs={'rows': 3, 'class': 'w-full border border-gray-300 rounded-lg px-3 py-2 text-sm'}),
            'overall_rating': forms.NumberInput(attrs={'min': 1, 'max': 5, 'class': 'w-24 border border-gray-300 rounded-lg px-3 py-2 text-sm'}),
            'technical_rating': forms.NumberInput(attrs={'min': 1, 'max': 5, 'class': 'w-24 border border-gray-300 rounded-lg px-3 py-2 text-sm'}),
            'communication_rating': forms.NumberInput(attrs={'min': 1, 'max': 5, 'class': 'w-24 border border-gray-300 rounded-lg px-3 py-2 text-sm'}),
            'recommendation': forms.Select(attrs={'class': 'w-full max-w-md border border-gray-300 rounded-lg px-3 py-2 text-sm'}),
        }

    def clean_overall_rating(self):
        v = self.cleaned_data.get('overall_rating')
        if v is not None and (v < 1 or v > 5):
            raise forms.ValidationError('Use 1–5.')
        return v
