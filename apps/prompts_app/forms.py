from django import forms
from .models import Prompt
from jobs.models import Job
from users.models import ConsultantProfile


class PromptForm(forms.ModelForm):
    class Meta:
        model = Prompt
        fields = [
            'name', 'description', 'system_text', 'template_text',
            'temperature', 'max_output_tokens', 'is_active', 'is_default'
        ]
        widgets = {
            'system_text': forms.Textarea(attrs={'rows': 4}),
            'template_text': forms.Textarea(attrs={'rows': 8}),
        }

    def clean_template_text(self):
        template_text = self.cleaned_data.get('template_text', '')
        required = [
            '{job_title}', '{company}', '{job_description}',
            '{consultant_name}', '{consultant_bio}', '{consultant_skills}',
            '{experience_summary}', '{certifications}',
        ]
        missing = [r for r in required if r not in template_text]
        if missing:
            raise forms.ValidationError(
                "Template missing placeholders: " + ", ".join(missing)
            )
        return template_text


class PromptTestForm(forms.Form):
    prompt = forms.ModelChoiceField(queryset=Prompt.objects.all())
    job = forms.ModelChoiceField(queryset=Job.objects.all())
    consultant = forms.ModelChoiceField(queryset=ConsultantProfile.objects.all())
