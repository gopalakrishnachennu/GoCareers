from django import forms
import string
from .models import Prompt


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
            'is_active': forms.CheckboxInput(),
            'is_default': forms.CheckboxInput(),
        }

    def clean_template_text(self):
        template_text = self.cleaned_data.get('template_text', '')
        if not template_text:
            return template_text

        allowed = {
            'job_title', 'company', 'job_description',
            'consultant_name', 'consultant_bio', 'consultant_skills',
            'experience_summary', 'certifications',
            'base_resume_text', 'input_summary',
        }

        formatter = string.Formatter()
        field_names = []
        for _, field_name, _, _ in formatter.parse(template_text):
            if not field_name:
                continue
            base = field_name.split('.')[0].split('[')[0]
            field_names.append(base)

        unknown = sorted({name for name in field_names if name not in allowed})
        if unknown:
            raise forms.ValidationError(
                "Unknown placeholders: " + ", ".join(unknown)
            )
        return template_text
