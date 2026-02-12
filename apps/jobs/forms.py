from django import forms
from .models import Job

class JobForm(forms.ModelForm):
    class Meta:
        model = Job
        fields = ['title', 'company', 'location', 'description', 'original_link', 'salary_range', 'job_type', 'status', 'marketing_roles']
        widgets = {
            'marketing_roles': forms.CheckboxSelectMultiple(),
        }

class JobBulkUploadForm(forms.Form):
    csv_file = forms.FileField(label="Upload CSV File", help_text="Upload a CSV file with columns: title, company, location, description, requirements, salary_range")
