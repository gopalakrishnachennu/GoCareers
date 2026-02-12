from django import forms
from .models import PlatformConfig

class PlatformConfigForm(forms.ModelForm):
    class Meta:
        model = PlatformConfig
        fields = '__all__'
        widgets = {
            'site_description': forms.Textarea(attrs={'rows': 3}),
            'meta_description': forms.Textarea(attrs={'rows': 3}),
            'address': forms.Textarea(attrs={'rows': 3}),
            'maintenance_message': forms.Textarea(attrs={'rows': 3}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Add basic styling
        for field in self.fields:
            if isinstance(self.fields[field].widget, (forms.TextInput, forms.URLInput, forms.EmailInput, forms.NumberInput, forms.Textarea)):
                 self.fields[field].widget.attrs.update({'class': 'w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500'})
            elif isinstance(self.fields[field].widget, forms.CheckboxInput):
                 self.fields[field].widget.attrs.update({'class': 'h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded'})
