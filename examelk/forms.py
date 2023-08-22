from django.forms import ModelForm
from models import ElasticDetails, MangoDBDetail
from django import forms
from .models import UserProfile


class ElasticConfigForm(ModelForm):
    class Meta:
        model = ElasticDetails
        fields = "__all__"
        labels = {
            '': ('Exam name'),
            'clientname': ('Client name'),
            'no_of_examdays': ('No of examdays'),
            'no_of_examslot': ('No of examslot'),
            'no_of_regions': ('No of regions'),
            'no_of_centers': ('No of centers'),
            'exam_start_date': ('Exam start date'),
            'exam_end_date': ('Exam end date'),
            'mock_start_date': ('Mock start date'),
            'mock_end_date': ('Mock end date')
        }

    def clean(self):
        cleaned_data = super().clean()
        esdate = cleaned_data.get("exam_start_date")
        eedate = cleaned_data.get("exam_end_date")
        msdate = cleaned_data.get("mock_start_date")
        medate = cleaned_data.get("mock_end_date")
        if esdate > eedate:
            self._errors['exam_start_date'] = self.error_class([
                'Exam state date must be before Exam end date'])
        if msdate > medate:
            self._errors['mock_start_date'] = self.error_class([
                'Mock state date must be before Mock end date'])
        if medate >= esdate:
            self._errors['exam_start_date'] = self.error_class([
                'Mock end date must be before Exam start date'])
        return self.cleaned_data


