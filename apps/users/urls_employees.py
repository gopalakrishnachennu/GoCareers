from django.urls import path
from .views import EmployeeListView, EmployeeDetailView, EmployeeEditView, EmployeeCreateView

urlpatterns = [
    path('', EmployeeListView.as_view(), name='employee-list'),
    path('add/', EmployeeCreateView.as_view(), name='employee-add'),
    path('<int:pk>/', EmployeeDetailView.as_view(), name='employee-detail'),
    path('<int:pk>/edit/', EmployeeEditView.as_view(), name='employee-edit'),
]
