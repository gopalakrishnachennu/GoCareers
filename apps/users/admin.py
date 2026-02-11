from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import User, ConsultantProfile, EmployeeProfile, Experience, Education, Certification, Department

@admin.register(User)
class CustomUserAdmin(UserAdmin):
    fieldsets = UserAdmin.fieldsets + (
        (None, {'fields': ('role', 'avatar')}),
    )
    add_fieldsets = UserAdmin.add_fieldsets + (
        (None, {'fields': ('role', 'avatar')}),
    )
    list_display = ('username', 'email', 'role', 'is_staff')
    list_filter = ('role', 'is_staff', 'is_active')

class ExperienceInline(admin.StackedInline):
    model = Experience
    extra = 0

class EducationInline(admin.StackedInline):
    model = Education
    extra = 0
    
class CertificationInline(admin.StackedInline):
    model = Certification
    extra = 0

@admin.register(ConsultantProfile)
class ConsultantProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'hourly_rate')
    inlines = [ExperienceInline, EducationInline, CertificationInline]

@admin.register(EmployeeProfile)
class EmployeeProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'department', 'company_name')

@admin.register(Department)
class DepartmentAdmin(admin.ModelAdmin):
    list_display = ('name', 'created_at')
    search_fields = ('name',)
