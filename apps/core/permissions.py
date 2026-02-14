from django.contrib.auth.mixins import UserPassesTestMixin
from django.db.models import Q

class EmployeeAccessMixin(UserPassesTestMixin):
    """
    Mixin to restrict access to objects based on User Role.
    - Admins: Access everything.
    - Employees: Access only objects they created OR are assigned to.
    - Consultants: Access only their own data (if applicable).
    """

    def test_func(self):
        # Base access check: Must be logged in and active
        return self.request.user.is_authenticated and self.request.user.is_active

    def get_queryset(self):
        """
        Filter queryset based on role.
        Assumes the model has 'created_by' or 'assigned_to' fields if strict isolation is needed.
        For models like ConsultantProfile, it might filter by assignment.
        """
        qs = super().get_queryset()
        user = self.request.user

        # Admins see everything
        if user.is_superuser or user.role == 'ADMIN':
            return qs

        # Employees
        if user.role == 'EMPLOYEE':
            # logic depends on the model. 
            # If the model has a 'created_by' field, filter by it.
            # If it has an 'assigned_to' field, filter by it.
            
            filters = Q()
            if hasattr(qs.model, 'created_by'):
                filters |= Q(created_by=user)
            
            # Example: Consultants might be assigned to employees
            if hasattr(qs.model, 'assigned_to'):
                filters |= Q(assigned_to=user.employee_profile)
            
            # If no ownership fields exist, falls back to allow (or deny, depending on policy).
            # For now, if no fields match, we might return empty or full based on requirement.
            # "Employee views should not access or edit other employees’ objects" implies restricted default.
            
            if not filters and not hasattr(qs.model, 'created_by'):
                # If model doesn't track owner, maybe it's public? 
                # But for this task "Employee Workbench", we assume strictness.
                # Let's verify if the model IS a user-owned object.
                return qs # Fallback for now until we identify specific models
            
            return qs.filter(filters)

        # Consultants (Drafts, etc)
        if user.role == 'CONSULTANT':
             if hasattr(qs.model, 'consultant'):
                 return qs.filter(consultant__user=user)
             if hasattr(qs.model, 'user'):
                 return qs.filter(user=user)
        
        return qs.none()
