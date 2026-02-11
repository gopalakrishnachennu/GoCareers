"""
Impersonate views — admin can "log in as" any user to see their dashboard.
"""
from django.shortcuts import get_object_or_404, redirect
from django.contrib.auth import get_user_model
from django.contrib import messages
from django.contrib.auth.decorators import login_required

User = get_user_model()


def _is_admin(user):
    return user.is_superuser or user.role == 'ADMIN'


@login_required
def start_impersonate(request, user_id):
    """Start impersonating a user. Admin only."""
    if not _is_admin(request.user):
        messages.error(request, "You don't have permission to impersonate users.")
        return redirect('home')

    target = get_object_or_404(User, pk=user_id)
    if target == request.user:
        messages.warning(request, "You can't impersonate yourself!")
        return redirect('home')

    request.session['_impersonate_user_id'] = target.pk
    messages.info(request, f'You are now viewing the platform as "{target.get_full_name() or target.username}".')

    # Redirect to the appropriate dashboard based on role
    if target.role == 'CONSULTANT' and hasattr(target, 'consultant_profile'):
        return redirect('consultant-dashboard')
    elif target.role == 'EMPLOYEE':
        return redirect('job-list')
    else:
        return redirect('home')


@login_required
def stop_impersonate(request):
    """Stop impersonating and return to admin."""
    if '_impersonate_user_id' in request.session:
        del request.session['_impersonate_user_id']
        messages.success(request, 'Stopped impersonating. You are back to your admin account.')
    return redirect('home')
