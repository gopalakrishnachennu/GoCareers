import json
from .models import AuditLog

class AuditMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)

        if request.user.is_authenticated and request.method in ['POST', 'PUT', 'PATCH', 'DELETE']:
            self.log_action(request, response)

        return response

    def log_action(self, request, response):
        # Skip logging for certain paths if needed (e.g. login/logout might be handled by signals, but good to have here too)
        # Also skip if response was not successful (optional, but usually we want to know about attempts too)
        
        try:
            # Try to get target info from URL kwargs if available (Django resolves them before middleware finishes)
            resolver_match = request.resolver_match
            target_id = ""
            if resolver_match and resolver_match.kwargs:
                target_id = str(resolver_match.kwargs.get('pk', '') or resolver_match.kwargs.get('id', ''))

            details = {
                'path': request.path,
                'method': request.method,
                'status_code': response.status_code,
                'query_params': dict(request.GET),
            }
            
            # Be careful logging POST data (passwords, etc.)
            # For now, we skip body logging to be safe, or we could whitelist fields.

            AuditLog.objects.create(
                actor=request.user,
                action=f"{request.method} {request.path}",
                target_model="", # Infer from path or view name if possible, expensive to do generic logic here
                target_id=target_id,
                details=details,
                ip_address=self.get_client_ip(request)
            )
        except Exception as e:
            # Prevent logging errors from crashing the request
            print(f"Audit Log Error: {e}")

    def get_client_ip(self, request):
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip
