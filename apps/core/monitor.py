import time
from django.db import connections
from django.db.utils import OperationalError
from django.test import Client
from django.urls import reverse

class SystemMonitor:
    def check_all(self):
        """
        Run all system checks and return a dict of results.
        """
        return {
            'database': self.check_database(),
            'pages': self.check_critical_pages(),
        }

    def check_database(self):
        """
        Check connectivity to the default database.
        """
        start = time.time()
        status = "Operational"
        error = None
        try:
            conn = connections['default']
            conn.cursor()
        except OperationalError as e:
            status = "Failed"
            error = str(e)
        
        duration = (time.time() - start) * 1000
        return {
            'name': 'Default Database',
            'status': status,
            'duration_ms': round(duration, 2),
            'error': error
        }

    def check_critical_pages(self):
        """
        Ping critical internal URLs to ensure they load (200 or 302).
        Uses Django Test Client to avoid network overhead.
        """
        client = Client()
        pages = [
            # Core
            {'name': 'Home Page', 'url': reverse('home')},
            {'name': 'Admin Login', 'url': reverse('admin:login')},
            {'name': 'Settings Dashboard', 'url': reverse('settings-dashboard')},
            
            # Consultants & Users
            {'name': 'Consultant List', 'url': reverse('consultant-list')},
            {'name': 'Consultant Dashboard', 'url': reverse('consultant-dashboard')},
            {'name': 'Saved Jobs', 'url': reverse('saved-jobs')},
            
            # Employees
            {'name': 'Employee List', 'url': reverse('employee-list')},
            {'name': 'Employee Dashboard', 'url': reverse('employee-dashboard')},

            # Jobs
            {'name': 'Job List', 'url': reverse('job-list')},
            {'name': 'Job Create', 'url': reverse('job-create')},
            
            # Communications
            {'name': 'Inbox', 'url': reverse('inbox')},
            
            # Admin Tools
            {'name': 'Analytics Dashboard', 'url': reverse('analytics-dashboard')},
            {'name': 'Marketing Roles', 'url': reverse('marketing-role-list')},
        ]

        results = []
        for page in pages:
            start = time.time()
            try:
                # Use HTTP_HOST='localhost' to bypass ALLOWED_HOSTS checks for 'testserver'
                response = client.get(page['url'], HTTP_HOST='localhost')
                status_code = response.status_code
                if status_code in [200, 302]:
                    status = "Operational"
                    error = None
                else:
                    status = "Failed"
                    error = f"HTTP {status_code}"
            except Exception as e:
                status = "Failed"
                status_code = 0
                error = str(e)

            duration = (time.time() - start) * 1000
            results.append({
                'name': page['name'],
                'url': page['url'],
                'status': status,
                'status_code': status_code,
                'duration_ms': round(duration, 2),
                'error': error
            })
        return results
