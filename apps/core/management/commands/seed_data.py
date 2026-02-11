from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from users.models import ConsultantProfile, EmployeeProfile
from jobs.models import Job

User = get_user_model()

class Command(BaseCommand):
    help = 'Seeds database with initial data for testing'

    def handle(self, *args, **kwargs):
        self.stdout.write('Seeding data...')

        # Create Admin
        if not User.objects.filter(username='admin').exists():
            User.objects.create_superuser('admin', 'admin@example.com', 'admin')
            self.stdout.write(self.style.SUCCESS('Created superuser: admin'))

        # Create Employee
        if not User.objects.filter(username='employee').exists():
            user = User.objects.create_user('employee', 'employee@example.com', 'password')
            user.role = User.Role.EMPLOYEE
            user.save()
            EmployeeProfile.objects.create(user=user, department='HR', company_name='TechCorp Inc.')
            self.stdout.write(self.style.SUCCESS('Created employee: employee'))

        # Create Consultant
        if not User.objects.filter(username='consultant').exists():
            user = User.objects.create_user('consultant', 'consultant@example.com', 'password')
            user.role = User.Role.CONSULTANT
            user.save()
            ConsultantProfile.objects.create(
                user=user, 
                bio='Senior Developer with 10 years experience in Python and React.',
                skills=['Python', 'Django', 'React', 'AWS'],
                hourly_rate=150.00
            )
            self.stdout.write(self.style.SUCCESS('Created consultant: consultant'))

        # Create Job
        employee = User.objects.get(username='employee')
        if not Job.objects.filter(title='Senior Python Developer').exists():
            Job.objects.create(
                title='Senior Python Developer',
                company='TechCorp Inc.',
                location='Remote',
                description='We are looking for a senior developer to lead our backend team. Must have experience with Django and DRF.\n\nGreat benefits!',
                requirements='- 5+ years Python\n- 3+ years Django\n- Experience with AWS',
                salary_range='$120k - $160k',
                job_type=Job.JobType.FULL_TIME,
                posted_by=employee
            )
            self.stdout.write(self.style.SUCCESS('Created job: Senior Python Developer'))

        self.stdout.write(self.style.SUCCESS('Data seeding complete!'))
