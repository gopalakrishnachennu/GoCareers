from django.core.management.base import BaseCommand
from users.models import MarketingRole


ROLES = [
    ("DevOps Engineer", "Build and manage CI/CD pipelines, infrastructure automation, and cloud deployments."),
    ("Cloud Architect", "Design and implement scalable cloud infrastructure solutions."),
    ("Full Stack Developer", "Develop both frontend and backend components of web applications."),
    ("Backend Developer", "Build server-side logic, APIs, and database integrations."),
    ("Frontend Developer", "Create responsive, interactive user interfaces and web experiences."),
    ("Data Engineer", "Design data pipelines, ETL processes, and data warehousing solutions."),
    ("ML/AI Engineer", "Develop machine learning models and AI-powered applications."),
    ("Platform Engineer", "Build and maintain internal developer platforms and tooling."),
    ("SRE / Reliability Engineer", "Ensure system reliability, monitoring, and incident response."),
    ("Security Engineer", "Implement security best practices, audits, and vulnerability management."),
    ("QA / Test Engineer", "Design and execute testing strategies for software quality assurance."),
    ("Mobile Developer", "Build native and cross-platform mobile applications."),
]


class Command(BaseCommand):
    help = "Seed the database with predefined marketing roles."

    def handle(self, *args, **options):
        created_count = 0
        for name, description in ROLES:
            _, created = MarketingRole.objects.get_or_create(
                name=name,
                defaults={"description": description}
            )
            if created:
                created_count += 1
                self.stdout.write(self.style.SUCCESS(f"  Created: {name}"))
            else:
                self.stdout.write(f"  Already exists: {name}")
        self.stdout.write(self.style.SUCCESS(f"\nDone! {created_count} new roles created."))
