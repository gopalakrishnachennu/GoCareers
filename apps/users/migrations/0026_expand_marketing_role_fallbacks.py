from django.db import migrations


ROLES = [
    {
        "slug": "backend-developer",
        "name": "Backend Developer",
        "top_category": "IT",
        "display_order": 46,
        "description": "Backend APIs, services, and server-side application development.",
        "match_keywords": ["backend developer", "backend engineer", "api developer", "server-side engineer"],
    },
    {
        "slug": "full-stack-developer",
        "name": "Full Stack Developer",
        "top_category": "IT",
        "display_order": 47,
        "description": "Full-stack application development across frontend and backend systems.",
        "match_keywords": ["full stack developer", "full-stack engineer", "fullstack developer"],
    },
    {
        "slug": "cloud-engineer",
        "name": "Cloud Engineer",
        "top_category": "IT",
        "display_order": 48,
        "description": "Cloud infrastructure engineering across AWS, Azure, and GCP.",
        "match_keywords": ["cloud engineer", "cloud architect", "aws engineer", "azure engineer", "gcp engineer"],
    },
    {
        "slug": "platform-engineer",
        "name": "Platform Engineer",
        "top_category": "IT",
        "display_order": 49,
        "description": "Platform, internal developer platform, and infrastructure enablement engineering.",
        "match_keywords": ["platform engineer", "internal developer platform", "platform reliability"],
    },
    {
        "slug": "systems-engineer",
        "name": "Systems Engineer",
        "top_category": "IT",
        "display_order": 52,
        "description": "Systems engineering across infrastructure, servers, and enterprise platforms.",
        "match_keywords": ["systems engineer", "system engineer", "enterprise systems engineer"],
    },
    {
        "slug": "systems-administrator",
        "name": "Systems Administrator",
        "top_category": "IT",
        "display_order": 53,
        "description": "Systems administration for servers, identity, endpoints, and enterprise systems.",
        "match_keywords": ["systems administrator", "system administrator", "sysadmin", "server administrator"],
    },
    {
        "slug": "infrastructure-engineer",
        "name": "Infrastructure Engineer",
        "top_category": "IT",
        "display_order": 54,
        "description": "Infrastructure engineering for compute, storage, networking, and automation.",
        "match_keywords": ["infrastructure engineer", "infrastructure architect", "infrastructure automation"],
    },
    {
        "slug": "database-administrator",
        "name": "Database Administrator",
        "top_category": "IT",
        "display_order": 55,
        "description": "Database administration, performance, replication, and reliability.",
        "match_keywords": ["database administrator", "dba", "sql server admin", "oracle dba", "postgres admin"],
    },
    {
        "slug": "business-analyst-it",
        "name": "Business Analyst (IT)",
        "top_category": "IT",
        "display_order": 56,
        "description": "IT business analysis, requirements gathering, systems/process analysis.",
        "match_keywords": ["business analyst", "business systems analyst", "technical business analyst", "it business analyst"],
    },
    {
        "slug": "systems-analyst",
        "name": "Systems Analyst",
        "top_category": "IT",
        "display_order": 57,
        "description": "Systems analysis, requirements analysis, and application/process design.",
        "match_keywords": ["systems analyst", "system analyst", "application analyst"],
    },
    {
        "slug": "it-project-manager",
        "name": "IT Project Manager",
        "top_category": "IT",
        "display_order": 58,
        "description": "IT and software delivery project management.",
        "match_keywords": ["it project manager", "technical project manager", "technology project manager"],
    },
    {
        "slug": "scrum-master",
        "name": "Scrum Master / Agile Coach",
        "top_category": "IT",
        "display_order": 59,
        "description": "Scrum delivery and agile coaching.",
        "match_keywords": ["scrum master", "agile coach", "safe agil", "kanban coach"],
    },
    {
        "slug": "erp-consultant",
        "name": "ERP Consultant",
        "top_category": "IT",
        "display_order": 60,
        "description": "ERP implementations outside SAP/Oracle/Workday-specific routing.",
        "match_keywords": ["erp consultant", "erp analyst", "peoplesoft", "dynamics 365", "netsuite consultant"],
    },
    {
        "slug": "general-it",
        "name": "General IT",
        "top_category": "IT",
        "display_order": 90,
        "description": "Catch-all role for IT jobs that need routing before deeper specialization.",
        "match_keywords": [],
    },
    {
        "slug": "general-engineering",
        "name": "General Engineering",
        "top_category": "ENGINEERING",
        "display_order": 91,
        "description": "Catch-all role for non-IT engineering jobs awaiting deeper specialization.",
        "match_keywords": [],
    },
    {
        "slug": "general-healthcare",
        "name": "General Healthcare",
        "top_category": "HEALTHCARE",
        "display_order": 92,
        "description": "Catch-all role for healthcare jobs awaiting clinical specialization.",
        "match_keywords": [],
    },
    {
        "slug": "general-business",
        "name": "General Business",
        "top_category": "NON_IT",
        "display_order": 93,
        "description": "Catch-all role for non-IT business jobs awaiting deeper specialization.",
        "match_keywords": [],
    },
    {
        "slug": "other-generalist",
        "name": "Other / Generalist",
        "top_category": "OTHER",
        "display_order": 94,
        "description": "Last-resort routing role so harvested jobs never remain unassigned.",
        "match_keywords": [],
    },
]


def seed_roles(apps, schema_editor):
    MarketingRole = apps.get_model("users", "MarketingRole")
    for role_data in ROLES:
        MarketingRole.objects.update_or_create(
            slug=role_data["slug"],
            defaults={
                "name": role_data["name"],
                "top_category": role_data["top_category"],
                "display_order": role_data["display_order"],
                "description": role_data["description"],
                "match_keywords": role_data["match_keywords"],
                "is_active": True,
            },
        )


def unseed_roles(apps, schema_editor):
    MarketingRole = apps.get_model("users", "MarketingRole")
    MarketingRole.objects.filter(slug__in=[role["slug"] for role in ROLES]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0025_seed_marketing_roles"),
    ]

    operations = [
        migrations.RunPython(seed_roles, reverse_code=unseed_roles),
    ]
