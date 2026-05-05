"""
Seed the canonical MarketingRole records that power the job domain taxonomy.

Every entry maps to a MarketingRole.slug and carries:
  - top_category  — IT / NON_IT / ENGINEERING / HEALTHCARE / OTHER
  - match_keywords — used by detect_job_domain() in enrichments.py
  - display_order  — groups by category (IT first, then NON_IT, etc.)

Run once on migration. Safe to re-run: uses update_or_create so existing
manually-created rows with the same slug are updated, not duplicated.
"""
from django.db import migrations

ROLES = [
    # ── IT: Named platforms ───────────────────────────────────────────────────
    {
        "slug": "servicenow-developer",
        "name": "ServiceNow Developer",
        "top_category": "IT",
        "display_order": 10,
        "description": "ServiceNow platform development, configuration, and ITSM integrations.",
        "match_keywords": ["servicenow", "snow developer", "itsm developer"],
    },
    {
        "slug": "salesforce-developer",
        "name": "Salesforce Developer",
        "top_category": "IT",
        "display_order": 11,
        "description": "Salesforce CRM development — Apex, LWC, Visualforce, SFDC admin.",
        "match_keywords": ["salesforce", "sfdc", "apex developer", "salesforce admin", "lwc developer"],
    },
    {
        "slug": "sap-consultant",
        "name": "SAP Consultant",
        "top_category": "IT",
        "display_order": 12,
        "description": "SAP ERP modules: ABAP, BASIS, HANA, MM, SD, FI, S/4HANA, SuccessFactors.",
        "match_keywords": ["sap abap", "sap basis", "sap hana", "sap mm", "sap sd", "sap fi", "sap consultant", "s4hana", "sap developer"],
    },
    {
        "slug": "workday-consultant",
        "name": "Workday Consultant",
        "top_category": "IT",
        "display_order": 13,
        "description": "Workday HCM, Payroll, Integrations, Reporting, and Studio.",
        "match_keywords": ["workday hcm", "workday payroll", "workday integrations", "workday consultant", "workday developer"],
    },
    {
        "slug": "oracle-consultant",
        "name": "Oracle Consultant",
        "top_category": "IT",
        "display_order": 14,
        "description": "Oracle EBS, Fusion, HCM, Financials, DBA, and PL/SQL development.",
        "match_keywords": ["oracle dba", "oracle ebs", "oracle fusion", "oracle hcm", "pl/sql", "oracle developer"],
    },
    {
        "slug": "healthcare-it",
        "name": "Healthcare IT",
        "top_category": "IT",
        "display_order": 15,
        "description": "EHR/EMR systems (Epic, Cerner, Meditech), clinical informatics, healthcare IT.",
        "match_keywords": ["epic", "cerner", "meditech", "healthcare it", "ehr implementation", "emr consultant", "clinical informatics"],
    },
    # ── IT: AI / ML ───────────────────────────────────────────────────────────
    {
        "slug": "ml-ai-engineer",
        "name": "ML / AI Engineer",
        "top_category": "IT",
        "display_order": 20,
        "description": "Machine learning, deep learning, NLP, generative AI, MLOps, data science.",
        "match_keywords": ["machine learning", "deep learning", "llm engineer", "ai engineer", "mlops", "data scientist", "nlp engineer", "generative ai"],
    },
    # ── IT: Data ──────────────────────────────────────────────────────────────
    {
        "slug": "data-engineer",
        "name": "Data Engineer",
        "top_category": "IT",
        "display_order": 21,
        "description": "Data pipelines, ETL, Spark, Kafka, Databricks, dbt, data platform.",
        "match_keywords": ["data engineer", "etl developer", "databricks", "apache spark", "apache kafka", "dbt developer", "data platform engineer"],
    },
    {
        "slug": "data-analyst",
        "name": "Data Analyst",
        "top_category": "IT",
        "display_order": 22,
        "description": "BI, business intelligence, Tableau, Power BI, analytics engineering.",
        "match_keywords": ["data analyst", "business intelligence", "bi analyst", "tableau", "power bi", "looker", "analytics engineer"],
    },
    # ── IT: Infrastructure / Cloud ────────────────────────────────────────────
    {
        "slug": "devops-cloud",
        "name": "DevOps / Cloud Engineer",
        "top_category": "IT",
        "display_order": 30,
        "description": "DevOps, SRE, cloud engineering (AWS/Azure/GCP), infrastructure, CI/CD.",
        "match_keywords": ["devops engineer", "cloud engineer", "platform engineer", "site reliability engineer", "sre", "aws engineer", "azure devops", "gcp engineer", "infrastructure engineer"],
    },
    {
        "slug": "cybersecurity",
        "name": "Cybersecurity Engineer",
        "top_category": "IT",
        "display_order": 31,
        "description": "Security engineering, SOC, pen testing, AppSec, cloud security, IAM.",
        "match_keywords": ["security engineer", "cybersecurity", "soc analyst", "penetration testing", "information security", "appsec", "cloud security"],
    },
    {
        "slug": "network-systems",
        "name": "Network / Systems Engineer",
        "top_category": "IT",
        "display_order": 32,
        "description": "Network engineering, Cisco/Juniper, routing/switching, systems administration.",
        "match_keywords": ["network engineer", "network administrator", "cisco", "juniper", "routing and switching", "network operations"],
    },
    # ── IT: QA ────────────────────────────────────────────────────────────────
    {
        "slug": "qa-test-engineer",
        "name": "QA / Test Engineer",
        "top_category": "IT",
        "display_order": 33,
        "description": "QA, SDET, automation testing (Selenium, Cypress, Playwright), quality assurance.",
        "match_keywords": ["qa engineer", "test automation engineer", "sdet", "quality assurance engineer", "selenium", "cypress automation"],
    },
    {
        "slug": "it-support",
        "name": "IT Support / Help Desk",
        "top_category": "IT",
        "display_order": 34,
        "description": "Help desk, IT support, desktop support, service desk, end-user support.",
        "match_keywords": ["help desk", "it support", "desktop support", "service desk", "end user support", "l1 support", "l2 support"],
    },
    # ── IT: Language-specific ─────────────────────────────────────────────────
    {
        "slug": "java-developer",
        "name": "Java Developer",
        "top_category": "IT",
        "display_order": 40,
        "description": "Java development — Spring Boot, Microservices, J2EE, Jakarta EE.",
        "match_keywords": ["java developer", "java engineer", "spring boot", "j2ee", "java microservices"],
    },
    {
        "slug": "python-developer",
        "name": "Python Developer",
        "top_category": "IT",
        "display_order": 41,
        "description": "Python development — Django, Flask, FastAPI, scripting.",
        "match_keywords": ["python developer", "django developer", "flask developer", "fastapi developer"],
    },
    {
        "slug": "dotnet-developer",
        "name": ".NET Developer",
        "top_category": "IT",
        "display_order": 42,
        "description": ".NET, C#, ASP.NET, Blazor development.",
        "match_keywords": [".net developer", "c# developer", "asp.net developer", "dotnet developer", "blazor developer"],
    },
    {
        "slug": "mobile-developer",
        "name": "Mobile Developer",
        "top_category": "IT",
        "display_order": 43,
        "description": "iOS, Android, React Native, Flutter mobile development.",
        "match_keywords": ["ios developer", "android developer", "react native developer", "flutter developer", "mobile developer"],
    },
    {
        "slug": "frontend-developer",
        "name": "Frontend Developer",
        "top_category": "IT",
        "display_order": 44,
        "description": "Frontend/UI — React, Angular, Vue, JavaScript development.",
        "match_keywords": ["frontend developer", "react developer", "angular developer", "vue developer", "javascript developer", "ui developer"],
    },
    {
        "slug": "embedded-systems",
        "name": "Embedded Systems Engineer",
        "top_category": "IT",
        "display_order": 45,
        "description": "Embedded software, firmware, RTOS, IoT development.",
        "match_keywords": ["embedded software engineer", "firmware engineer", "rtos developer", "iot engineer"],
    },
    {
        "slug": "software-developer",
        "name": "Software Developer",
        "top_category": "IT",
        "display_order": 50,
        "description": "General software engineering — full-stack, backend, application development.",
        "match_keywords": ["software engineer", "software developer", "full stack developer", "backend developer", "application developer"],
    },
    {
        "slug": "it-management",
        "name": "IT Management / Architecture",
        "top_category": "IT",
        "display_order": 51,
        "description": "CTO, CIO, VP Engineering, IT Director, IT Architect.",
        "match_keywords": ["it manager", "it director", "cto", "cio", "vp engineering", "director of engineering", "it architect"],
    },
    # ── NON-IT: Business ──────────────────────────────────────────────────────
    {
        "slug": "product-manager",
        "name": "Product Manager",
        "top_category": "NON_IT",
        "display_order": 60,
        "description": "Product management, product ownership, technical program management.",
        "match_keywords": ["product manager", "product owner", "technical program manager", "director of product"],
    },
    {
        "slug": "sales",
        "name": "Sales",
        "top_category": "NON_IT",
        "display_order": 61,
        "description": "Sales representative, account executive, business development, SDR/BDR.",
        "match_keywords": ["sales representative", "account executive", "business development manager", "sdr", "bdr", "inside sales", "sales manager"],
    },
    {
        "slug": "marketing-specialist",
        "name": "Marketing Specialist",
        "top_category": "NON_IT",
        "display_order": 62,
        "description": "Digital marketing, SEO, content marketing, demand generation, brand management.",
        "match_keywords": ["marketing manager", "digital marketing", "seo specialist", "content marketing", "demand generation", "brand manager"],
    },
    {
        "slug": "finance-accounting",
        "name": "Finance / Accounting",
        "top_category": "NON_IT",
        "display_order": 63,
        "description": "Financial analyst, accountant, CPA, controller, FP&A, auditor.",
        "match_keywords": ["financial analyst", "accountant", "cpa", "controller", "fp&a", "accounts payable", "accounts receivable", "auditor"],
    },
    {
        "slug": "hr-recruiter",
        "name": "HR / Recruiter",
        "top_category": "NON_IT",
        "display_order": 64,
        "description": "HR generalist, recruiter, talent acquisition, people ops, compensation.",
        "match_keywords": ["human resources", "recruiter", "talent acquisition", "hr generalist", "people operations", "hrbp"],
    },
    {
        "slug": "operations",
        "name": "Operations",
        "top_category": "NON_IT",
        "display_order": 65,
        "description": "Operations management, supply chain, logistics, procurement, project management.",
        "match_keywords": ["operations manager", "supply chain", "logistics manager", "procurement manager", "project manager"],
    },
    {
        "slug": "customer-success",
        "name": "Customer Success",
        "top_category": "NON_IT",
        "display_order": 66,
        "description": "Customer success, account management, client services, customer support.",
        "match_keywords": ["customer success manager", "account manager", "client services", "customer support manager"],
    },
    {
        "slug": "administrative",
        "name": "Administrative",
        "top_category": "NON_IT",
        "display_order": 67,
        "description": "Executive assistant, office manager, administrative coordinator.",
        "match_keywords": ["executive assistant", "office manager", "administrative assistant", "administrative coordinator"],
    },
    # ── ENGINEERING (Non-IT) ──────────────────────────────────────────────────
    {
        "slug": "civil-engineer",
        "name": "Civil Engineer",
        "top_category": "ENGINEERING",
        "display_order": 70,
        "description": "Civil, structural, geotechnical, transportation, construction engineering.",
        "match_keywords": ["civil engineer", "structural engineer", "geotechnical engineer", "construction manager", "transportation engineer"],
    },
    {
        "slug": "mechanical-engineer",
        "name": "Mechanical Engineer",
        "top_category": "ENGINEERING",
        "display_order": 71,
        "description": "Mechanical, manufacturing, product design engineering, CAD, SolidWorks.",
        "match_keywords": ["mechanical engineer", "manufacturing engineer", "product design engineer", "cad engineer", "solidworks engineer"],
    },
    {
        "slug": "electrical-engineer",
        "name": "Electrical Engineer",
        "top_category": "ENGINEERING",
        "display_order": 72,
        "description": "Electrical, power systems, PCB design, control systems engineering.",
        "match_keywords": ["electrical engineer", "power systems engineer", "pcb design engineer", "control systems engineer"],
    },
    # ── HEALTHCARE ────────────────────────────────────────────────────────────
    {
        "slug": "clinical-nursing",
        "name": "Clinical Nursing",
        "top_category": "HEALTHCARE",
        "display_order": 80,
        "description": "Registered nurse (RN), LPN, nurse practitioner, clinical nursing.",
        "match_keywords": ["registered nurse", "rn", "lpn", "nurse practitioner", "clinical nurse"],
    },
    {
        "slug": "physician",
        "name": "Physician",
        "top_category": "HEALTHCARE",
        "display_order": 81,
        "description": "Physician, hospitalist, surgeon, attending physician.",
        "match_keywords": ["physician", "hospitalist", "surgeon", "attending physician"],
    },
    {
        "slug": "allied-health",
        "name": "Allied Health",
        "top_category": "HEALTHCARE",
        "display_order": 82,
        "description": "Physical therapist, occupational therapist, medical assistant, radiologist.",
        "match_keywords": ["physical therapist", "occupational therapist", "speech pathologist", "medical assistant", "radiologist"],
    },
    {
        "slug": "pharmacy",
        "name": "Pharmacy",
        "top_category": "HEALTHCARE",
        "display_order": 83,
        "description": "Pharmacist, pharmacy technician, clinical pharmacist.",
        "match_keywords": ["pharmacist", "pharmacy technician", "clinical pharmacist"],
    },
    {
        "slug": "healthcare-clinical",
        "name": "Healthcare Administration",
        "top_category": "HEALTHCARE",
        "display_order": 84,
        "description": "Healthcare administrator, clinical coordinator, medical director.",
        "match_keywords": ["healthcare administrator", "clinical coordinator", "medical director", "healthcare manager"],
    },
]


def seed_marketing_roles(apps, schema_editor):
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


def unseed_marketing_roles(apps, schema_editor):
    """Reverse: remove only the seeded slugs, leave manually-created roles alone."""
    MarketingRole = apps.get_model("users", "MarketingRole")
    slugs = [r["slug"] for r in ROLES]
    MarketingRole.objects.filter(slug__in=slugs).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("users", "0024_marketing_role_domain_fields"),
    ]

    operations = [
        migrations.RunPython(seed_marketing_roles, reverse_code=unseed_marketing_roles),
    ]
