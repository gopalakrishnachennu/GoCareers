from django.db import migrations, models


# ── Seed data: exact copy of _DOMAIN_PATTERNS from enrichments.py ────────────
# Priority = position * 10 so gaps are left for future insertions.
_SEED_DOMAINS = [
    # slug, name, regex, top_category, priority
    # ── IT: Named platforms ──────────────────────────────────────────────────
    ("servicenow-developer",  "ServiceNow Developer",
     r"\bservicenow\b",
     "IT", 10),
    ("salesforce-developer",  "Salesforce Developer",
     r"\bsalesforce\b|\bsfdc\b|\bapex\s*(developer|admin|architect)\b",
     "IT", 20),
    ("sap-consultant",        "SAP Consultant",
     r"\bsap\b\s*(abap|basis|hana|mm|sd|fi|co|wm|pp|ariba|successfactors|bw|s/4|s4hana|consultant|developer|analyst|erp)",
     "IT", 30),
    ("workday-consultant",    "Workday Consultant",
     r"\bworkday\s*(hcm|payroll|integ|studio|report|consultant|developer|analyst)\b",
     "IT", 40),
    ("oracle-consultant",     "Oracle Consultant",
     r"\boracle\s*(dba|ebs|fusion|hcm|cloud|financials|scm|consultant|developer)\b|\bpl/sql\b|\bpl\\sql\b",
     "IT", 50),
    ("healthcare-it",         "Healthcare IT",
     r"\b(epic|cerner|meditech|allscripts|mckesson)\b"
     r"|\bhealthcare\s*it\b|\bclinical\s*informatics\b"
     r"|\b(ehr|emr)\s*(implementation|consultant|analyst|specialist|developer)\b",
     "IT", 60),
    # ── IT: AI / ML ──────────────────────────────────────────────────────────
    ("ml-ai-engineer",        "ML / AI Engineer",
     r"\b(machine\s*learning|deep\s*learning|llm|generative\s*ai|mlops|ai\s*engineer"
     r"|nlp\s*(engineer|scientist)?|data\s*scien(ce|tist)|computer\s*vision|reinforcement\s*learning)\b",
     "IT", 70),
    # ── IT: Data ─────────────────────────────────────────────────────────────
    ("data-engineer",         "Data Engineer",
     r"\b(data\s*engineer|etl\s*(developer|engineer)|pipeline\s*engineer|databricks"
     r"|apache\s*(spark|kafka|flink|beam)|dbt\s*(developer|analyst)|data\s*platform)\b",
     "IT", 80),
    ("data-analyst",          "Data Analyst",
     r"\b(data\s*analyst|business\s*(intelligence|analyst)|bi\s*(analyst|developer|engineer)"
     r"|tableau|power\s*bi|looker|analytics\s*engineer)\b",
     "IT", 90),
    # ── IT: Infrastructure / Cloud ────────────────────────────────────────────
    ("devops-cloud",          "DevOps / Cloud Engineer",
     r"\b(devops\s*engineer|cloud\s*engineer|platform\s*engineer|site\s*reliability"
     r"|\bsre\b|devsecops|infrastructure\s*engineer"
     r"|aws\s*(architect|engineer|devops)|azure\s*(devops|engineer)|gcp\s*(engineer|architect))\b",
     "IT", 100),
    ("cybersecurity",         "Cybersecurity Engineer",
     r"\b(security\s*engineer|cybersecurity|information\s*security|soc\s*analyst"
     r"|penetration\s*test|red\s*team|blue\s*team|\bciso\b|\bappsec\b"
     r"|cloud\s*security|network\s*security|identity\s*(access|management))\b",
     "IT", 110),
    ("network-systems",       "Network / Systems Engineer",
     r"\b(network\s*(engineer|administrator|architect|\bops\b)"
     r"|\bcisco\b|\bjuniper\b|routing\s*(and|&)\s*switching"
     r"|network\s*security|vpn\s*engineer|network\s*operations)\b",
     "IT", 120),
    # ── IT: QA ───────────────────────────────────────────────────────────────
    ("qa-test-engineer",      "QA / Test Engineer",
     r"\b(qa\s*(engineer|analyst|lead)|quality\s*assurance\s*engineer"
     r"|test\s*(automation|engineer|lead)|\bsdet\b|automation\s*(test|engineer)"
     r"|selenium|cypress|playwright\s*test)\b",
     "IT", 130),
    # ── IT: Support ──────────────────────────────────────────────────────────
    ("it-support",            "IT Support",
     r"\b(help\s*desk|\bit\s*support\b|desktop\s*support|end\s*user\s*support"
     r"|service\s*desk|deskside|l[12]\s*support)\b",
     "IT", 140),
    # ── IT: Language-specific ────────────────────────────────────────────────
    ("java-developer",        "Java Developer",
     r"\b(java\s*(developer|engineer|backend|architect)|spring\s*(boot|framework)"
     r"|\bj2ee\b|jakarta\s*ee|java\s*microservices)\b",
     "IT", 150),
    ("python-developer",      "Python Developer",
     r"\b(python\s*(developer|engineer|backend)|django\s*developer|flask\s*developer"
     r"|fastapi\s*(developer|engineer))\b",
     "IT", 160),
    ("dotnet-developer",      ".NET / C# Developer",
     r"\b(\.net\s*(developer|engineer)|c#\s*(developer|engineer)|asp\.net|blazor"
     r"|dotnet\s*(developer|core))\b",
     "IT", 170),
    ("mobile-developer",      "Mobile Developer",
     r"\b(ios\s*(developer|engineer)|android\s*(developer|engineer)"
     r"|react\s*native\s*(developer|engineer)|flutter\s*(developer|engineer)"
     r"|mobile\s*(developer|engineer|app))\b",
     "IT", 180),
    ("frontend-developer",    "Frontend Developer",
     r"\b(frontend\s*(developer|engineer)|react\s*(developer|engineer)"
     r"|angular\s*(developer|engineer)|vue\s*(developer|engineer)"
     r"|javascript\s*(developer|engineer)|ui\s*(developer|engineer))\b",
     "IT", 190),
    ("embedded-systems",      "Embedded Systems Engineer",
     r"\b(embedded\s*(software|engineer|developer)|firmware\s*(engineer|developer)"
     r"|\brtos\b|\biot\s*(engineer|developer)\b|real.time\s*(systems|os))\b",
     "IT", 200),
    # ── IT: Broad ────────────────────────────────────────────────────────────
    ("software-developer",    "Software Developer / Engineer",
     r"\b(software\s*(engineer|developer|architect)|full.?stack\s*(developer|engineer)"
     r"|backend\s*(developer|engineer)|application\s*developer)\b",
     "IT", 210),
    ("it-management",         "IT Management",
     r"\b(\bit\s*manager\b|\bit\s*director\b|\bcio\b|\bcto\b|vp\s*(of\s*(\w+\s*)?)?engineering"
     r"|director\s*(of\s*(\w+\s*)?)?(engineering|technology|\bit\b)"
     r"|\bit\s*project\s*manager\b|it\s*(architect|strategist))\b",
     "IT", 220),
    # ── NON-IT: Business ─────────────────────────────────────────────────────
    ("product-manager",       "Product Manager",
     r"\b(product\s*manager|product\s*owner|technical\s*program\s*manager"
     r"|director\s*of\s*product)\b",
     "NON_IT", 230),
    ("sales",                 "Sales",
     r"\b(sales\s*(representative|executive|manager|engineer|development|director)"
     r"|account\s*executive|business\s*development\s*(manager|rep)"
     r"|\bsdr\b|\bbdr\b|inside\s*sales)\b",
     "NON_IT", 240),
    ("marketing-specialist",  "Marketing Specialist",
     r"\b(marketing\s*(manager|director|specialist|coordinator|analyst)"
     r"|digital\s*marketing|\bseo\s*(specialist|manager)\b|content\s*market"
     r"|demand\s*gen|brand\s*(manager|strategist)|growth\s*market)\b",
     "NON_IT", 250),
    ("finance-accounting",    "Finance / Accounting",
     r"\b(financial\s*analyst|accountant|\bcpa\b|controller|chief\s*financial"
     r"|\bcfo\b|\bfp&a\b|accounts\s*(payable|receivable)|staff\s*accountant"
     r"|tax\s*(analyst|manager)|audit(or)?)\b",
     "NON_IT", 260),
    ("hr-recruiter",          "HR / Recruiter",
     r"\b(human\s*resources|hr\s*(generalist|manager|business\s*partner|director)"
     r"|recruiter|talent\s*acquisition|sourcer|people\s*ops|\bhrbp\b"
     r"|compensation\s*(analyst|manager))\b",
     "NON_IT", 270),
    ("operations",            "Operations",
     r"\b(operations\s*(manager|director|analyst)|supply\s*chain"
     r"|logistics\s*(manager|coordinator)|procurement\s*(manager|analyst)"
     r"|project\s*manager)\b",
     "NON_IT", 280),
    ("customer-success",      "Customer Success",
     r"\b(customer\s*success\s*(manager|specialist)|account\s*manager"
     r"|client\s*(services|success|relations)"
     r"|customer\s*(support|experience)\s*manager)\b",
     "NON_IT", 290),
    ("administrative",        "Administrative",
     r"\b(executive\s*assistant|office\s*manager|administrative\s*(assistant|coordinator)"
     r"|receptionist|office\s*coordinator)\b",
     "NON_IT", 300),
    # ── ENGINEERING (Non-IT) ─────────────────────────────────────────────────
    ("civil-engineer",        "Civil Engineer",
     r"\b(civil\s*engineer|structural\s*engineer|geotechnical|construction\s*(manager|engineer)"
     r"|transportation\s*engineer)\b",
     "ENGINEERING", 310),
    ("mechanical-engineer",   "Mechanical Engineer",
     r"\b(mechanical\s*engineer|manufacturing\s*engineer"
     r"|product\s*(design|development)\s*engineer|cad\s*engineer|solidworks)\b",
     "ENGINEERING", 320),
    ("electrical-engineer",   "Electrical Engineer",
     r"\b(electrical\s*engineer|power\s*(systems|electronics)\s*engineer"
     r"|pcb\s*design|control\s*systems\s*engineer)\b",
     "ENGINEERING", 330),
    # ── HEALTHCARE ───────────────────────────────────────────────────────────
    ("clinical-nursing",      "Clinical Nursing",
     r"\b(registered\s*nurse|\brn\b|licensed\s*practical\s*nurse|\blpn\b"
     r"|nurse\s*(practitioner|anesthetist|educator|manager)|clinical\s*nurse)\b",
     "HEALTHCARE", 340),
    ("physician",             "Physician",
     r"\b(physician\b|hospitalist\b|surgeon\b|attending\s*physician)\b",
     "HEALTHCARE", 350),
    ("allied-health",         "Allied Health",
     r"\b(physical\s*therapist|occupational\s*therapist"
     r"|speech\s*(language\s*)?pathologist|medical\s*assistant"
     r"|radiolog|sonographer|respiratory\s*therapist)\b",
     "HEALTHCARE", 360),
    ("pharmacy",              "Pharmacy",
     r"\b(pharmacist\b|pharmacy\s*technician|clinical\s*pharmacist)\b",
     "HEALTHCARE", 370),
    ("healthcare-clinical",   "Healthcare Clinical",
     r"\b(healthcare\s*(administrator|manager|coordinator)"
     r"|clinical\s*(coordinator|specialist|manager)"
     r"|medical\s*(director|officer|coordinator))\b",
     "HEALTHCARE", 380),
]


def seed_job_domains(apps, schema_editor):
    JobDomain = apps.get_model("harvest", "JobDomain")
    for slug, name, regex, top_category, priority in _SEED_DOMAINS:
        JobDomain.objects.get_or_create(
            slug=slug,
            defaults={
                "name": name,
                "regex_pattern": regex,
                "top_category": top_category,
                "priority": priority,
                "is_active": True,
            },
        )


def unseed_job_domains(apps, schema_editor):
    JobDomain = apps.get_model("harvest", "JobDomain")
    JobDomain.objects.all().delete()


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0063_alter_vetgateconfig_id"),
    ]

    operations = [
        migrations.CreateModel(
            name="JobDomain",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("slug", models.SlugField(max_length=80, unique=True)),
                ("name", models.CharField(max_length=100)),
                ("regex_pattern", models.TextField(
                    help_text="Python regex matched against job title and description (case-insensitive). "
                              "Validated before save — bad regex is rejected."
                )),
                ("top_category", models.CharField(
                    db_index=True, default="IT", max_length=20,
                    choices=[
                        ("IT", "Information Technology"),
                        ("NON_IT", "Non-IT / Business"),
                        ("ENGINEERING", "Engineering (Non-IT)"),
                        ("HEALTHCARE", "Healthcare & Clinical"),
                        ("OTHER", "Other"),
                    ],
                )),
                ("priority", models.PositiveSmallIntegerField(
                    default=500,
                    help_text="Lower = matched first. Keep gaps (10, 20, 30…) so you can insert between.",
                )),
                ("is_active", models.BooleanField(db_index=True, default=True)),
                ("notes", models.TextField(blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Job Domain",
                "verbose_name_plural": "Job Domains",
                "ordering": ["priority", "slug"],
            },
        ),
        # Seed all 38 existing hardcoded patterns into DB
        migrations.RunPython(seed_job_domains, reverse_code=unseed_job_domains),
    ]
