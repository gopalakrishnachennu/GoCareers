"""seed_harvest_categories — upsert the canonical HarvestRoleCategory rows.

This command creates-or-updates the 9 canonical harvest categories that align
with all 31 IT MarketingRole slugs.  It is safe to run multiple times; it
uses update_or_create on slug so existing phrase customisations are preserved
(only adds missing phrases, never removes user additions).

Usage:
    python manage.py seed_harvest_categories           # preview changes
    python manage.py seed_harvest_categories --apply   # write to DB
"""
from __future__ import annotations

from django.core.management.base import BaseCommand

# ── Canonical category definitions ───────────────────────────────────────────
# Priority controls which category wins when a title could match multiple ones.
# Lower number = checked first.
# Rules:
#   - Phrases must NOT contain seniority words (senior / staff / lead / etc.)
#     because the title normalizer strips those before matching.
#   - Use multi-word phrases wherever possible to avoid false positives.
#   - Every IT MarketingRole slug must be reachable by at least one category.
# ─────────────────────────────────────────────────────────────────────────────
CATEGORIES = [
    # ── 1. DevOps / Cloud / Infrastructure ───────────────────────────────────
    # Covers: devops-cloud (MarketingRole)
    {
        "name": "DevOps / Cloud / Infrastructure",
        "slug": "devops",
        "priority": 10,
        "include_phrases": [
            "devops engineer",
            "devops",
            "sre",
            "site reliability engineer",
            "site reliability",
            "cloud engineer",
            "cloud infrastructure",
            "infrastructure engineer",
            "platform engineer",
            "kubernetes engineer",
            "k8s engineer",
            "terraform engineer",
            "ansible engineer",
            "ci cd engineer",
            "cicd engineer",
            "release engineer",
            "build release engineer",
            "aws engineer",
            "azure engineer",
            "gcp engineer",
            "cloud operations",
            "cloudops",
            "finops engineer",
            "devsecops engineer",
            "gitops",
            "helm",
        ],
        "exclude_phrases": [],
        "notes": "Covers MarketingRole: devops-cloud",
    },

    # ── 2. Software Engineering / Full Stack / Backend / Frontend ─────────────
    # Covers: java-developer, python-developer, dotnet-developer, mobile-developer,
    #         frontend-developer, software-developer, embedded-systems-engineer
    {
        "name": "Software Engineering / Full Stack / Backend / Frontend",
        "slug": "software",
        "priority": 20,
        "include_phrases": [
            "software engineer",
            "software developer",
            "full stack engineer",
            "full stack developer",
            "backend engineer",
            "backend developer",
            "frontend engineer",
            "frontend developer",
            "java developer",
            "java engineer",
            "spring boot developer",
            "python developer",
            "python engineer",
            "django developer",
            "fastapi developer",
            "dotnet developer",
            "net developer",
            "csharp developer",
            "asp net developer",
            "mobile developer",
            "ios developer",
            "android developer",
            "react native developer",
            "flutter developer",
            "react developer",
            "angular developer",
            "vue developer",
            "node developer",
            "nodejs developer",
            "typescript developer",
            "golang developer",
            "go developer",
            "rust developer",
            "embedded software engineer",
            "embedded systems engineer",
            "firmware engineer",
            "application developer",
            "application engineer",
        ],
        "exclude_phrases": [
            # These words in the title usually mean non-tech even if "engineer" appears
            "civil engineer",
            "mechanical engineer",
            "electrical engineer",
            "chemical engineer",
            "structural engineer",
            "biomedical engineer",
            "manufacturing engineer",
            "process engineer",
            "environmental engineer",
            "project engineer",
            "field engineer",
            "application support",  # support ≠ developer
        ],
        "notes": (
            "Covers MarketingRole: java-developer, python-developer, dotnet-developer, "
            "mobile-developer, frontend-developer, software-developer, embedded-systems-engineer. "
            "Do NOT add seniority words (senior/staff/lead) as phrases — the title normalizer "
            "already strips them so 'Senior Software Engineer' matches 'software engineer'."
        ),
    },

    # ── 3. Data Engineering / Analytics / BI ─────────────────────────────────
    # Covers: data-engineer, data-analyst
    {
        "name": "Data Engineering / Analytics / BI",
        "slug": "data",
        "priority": 30,
        "include_phrases": [
            "data engineer",
            "data analyst",
            "data architect",
            "analytics engineer",
            "analytics developer",
            "bi developer",
            "bi engineer",
            "business intelligence developer",
            "business intelligence engineer",
            "etl developer",
            "etl engineer",
            "databricks engineer",
            "spark engineer",
            "kafka engineer",
            "flink engineer",
            "dbt developer",
            "dbt engineer",
            "snowflake developer",
            "snowflake engineer",
            "redshift engineer",
            "bigquery engineer",
            "tableau developer",
            "power bi developer",
            "looker developer",
            "data platform engineer",
            "data pipeline engineer",
            "data warehouse engineer",
            "data lake engineer",
        ],
        "exclude_phrases": [
            "data entry",
            "data entry clerk",
        ],
        "notes": "Covers MarketingRole: data-engineer, data-analyst",
    },

    # ── 4. AI / ML / Data Science ─────────────────────────────────────────────
    # Covers: ml-ai-engineer (MarketingRole)
    {
        "name": "AI / ML / Data Science",
        "slug": "ai_ml",
        "priority": 40,
        "include_phrases": [
            "machine learning engineer",
            "ml engineer",
            "ai engineer",
            "artificial intelligence engineer",
            "data scientist",
            "deep learning engineer",
            "nlp engineer",
            "computer vision engineer",
            "mlops engineer",
            "generative ai engineer",
            "llm engineer",
            "research scientist",
            "applied scientist",
            "applied ml engineer",
            "ai researcher",
            "ml researcher",
            "machine learning scientist",
            "reinforcement learning engineer",
            "recommendation systems engineer",
        ],
        "exclude_phrases": [],
        "notes": "Covers MarketingRole: ml-ai-engineer",
    },

    # ── 5. Security / InfoSec / Network ──────────────────────────────────────
    # Covers: cybersecurity, network-systems (MarketingRole)
    {
        "name": "Security / InfoSec / Network",
        "slug": "security",
        "priority": 50,
        "include_phrases": [
            "security engineer",
            "cybersecurity engineer",
            "information security engineer",
            "infosec engineer",
            "penetration tester",
            "pen tester",
            "soc analyst",
            "security analyst",
            "threat intelligence analyst",
            "cloud security engineer",
            "appsec engineer",
            "application security engineer",
            "devsecops",
            "iam engineer",
            "identity access management",
            "network engineer",
            "network architect",
            "network administrator",
            "systems administrator",
            "sysadmin",
            "active directory engineer",
            "cisco engineer",
            "juniper engineer",
            "routing switching engineer",
        ],
        "exclude_phrases": [],
        "notes": "Covers MarketingRole: cybersecurity, network-systems",
    },

    # ── 6. QA / SDET / Test Automation ───────────────────────────────────────
    # Covers: qa-test-engineer (MarketingRole)
    {
        "name": "QA / SDET / Test Automation",
        "slug": "qa",
        "priority": 60,
        "include_phrases": [
            "qa engineer",
            "quality assurance engineer",
            "quality engineer",
            "sdet",
            "test automation engineer",
            "automation test engineer",
            "selenium engineer",
            "cypress engineer",
            "playwright engineer",
            "performance test engineer",
            "load test engineer",
            "software test engineer",
            "software tester",
            "mobile qa engineer",
            "api test engineer",
        ],
        "exclude_phrases": [],
        "notes": "Covers MarketingRole: qa-test-engineer",
    },

    # ── 7. Technical Product / Program / Architecture ─────────────────────────
    # Covers: it-management-architecture (MarketingRole)
    {
        "name": "Technical Product / Program / Architecture",
        "slug": "product_tech",
        "priority": 70,
        "include_phrases": [
            "technical program manager",
            "technical product manager",
            "solutions architect",
            "enterprise architect",
            "it architect",
            "technical architect",
            "engineering manager",
            "director of engineering",
            "vp engineering",
            "chief technology officer",
            "cto",
            "cloud solutions architect",
            "aws solutions architect",
            "azure solutions architect",
        ],
        "exclude_phrases": [],
        "notes": "Covers MarketingRole: it-management-architecture",
    },

    # ── 8. Enterprise Platform / ERP ─────────────────────────────────────────
    # NEW CATEGORY — covers the 5 enterprise roles missing from original 7 buckets
    # Covers: servicenow-developer, salesforce-developer, sap-consultant,
    #         workday-consultant, oracle-consultant (MarketingRole)
    {
        "name": "Enterprise Platform / ERP",
        "slug": "enterprise_erp",
        "priority": 80,
        "include_phrases": [
            # ServiceNow
            "servicenow developer",
            "servicenow administrator",
            "servicenow architect",
            "servicenow consultant",
            "servicenow engineer",
            "now platform developer",
            "itsm developer",
            # Salesforce
            "salesforce developer",
            "salesforce administrator",
            "salesforce architect",
            "salesforce consultant",
            "salesforce engineer",
            "sfdc developer",
            "apex developer",
            "lwc developer",
            "lightning developer",
            "salesforce cpq",
            # SAP
            "sap consultant",
            "sap developer",
            "sap abap",
            "sap basis",
            "sap hana",
            "sap fiori",
            "s4hana",
            "sap mm",
            "sap sd",
            "sap fi",
            "sap successfactors",
            # Workday
            "workday consultant",
            "workday developer",
            "workday architect",
            "workday hcm",
            "workday integration",
            "workday studio",
            "workday reporting",
            # Oracle
            "oracle consultant",
            "oracle developer",
            "oracle dba",
            "oracle ebs",
            "oracle fusion",
            "oracle hcm",
            "oracle financials",
            "oracle cloud",
            "plsql developer",
            "pl sql developer",
            # Other ERP / Enterprise
            "peoplesoft developer",
            "peoplesoft consultant",
            "dynamics 365",
            "ms dynamics developer",
            "dynamics crm developer",
            "netsuite developer",
            "netsuite consultant",
        ],
        "exclude_phrases": [
            # Prevent matching non-tech sales roles
            "salesforce sales",
            "account executive",
            "business development",
        ],
        "notes": (
            "NEW — covers MarketingRole: servicenow-developer, salesforce-developer, "
            "sap-consultant, workday-consultant, oracle-consultant"
        ),
    },

    # ── 9. Healthcare IT / Clinical Informatics ───────────────────────────────
    # NEW CATEGORY — covers the healthcare IT roles missing from original 7 buckets
    # Covers: healthcare-it (MarketingRole)
    {
        "name": "Healthcare IT / Clinical Informatics",
        "slug": "healthcare_it",
        "priority": 90,
        "include_phrases": [
            "healthcare it",
            "health information technology",
            "ehr analyst",
            "emr analyst",
            "epic analyst",
            "epic developer",
            "epic builder",
            "epic implementation",
            "cerner analyst",
            "cerner developer",
            "meditech analyst",
            "clinical informatics",
            "health informatics",
            "clinical systems analyst",
            "healthcare analyst",
            "hl7 developer",
            "fhir developer",
            "his analyst",
            "healthcare data analyst",
            "himss",
            "ehr implementation",
        ],
        "exclude_phrases": [
            # Clinical/bedside roles that contain "clinical" or "health" but aren't IT
            "clinical nurse",
            "clinical pharmacist",
            "clinical coordinator",
            "clinical research coordinator",
            "clinical trials",
            "pharmacy technician",
            "nursing informatics",
        ],
        "notes": "NEW — covers MarketingRole: healthcare-it",
    },
]

# ── IT Support — add to Security/Network or its own bucket? ──────────────────
# it-support MarketingRole is covered by Security/InfoSec/Network via
# "systems administrator" / "sysadmin" phrases already.
# If you want explicit IT support phrases, add them there.
IT_SUPPORT_PHRASES_TO_ADD_TO_SECURITY = [
    "it support engineer",
    "help desk engineer",
    "service desk engineer",
    "desktop support engineer",
    "end user support engineer",
    "it technician",
    "field support engineer",
    "deskside support engineer",
]


class Command(BaseCommand):
    help = (
        "Upsert the 9 canonical HarvestRoleCategory rows.  "
        "Safe to run multiple times — uses update_or_create on slug.  "
        "Only ADDS missing phrases, never removes user customisations."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            default=False,
            help="Write changes to DB.  Without this flag, preview only.",
        )
        parser.add_argument(
            "--add-it-support",
            action="store_true",
            default=False,
            help="Also add IT support phrases to the Security/InfoSec/Network category.",
        )

    def handle(self, *args, **options):
        from harvest.models import HarvestRoleCategory

        apply = options["apply"]
        label = "APPLY" if apply else "DRY-RUN"
        self.stdout.write(self.style.WARNING(f"\n[{label}] seed_harvest_categories\n"))

        for defn in CATEGORIES:
            slug = defn["slug"]
            try:
                cat = HarvestRoleCategory.objects.get(slug=slug)
                existing_includes = set(cat.include_phrases or [])
                existing_excludes = set(cat.exclude_phrases or [])
                new_includes = [p for p in defn["include_phrases"] if p not in existing_includes]
                new_excludes = [p for p in defn["exclude_phrases"] if p not in existing_excludes]

                if not new_includes and not new_excludes:
                    self.stdout.write(f"  ✓ [{slug}] up to date ({len(existing_includes)} include phrases)")
                else:
                    self.stdout.write(self.style.WARNING(f"  ~ [{slug}] would add:"))
                    for p in new_includes:
                        self.stdout.write(f"      + include: {p!r}")
                    for p in new_excludes:
                        self.stdout.write(f"      + exclude: {p!r}")
                    if apply:
                        cat.include_phrases = sorted(existing_includes | set(defn["include_phrases"]))
                        cat.exclude_phrases = sorted(existing_excludes | set(defn["exclude_phrases"]))
                        cat.save(update_fields=["include_phrases", "exclude_phrases", "updated_at"])
                        self.stdout.write(self.style.SUCCESS(f"    → saved"))

            except HarvestRoleCategory.DoesNotExist:
                self.stdout.write(self.style.SUCCESS(f"  + [{slug}] NEW — {defn['name']}"))
                if apply:
                    HarvestRoleCategory.objects.create(
                        name=defn["name"],
                        slug=slug,
                        priority=defn["priority"],
                        include_phrases=defn["include_phrases"],
                        exclude_phrases=defn["exclude_phrases"],
                        notes=defn.get("notes", ""),
                        is_active=True,
                    )
                    self.stdout.write(self.style.SUCCESS(f"    → created"))

        # IT support phrases to Security/InfoSec/Network
        if options["add_it_support"]:
            try:
                sec = HarvestRoleCategory.objects.get(slug="security")
                existing = set(sec.include_phrases or [])
                to_add = [p for p in IT_SUPPORT_PHRASES_TO_ADD_TO_SECURITY if p not in existing]
                if to_add:
                    self.stdout.write(self.style.WARNING(f"\n  ~ [security] adding IT support phrases:"))
                    for p in to_add:
                        self.stdout.write(f"      + include: {p!r}")
                    if apply:
                        sec.include_phrases = sorted(existing | set(IT_SUPPORT_PHRASES_TO_ADD_TO_SECURITY))
                        sec.save(update_fields=["include_phrases", "updated_at"])
                        self.stdout.write(self.style.SUCCESS(f"    → saved"))
                else:
                    self.stdout.write(f"  ✓ [security] IT support phrases already present")
            except HarvestRoleCategory.DoesNotExist:
                self.stdout.write(self.style.ERROR("  ✗ security category not found — run without --add-it-support first"))

        self.stdout.write("")
        if apply:
            self.stdout.write(self.style.SUCCESS("Done."))
        else:
            self.stdout.write(self.style.NOTICE("Preview only.  Run with --apply to write to DB."))
