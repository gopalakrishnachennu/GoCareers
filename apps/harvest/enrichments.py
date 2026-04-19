"""
Job posting enrichment — extract 20+ structured signals from free-text fields.

No external NLP dependencies — pure regex + keyword matching. Fast enough to
run on every ingest (Jarvis) and in the background backfill task.

Usage:
    from harvest.enrichments import extract_enrichments
    enriched = extract_enrichments(job_dict)  # job_dict has title/description/etc.
    # enriched is a plain dict — merge into your RawJob.update_fields
"""
from __future__ import annotations

import re
from typing import Optional

# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    """Remove HTML tags and decode common entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&nbsp;", " ").replace("&#39;", "'").replace("&quot;", '"')
    return text


# ─────────────────────────────────────────────────────────────────────────────
# 1. TECH SKILLS vocabulary
# ─────────────────────────────────────────────────────────────────────────────

TECH_SKILLS: set[str] = {
    # ── Languages ──────────────────────────────────────────────────────────
    "Python", "JavaScript", "TypeScript", "Java", "C++", "C#", "C", "Go",
    "Rust", "Swift", "Kotlin", "Scala", "Ruby", "PHP", "R", "MATLAB", "Perl",
    "Haskell", "Elixir", "Erlang", "Clojure", "Groovy", "Julia", "Lua", "Dart",
    "Objective-C", "COBOL", "Fortran", "Bash", "Shell", "PowerShell",
    "VBA", "SQL", "PL/SQL", "T-SQL",
    # ── Frontend ───────────────────────────────────────────────────────────
    "React", "Vue", "Angular", "Next.js", "Nuxt.js", "Svelte",
    "HTML", "CSS", "SCSS", "Sass", "Tailwind", "Bootstrap", "jQuery",
    "Redux", "GraphQL", "REST", "WebSockets", "Three.js", "D3.js",
    # ── Backend ────────────────────────────────────────────────────────────
    "Node.js", "Express", "FastAPI", "Django", "Flask",
    "Spring Boot", "Rails", "Laravel", "ASP.NET", ".NET",
    "gRPC", "Microservices", "Serverless", "NestJS",
    # ── Data / ML / AI ─────────────────────────────────────────────────────
    "TensorFlow", "PyTorch", "Keras", "scikit-learn", "Pandas", "NumPy",
    "Spark", "PySpark", "Hadoop", "Kafka", "Flink", "Airflow", "dbt",
    "MLflow", "Kubeflow", "Ray", "Dask", "Polars",
    "LLM", "GPT", "BERT", "Transformers", "OpenAI", "LangChain", "RAG",
    "Computer Vision", "NLP", "Deep Learning", "Machine Learning",
    # ── Databases ──────────────────────────────────────────────────────────
    "PostgreSQL", "MySQL", "SQLite", "MongoDB", "Redis", "Elasticsearch",
    "Cassandra", "DynamoDB", "Snowflake", "BigQuery", "Redshift", "Databricks",
    "Neo4j", "InfluxDB", "Oracle", "SQL Server", "MariaDB", "CockroachDB",
    "Pinecone", "Weaviate", "ChromaDB", "Supabase", "PlanetScale",
    # ── Cloud ──────────────────────────────────────────────────────────────
    "AWS", "GCP", "Azure", "Kubernetes", "Docker", "Terraform", "Helm",
    "Ansible", "Pulumi", "CloudFormation", "CDK",
    "Lambda", "EC2", "S3", "ECS", "EKS", "GKE", "AKS", "Cloud Run",
    # ── DevOps / CI/CD ──────────────────────────────────────────────────────
    "CI/CD", "GitHub Actions", "Jenkins", "CircleCI", "GitLab CI", "ArgoCD",
    "Prometheus", "Grafana", "Datadog", "New Relic", "Splunk",
    "PagerDuty", "ELK", "Logstash", "Kibana", "OpenTelemetry",
    # ── Mobile ─────────────────────────────────────────────────────────────
    "iOS", "Android", "React Native", "Flutter", "Xamarin",
    # ── Testing ────────────────────────────────────────────────────────────
    "Jest", "pytest", "JUnit", "Selenium", "Cypress", "Playwright",
    "TestNG", "Mocha", "RSpec", "Vitest",
    # ── Tools ──────────────────────────────────────────────────────────────
    "Git", "GitHub", "GitLab", "Bitbucket", "Jira", "Confluence",
    "Figma", "Sketch", "Notion", "Linear",
    # ── APIs / Security ─────────────────────────────────────────────────────
    "OpenAPI", "Swagger", "OAuth", "JWT", "SAML", "LDAP", "WebRTC",
    "SIEM", "SOC", "IAM", "Zero Trust", "OWASP", "PKI",
    # ── Blockchain ─────────────────────────────────────────────────────────
    "Ethereum", "Solidity", "Web3", "Blockchain",
    # ── Analytics tools ────────────────────────────────────────────────────
    "Tableau", "Power BI", "Looker", "dbt", "Metabase", "Amplitude",
    "Mixpanel", "Segment", "Fivetran", "Airbyte",
    # ── Salesforce / CRM ───────────────────────────────────────────────────
    "Salesforce", "HubSpot", "Marketo", "Pardot", "ServiceNow",
}

# Canonical lookup: lowercase → canonical spelling
_TECH_LOWER: dict[str, str] = {s.lower(): s for s in TECH_SKILLS}

# Aliases: common abbreviations / alternate spellings → canonical
_TECH_ALIASES: dict[str, str] = {
    "node": "Node.js", "nodejs": "Node.js", "react.js": "React",
    "vue.js": "Vue", "angular.js": "Angular",
    "golang": "Go", "k8s": "Kubernetes", "k8": "Kubernetes",
    "postgres": "PostgreSQL", "mongo": "MongoDB",
    "elastic": "Elasticsearch", "es": "Elasticsearch",
    "pyspark": "PySpark", "sklearn": "scikit-learn",
    "ml": "Machine Learning", "dl": "Deep Learning",
    "llms": "LLM", "large language model": "LLM",
    "gen ai": "Machine Learning", "generative ai": "Machine Learning",
    "gpt-4": "GPT", "gpt4": "GPT", "chatgpt": "GPT",
    "ci/cd": "CI/CD", "cicd": "CI/CD",
    "typescript": "TypeScript", "javascript": "JavaScript",
    "c sharp": "C#", "csharp": "C#",
    "dotnet": ".NET", "dot net": ".NET",
    "spring": "Spring Boot", "springboot": "Spring Boot",
    "tf": "Terraform", "ansible playbook": "Ansible",
    "aws lambda": "Lambda", "amazon s3": "S3",
    "google bigquery": "BigQuery", "azure ml": "Azure",
}
_TECH_LOWER.update(_TECH_ALIASES)


# ─────────────────────────────────────────────────────────────────────────────
# 2. SOFT SKILLS / methodologies
# ─────────────────────────────────────────────────────────────────────────────

SOFT_SKILLS: set[str] = {
    "Agile", "Scrum", "Kanban", "SDLC", "SAFe",
    "Communication", "Leadership", "Problem Solving", "Critical Thinking",
    "Collaboration", "Teamwork", "Project Management",
    "Cross-functional", "Stakeholder Management",
    "Data Analysis", "Business Analysis", "Product Management",
    "Account Management", "Customer Success",
    "SEO", "SEM", "Google Analytics",
    "Excel", "Six Sigma", "Lean", "PMP", "ITIL",
    "CISSP", "CPA", "CFA", "CISM",
}
_SOFT_LOWER: dict[str, str] = {s.lower(): s for s in SOFT_SKILLS}


# ─────────────────────────────────────────────────────────────────────────────
# 3. BENEFIT patterns
# ─────────────────────────────────────────────────────────────────────────────

BENEFIT_PATTERNS: dict[str, str] = {
    "Health Insurance":     r"\b(health|medical|dental|vision|healthcare)\s*(insurance|coverage|benefits?|plan)?\b",
    "401(k) / Pension":     r"\b(401[(\s]?k|retirement\s*(plan|savings)|pension|rrsp|superannuation)\b",
    "Equity / RSUs":        r"\b(equity|rsu|stock\s*(options?|grants?|awards?|units?)|esop|espp)\b",
    "Unlimited PTO":        r"\bunlimited\s*(pto|vacation|time\s*off)\b",
    "PTO / Vacation":       r"\b(pto|paid\s*time\s*off|vacation\s*days?|holidays?|sick\s*(days?|leave))\b",
    "Remote / WFH":         r"\b(remote\s*(work|friendly|first|option)|work\s*from\s*home|wfh|distributed\s*team)\b",
    "Flexible Hours":       r"\b(flex(ible)?\s*(hours?|schedule|time|work)|work.life\s*balance)\b",
    "Parental Leave":       r"\b(parental|maternity|paternity|family)\s*(leave|benefits?)\b",
    "Learning Budget":      r"\b(learning|education|training|conference|certification|tuition)\s*(budget|stipend|reimburse|allowance|assist)\b",
    "Wellness Program":     r"\b(wellness|gym|fitness|mental\s*health|therapy|eap|employee\s*assist)\s*(program|benefit|stipend|membership)?\b",
    "Signing Bonus":        r"\bsigning\s*bonus\b",
    "Performance Bonus":    r"\b(performance|annual|quarterly)\s*bonus\b",
    "Home Office Stipend":  r"\bhome\s*office\s*(allowance|stipend|budget|reimburse)\b",
    "Commuter Benefits":    r"\b(commuter|transit|transportation|parking)\s*(benefits?|allowance|reimburse|pass)?\b",
    "Life / Disability Ins":r"\b(life\s*insurance|disability\s*insurance|long.term\s*disability)\b",
    "Free Food / Meals":    r"\b(free\s*(lunch|meals?|snacks?|food|breakfast|coffee)|catered|cafeteria)\b",
    "Relocation Assistance":r"\b(relocation\s*(assist|support|package|allowance|reimburse)|relo\s*package)\b",
    "Employee Discounts":   r"\b(employee\s*discount|product\s*discount|company\s*discount)\b",
    "Stock Purchase Plan":  r"\bespp\b",
    "Visa Sponsorship":     r"\bvisa\s*sponsor(ship)?\b",
}


# ─────────────────────────────────────────────────────────────────────────────
# 4. EDUCATION patterns (checked in priority order)
# ─────────────────────────────────────────────────────────────────────────────

_EDUCATION_PATTERNS: list[tuple[str, str]] = [
    ("PHD",       r"\b(phd|ph\.d|doctorate|doctoral\s*degree)\b"),
    ("MBA",       r"\b(mba|master\s*of\s*business\s*administration)\b"),
    ("MS",        r"\b(m\.?s\.?c?\.?|master'?s?\s*(degree|in|of)?|graduate\s*degree|post.?grad)\b"),
    ("BS",        r"\b(b\.?s\.?c?\.?|b\.?e\.?|b\.?tech\.?|bachelor'?s?\s*(degree|in|of)?|undergraduate\s*degree)\b"),
    ("ASSOCIATE", r"\b(associate'?s?\s*degree|a\.a\.?|a\.s\.?)\b"),
    ("HS",        r"\b(high\s*school\s*diploma|ged)\b"),
]


# ─────────────────────────────────────────────────────────────────────────────
# 5. YEARS OF EXPERIENCE regex
# ─────────────────────────────────────────────────────────────────────────────

_YEARS_RE = re.compile(
    r"""
    (?:
      (\d{1,2})\+?\s*(?:[-–to]\s*(\d{1,2}))?\s*   # "5" or "5-8" or "5+"
      years?\s*(?:of\s+)?
      (?:professional\s+)?(?:relevant\s+)?(?:hands.on\s+)?
      (?:prior\s+)?(?:work\s+)?(?:industry\s+)?
      experience
    )
    |
    (?:
      experience\s+of\s+(\d{1,2})\+?\s*years?
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


# ─────────────────────────────────────────────────────────────────────────────
# 6. VISA / SPONSORSHIP patterns
# ─────────────────────────────────────────────────────────────────────────────

_VISA_YES_RE = re.compile(
    r"\b(visa\s*sponsor(ship|ed)?|will\s*sponsor|sponsors?\s*visa"
    r"|h.?1b?\s*sponsor|immigration\s*support|work\s*permit\s*support"
    r"|offer\s*visa\s*sponsor|provide\s*visa\s*sponsor)\b",
    re.IGNORECASE,
)
_VISA_NO_RE = re.compile(
    r"\b(no\s*visa\s*sponsor|not\s*sponsor|unable\s*to\s*sponsor"
    r"|cannot\s*sponsor|does\s*not\s*(provide|offer)\s*sponsor"
    r"|sponsorship\s*(?:is\s*)?not\s*(?:available|provided|offered)"
    r"|without\s*sponsorship|must\s*not\s*require\s*sponsorship)\b",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# 7. WORK AUTHORIZATION
# ─────────────────────────────────────────────────────────────────────────────

_AUTH_PATTERNS: list[tuple[str, str]] = [
    ("US citizens only",  r"\b(us\s*citizens?\s*only|authorized\s*to\s*work\s*in\s*the\s*u\.?s\.?|must\s*be\s*(a\s*)?us\s*citizen)\b"),
    ("US persons",        r"\b(us\s*persons?|itar|ear\s*controlled|export\s*control)\b"),
    ("No sponsorship",    r"\b(must\s*not\s*require\s*sponsorship|without\s*sponsorship)\b"),
]


# ─────────────────────────────────────────────────────────────────────────────
# 8. SECURITY CLEARANCE
# ─────────────────────────────────────────────────────────────────────────────

_CLEARANCE_RE = re.compile(
    r"\b(security\s*clearance|secret\s*clearance|top\s*secret|ts/?sci"
    r"|dod\s*clearance|nato\s*clearance|clearance\s*required"
    r"|must\s*(hold|have|obtain).{0,30}clearance|active\s*clearance)\b",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# 9. TRAVEL
# ─────────────────────────────────────────────────────────────────────────────

_TRAVEL_RE = re.compile(
    r"""
    (?:(?:up\s*to\s*|approximately\s*)?(\d{1,3})\s*%\s*travel)
    |(?:travel\s*(?:up\s*to\s*)?(\d{1,3})\s*%)
    |(?:(minimal|occasional|frequent|extensive|international|domestic|some|limited|moderate)\s*travel)
    """,
    re.IGNORECASE | re.VERBOSE,
)


# ─────────────────────────────────────────────────────────────────────────────
# 10. CERTIFICATIONS
# ─────────────────────────────────────────────────────────────────────────────

_CERT_PATTERNS: dict[str, str] = {
    "AWS Certified":       r"\b(aws\s*(certified|certification)|certified\s*aws|solutions?\s*architect|cloud\s*practitioner)\b",
    "GCP Certified":       r"\b(google\s*cloud\s*(certified|certification)|professional\s*cloud|gcp\s*(certified|certification))\b",
    "Azure Certified":     r"\b(microsoft\s*azure\s*(certified|certification)|az-\d{3}|azure\s*fundamentals)\b",
    "Kubernetes (CKA)":    r"\b(cka|ckad|ckss?|certified\s*kubernetes)\b",
    "PMP":                 r"\bpmp\b",
    "CISSP":               r"\bcissp\b",
    "CISA":                r"\bcisa\b",
    "CISM":                r"\bcism\b",
    "CEH":                 r"\bceh\b|\bethical\s*hack",
    "CPA":                 r"\bcpa\b",
    "CFA":                 r"\bcfa\b",
    "CCNA / CCNP":         r"\b(ccna|ccnp|ccie|cisco\s*certified)\b",
    "Scrum Master (CSM)":  r"\b(csm|psm|certified\s*scrum\s*master|professional\s*scrum\s*master)\b",
    "Six Sigma":           r"\b(six\s*sigma|lean\s*six\s*sigma|black\s*belt|green\s*belt)\b",
    "ITIL":                r"\bitil\b",
    "CompTIA":             r"\b(security\+|network\+|a\+|comptia)\b",
    "Salesforce Cert":     r"\bsalesforce\s*(certified|certification|admin|developer|architect)\b",
    "Terraform Associate": r"\bhashicorp\s*(certified|terraform\s*associate)\b",
    "Docker Certified":    r"\bdocker\s*(certified|dca)\b",
}


# ─────────────────────────────────────────────────────────────────────────────
# 11. JOB CATEGORY
# ─────────────────────────────────────────────────────────────────────────────

_CATEGORY_PATTERNS: list[tuple[str, str]] = [
    ("AI / ML",             r"\b(machine\s*learning|deep\s*learning|ai\s*engineer|nlp|computer\s*vision|research\s*scientist|llm|generative\s*ai|mlops|data\s*science)\b"),
    ("Data & Analytics",    r"\b(data\s*(scientist|analyst|analytics|warehouse|pipeline|platform)|bi\s*analyst|analytics\s*engineer|business\s*intelligence)\b"),
    ("Security",            r"\b(security\s*engineer|cybersecurity|information\s*security|soc\s*analyst|penetration|red\s*team|blue\s*team|ciso|appsec|devsecops)\b"),
    ("DevOps / SRE",        r"\b(devops|sre|site\s*reliability|platform\s*engineer|infrastructure|devsecops|cloud\s*engineer|systems?\s*engineer)\b"),
    ("Engineering",         r"\b(software\s*engineer|developer|backend|frontend|fullstack|full.stack|mobile\s*engineer|ios\s*engineer|android\s*engineer)\b"),
    ("Product",             r"\b(product\s*manager|product\s*owner|technical\s*program\s*manager|program\s*manager)\b"),
    ("Design",              r"\b(ux|ui\b|user\s*experience|user\s*interface|product\s*designer|graphic\s*design|visual\s*design)\b"),
    ("Marketing",           r"\b(marketing|growth\s*hack|seo|sem|content\s*market|demand\s*gen|brand\s*manager|copywriter|social\s*media)\b"),
    ("Sales",               r"\b(sales\s*(rep|executive|manager|engineer|development)|account\s*executive|business\s*development|sdr\b|bdr\b)\b"),
    ("Customer Success",    r"\b(customer\s*success|customer\s*support|account\s*manager|client\s*success|customer\s*experience|cx\b)\b"),
    ("Finance",             r"\b(finance|financial\s*analyst|accounting|accounts?\s*(payable|receivable)|controller|treasurer|bookkeeper|fp&a)\b"),
    ("HR & People",         r"\b(human\s*resources|hr\b|recruiter|talent\s*acquisition|people\s*ops|hrbp)\b"),
    ("Legal",               r"\b(lawyer|attorney|counsel|legal\s*(ops|analyst)|compliance|paralegal)\b"),
    ("Operations",          r"\b(operations\s*(manager|analyst|director)|supply\s*chain|logistics|procurement|biz\s*ops)\b"),
    ("Healthcare",          r"\b(nurse|physician|doctor|pharmacist|therapist|clinical|radiolog|health\s*care)\b"),
    ("Education",           r"\b(teacher|instructor|professor|curriculum|instructional\s*design|e.?learning)\b"),
]


# ─────────────────────────────────────────────────────────────────────────────
# 12. HUMAN LANGUAGES
# ─────────────────────────────────────────────────────────────────────────────

_LANGUAGE_PATTERNS: dict[str, str] = {
    "Spanish":    r"\b(spanish|español)\b",
    "French":     r"\b(french|français)\b",
    "German":     r"\b(german|deutsch)\b",
    "Mandarin":   r"\b(mandarin|cantonese)\b",
    "Japanese":   r"\bjapanese\b",
    "Portuguese": r"\bportuguese\b",
    "Arabic":     r"\barabic\b",
    "Korean":     r"\bkorean\b",
    "Italian":    r"\bitalian\b",
    "Dutch":      r"\bdutch\b",
    "Hindi":      r"\bhindi\b",
    "Russian":    r"\brussian\b",
    "Turkish":    r"\bturkish\b",
    "Polish":     r"\bpolish\b",
    "Vietnamese": r"\bvietnamese\b",
    "Hebrew":     r"\bhebrew\b",
    "Thai":       r"\bthai\b",
    "Swedish":    r"\bswedish\b",
    "Norwegian":  r"\bnorwegian\b",
    "Danish":     r"\bdanish\b",
    "Finnish":    r"\bfinnish\b",
}

# Language requirements signal phrases — must appear near the language name
_LANG_REQUIRED_CTX = re.compile(
    r"\b(require[sd]?|must|fluent|proficient|bilingual|native|business\s*(level|proficiency)"
    r"|professional\s*(level|proficiency)|strong|spoken|written|verbal|reading)\b",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# 13. QUALITY SCORE
# ─────────────────────────────────────────────────────────────────────────────

_QUALITY_CHECKS: list[tuple[str, float]] = [
    ("title",            0.10),
    ("description",      0.25),
    ("location_raw",     0.10),
    ("employment_type",  0.05),
    ("experience_level", 0.05),
    ("salary_raw",       0.12),
    ("company_name",     0.10),
    ("requirements",     0.08),
    ("benefits",         0.05),
    ("posted_date",      0.05),
    ("department",       0.05),
]

def _quality_score(job: dict) -> float:
    score = 0.0
    for field, weight in _QUALITY_CHECKS:
        val = job.get(field)
        if val and val not in ("UNKNOWN", "", None, 0):
            score += weight
    return round(min(score, 1.0), 2)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_enrichments(job: dict) -> dict:
    """
    Extract structured signals from a normalized job dict.

    Input keys used: title, description, requirements, benefits, department,
    location_raw, employment_type, experience_level, salary_raw, company_name,
    posted_date.

    Returns a dict with ~20 enrichment fields ready to .update() onto a RawJob.
    """
    title       = (job.get("title") or "")
    description = (job.get("description") or "")
    requirements = (job.get("requirements") or "")
    benefits    = (job.get("benefits") or "")

    # Build clean plain-text versions of each section
    title_c  = _strip_html(title).lower()
    desc_c   = _strip_html(description).lower()
    req_c    = _strip_html(requirements).lower()
    ben_c    = _strip_html(benefits).lower()

    # Full combined text for most checks
    full_c   = f"{title_c} {desc_c} {req_c} {ben_c}"

    # ── 1. Tech skills ────────────────────────────────────────────────────────
    found_tech: set[str] = set()
    for lower, canonical in _TECH_LOWER.items():
        if " " in lower:
            # multi-word: simple substring check
            if lower in full_c:
                found_tech.add(canonical)
        else:
            # single word: whole-word boundary
            if re.search(r"(?<![a-z])" + re.escape(lower) + r"(?![a-z])", full_c):
                found_tech.add(canonical)

    # ── 2. Soft skills ────────────────────────────────────────────────────────
    found_soft: set[str] = set()
    for lower, canonical in _SOFT_LOWER.items():
        if lower in full_c:
            found_soft.add(canonical)

    all_skills = sorted(found_tech | found_soft)
    tech_stack = sorted(found_tech)

    # ── 3. Years of experience ────────────────────────────────────────────────
    years_min: Optional[int] = None
    years_max: Optional[int] = None
    for m in _YEARS_RE.finditer(full_c):
        g1, g2, g3 = m.group(1), m.group(2), m.group(3)
        if g1:
            y = int(g1)
            if years_min is None or y < years_min:
                years_min = y
            if g2:
                y2 = int(g2)
                if years_max is None or y2 > years_max:
                    years_max = y2
        elif g3:
            y = int(g3)
            if years_min is None or y < years_min:
                years_min = y

    # ── 4. Education ─────────────────────────────────────────────────────────
    education = ""
    for level, pattern in _EDUCATION_PATTERNS:
        if re.search(pattern, full_c):
            education = level
            break

    # ── 5. Visa sponsorship ───────────────────────────────────────────────────
    visa_sponsorship: Optional[bool] = None
    full_raw = f"{title} {description} {requirements} {benefits}".lower()
    if _VISA_YES_RE.search(full_raw):
        visa_sponsorship = True
    if _VISA_NO_RE.search(full_raw):
        visa_sponsorship = False   # "no" always overrides "yes"

    # ── 6. Work authorization ─────────────────────────────────────────────────
    work_authorization = ""
    for label, pattern in _AUTH_PATTERNS:
        if re.search(pattern, full_c):
            work_authorization = label
            break

    # ── 7. Equity ─────────────────────────────────────────────────────────────
    salary_equity = bool(re.search(
        r"\b(equity|rsu|stock\s*option|esop|espp|share\s*grant|restricted\s*stock)\b", full_c
    ))

    # ── 8. Relocation ─────────────────────────────────────────────────────────
    relocation = bool(re.search(
        r"\b(relocation\s*(assist|support|package|allowance|reimburse|bonus|provided)?|relo\s*package|we\s*support\s*relocation)\b",
        full_c,
    ))

    # ── 9. Signing bonus ──────────────────────────────────────────────────────
    signing_bonus = bool(re.search(r"\bsigning\s*bonus\b", full_c))

    # ── 10. Security clearance ────────────────────────────────────────────────
    clearance = bool(_CLEARANCE_RE.search(full_raw))

    # ── 11. Travel ────────────────────────────────────────────────────────────
    travel = ""
    m = _TRAVEL_RE.search(full_c)
    if m:
        if m.group(1):
            travel = f"up to {m.group(1)}%"
        elif m.group(2):
            travel = f"up to {m.group(2)}%"
        elif m.group(3):
            travel = m.group(3).lower()

    # ── 12. Certifications ────────────────────────────────────────────────────
    certs: list[str] = []
    for name, pattern in _CERT_PATTERNS.items():
        if re.search(pattern, full_c):
            certs.append(name)

    # ── 13. Benefits list ─────────────────────────────────────────────────────
    benefits_found: list[str] = []
    for name, pattern in BENEFIT_PATTERNS.items():
        if re.search(pattern, full_c, re.IGNORECASE):
            benefits_found.append(name)

    # ── 14. Job category ──────────────────────────────────────────────────────
    category = ""
    dept = (job.get("department") or "").lower()
    title_dept = f"{title_c} {dept}"
    for name, pattern in _CATEGORY_PATTERNS:
        if re.search(pattern, title_dept):
            category = name
            break
    if not category:
        for name, pattern in _CATEGORY_PATTERNS:
            if re.search(pattern, desc_c):
                category = name
                break

    # ── 15. Human languages ───────────────────────────────────────────────────
    langs: list[str] = []
    # Focus on requirements + description; context signals needed
    lang_text = f"{req_c} {desc_c}"
    for lang, pattern in _LANGUAGE_PATTERNS.items():
        m_lang = re.search(pattern, lang_text, re.IGNORECASE)
        if m_lang:
            # Check nearby context for requirement signal (within 200 chars)
            start = max(0, m_lang.start() - 150)
            end   = min(len(lang_text), m_lang.end() + 150)
            ctx   = lang_text[start:end]
            if _LANG_REQUIRED_CTX.search(ctx):
                langs.append(lang)

    # ── 16. Word count ────────────────────────────────────────────────────────
    word_count = len(_strip_html(description).split())

    # ── 17. Quality score ─────────────────────────────────────────────────────
    quality = _quality_score(job)

    return {
        # Skills
        "skills":               all_skills,
        "tech_stack":           tech_stack,
        "job_category":         category,
        # Experience
        "years_required":       years_min,
        "years_required_max":   years_max,
        "education_required":   education,
        # Legal / visa
        "visa_sponsorship":     visa_sponsorship,
        "work_authorization":   work_authorization,
        "clearance_required":   clearance,
        # Compensation extras
        "salary_equity":        salary_equity,
        "signing_bonus":        signing_bonus,
        "relocation_assistance": relocation,
        # Work conditions
        "travel_required":      travel,
        # Structured lists
        "certifications":       certs,
        "benefits_list":        benefits_found,
        "languages_required":   langs,
        # Quality
        "word_count":           word_count,
        "quality_score":        quality,
    }
