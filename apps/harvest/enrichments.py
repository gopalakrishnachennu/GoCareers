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

import html as _html
import re
from typing import Optional

# ── Helpers ───────────────────────────────────────────────────────────────────

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HTML_SCRIPT_STYLE_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
_BROKEN_UNICODE_RE = re.compile(r"[\u0000-\u0008\u000b-\u001f\u007f]")


def _strip_html(text: str) -> str:
    """Remove heavy HTML noise while preserving section breaks and bullets."""
    src = str(text or "")
    src = _HTML_SCRIPT_STYLE_RE.sub(" ", src)
    src = _HTML_COMMENT_RE.sub(" ", src)
    src = re.sub(r"</(p|div|li|h[1-6]|tr|br|section|article|ul|ol)>", "\n", src, flags=re.I)
    src = re.sub(r"<li[^>]*>", "\n• ", src, flags=re.I)
    src = _HTML_TAG_RE.sub(" ", src)
    src = _html.unescape(src)
    src = _BROKEN_UNICODE_RE.sub(" ", src)
    return src


def normalize_job_title(title: str) -> str:
    """Normalize noisy titles into a stable canonical title string."""
    txt = clean_job_text(title or "", max_len=255)
    if not txt:
        return ""
    txt = re.sub(r"\s*[|/\\-]\s*(remote|hybrid|onsite|on-site)\b", "", txt, flags=re.I)
    txt = re.sub(r"\b(req(uisition)?\s*#?\s*\d+|job\s*id[:\s#-]*\w+)\b", "", txt, flags=re.I)
    txt = re.sub(r"\s+", " ", txt).strip(" -|,/")
    return txt[:255]


def clean_job_content(text: str, *, max_len: int | None = None) -> dict:
    """
    Return cleaned text + metadata used for JD quality and audit columns.
    """
    raw = str(text or "")
    has_html = bool(re.search(r"<[^>]+>", raw))
    stripped = _strip_html(raw)
    cleaned = re.sub(r"[ \t]+", " ", stripped)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = cleaned.strip()
    if max_len and max_len > 0:
        cleaned = cleaned[:max_len]
    raw_len = len(raw.strip())
    clean_len = len(cleaned)
    jd_quality = 0.0
    if clean_len > 0:
        length_signal = min(clean_len / 1200.0, 1.0) * 0.6
        structure_signal = (0.25 if "\n" in cleaned else 0.0) + (0.15 if has_html else 0.05)
        jd_quality = round(min(1.0, length_signal + structure_signal), 3)
    return {
        "clean_text": cleaned,
        "raw_html": raw if has_html else "",
        "has_html_content": has_html,
        "cleaning_version": "v2",
        "jd_quality_score": jd_quality,
    }


def clean_job_text(text: str, *, max_len: int | None = None) -> str:
    """
    Normalize scraped content into clean plain text:
    - strip HTML
    - collapse whitespace/newlines
    - optionally clamp length
    """
    return clean_job_content(text, max_len=max_len)["clean_text"]


# ─────────────────────────────────────────────────────────────────────────────
# Location / country inference
# ─────────────────────────────────────────────────────────────────────────────

_COUNTRY_ALIASES: dict[str, str] = {
    "us": "United States",
    "usa": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "united states": "United States",
    "united states of america": "United States",
    "ca": "Canada",
    "can": "Canada",
    "canada": "Canada",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "gb": "United Kingdom",
    "gbr": "United Kingdom",
    "united kingdom": "United Kingdom",
    "england": "United Kingdom",
    "scotland": "United Kingdom",
    "wales": "United Kingdom",
    "ie": "Ireland",
    "irl": "Ireland",
    "ireland": "Ireland",
    "au": "Australia",
    "aus": "Australia",
    "australia": "Australia",
    "nz": "New Zealand",
    "nzl": "New Zealand",
    "new zealand": "New Zealand",
    "de": "Germany",
    "deu": "Germany",
    "germany": "Germany",
    "fr": "France",
    "fra": "France",
    "france": "France",
    "es": "Spain",
    "esp": "Spain",
    "spain": "Spain",
    "it": "Italy",
    "ita": "Italy",
    "italy": "Italy",
    "nl": "Netherlands",
    "nld": "Netherlands",
    "netherlands": "Netherlands",
    "be": "Belgium",
    "bel": "Belgium",
    "belgium": "Belgium",
    "se": "Sweden",
    "swe": "Sweden",
    "sweden": "Sweden",
    "no": "Norway",
    "nor": "Norway",
    "norway": "Norway",
    "dk": "Denmark",
    "dnk": "Denmark",
    "denmark": "Denmark",
    "fi": "Finland",
    "fin": "Finland",
    "finland": "Finland",
    "ch": "Switzerland",
    "che": "Switzerland",
    "switzerland": "Switzerland",
    "at": "Austria",
    "aut": "Austria",
    "austria": "Austria",
    "pl": "Poland",
    "pol": "Poland",
    "poland": "Poland",
    "pt": "Portugal",
    "prt": "Portugal",
    "portugal": "Portugal",
    "gr": "Greece",
    "grc": "Greece",
    "greece": "Greece",
    "in": "India",
    "ind": "India",
    "india": "India",
    "sg": "Singapore",
    "sgp": "Singapore",
    "singapore": "Singapore",
    "my": "Malaysia",
    "mys": "Malaysia",
    "malaysia": "Malaysia",
    "ph": "Philippines",
    "phl": "Philippines",
    "philippines": "Philippines",
    "vn": "Vietnam",
    "vnm": "Vietnam",
    "vietnam": "Vietnam",
    "id": "Indonesia",
    "idn": "Indonesia",
    "indonesia": "Indonesia",
    "th": "Thailand",
    "tha": "Thailand",
    "thailand": "Thailand",
    "jp": "Japan",
    "jpn": "Japan",
    "japan": "Japan",
    "kr": "South Korea",
    "kor": "South Korea",
    "south korea": "South Korea",
    "cn": "China",
    "chn": "China",
    "china": "China",
    "hk": "Hong Kong",
    "hkg": "Hong Kong",
    "hong kong": "Hong Kong",
    "tw": "Taiwan",
    "twn": "Taiwan",
    "taiwan": "Taiwan",
    "mx": "Mexico",
    "mex": "Mexico",
    "mexico": "Mexico",
    "br": "Brazil",
    "bra": "Brazil",
    "brazil": "Brazil",
    "ar": "Argentina",
    "arg": "Argentina",
    "argentina": "Argentina",
    "cl": "Chile",
    "chl": "Chile",
    "chile": "Chile",
    "co": "Colombia",
    "col": "Colombia",
    "colombia": "Colombia",
    "za": "South Africa",
    "zaf": "South Africa",
    "south africa": "South Africa",
    "ae": "United Arab Emirates",
    "are": "United Arab Emirates",
    "united arab emirates": "United Arab Emirates",
    "sa": "Saudi Arabia",
    "sau": "Saudi Arabia",
    "saudi arabia": "Saudi Arabia",
}

_US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
}
_US_STATE_NAMES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming", "district of columbia",
}


def _normalize_country_token(value: str, *, allow_passthrough: bool = False) -> str:
    token = (value or "").strip()
    if not token:
        return ""
    lower = token.lower().strip(" .")
    if lower in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[lower]
    clean = re.sub(r"[^a-zA-Z ]+", "", token).strip().lower()
    if clean in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[clean]
    if len(token) == 2 and token.isalpha():
        return _COUNTRY_ALIASES.get(token.lower(), "")
    if len(token) == 3 and token.isalpha():
        return _COUNTRY_ALIASES.get(token.lower(), "")
    return token[:128] if allow_passthrough else ""


def infer_country_from_location(location_raw: str, state: str = "", country: str = "") -> str:
    """
    Best-effort country detection from explicit country field + location string.
    Examples:
    - "US-PA-West Chester" -> United States
    - "Paris, France" -> France
    - state="TX" (with no country) -> United States
    """
    direct = _normalize_country_token(country, allow_passthrough=True)
    if direct:
        return direct

    state_s = (state or "").strip()
    state_key = state_s.lower()

    loc = clean_job_text(location_raw or "", max_len=512)
    loc_l = loc.lower()

    # Common ATS prefix format: "US-PA-..." / "CA-ON-..."
    m_pref = re.match(r"^\s*([A-Za-z]{2})-[A-Za-z]{2}(?:-|$)", loc)
    if m_pref:
        pref = _normalize_country_token(m_pref.group(1))
        if pref:
            return pref

    # Split location fragments and check likely country token first.
    parts = [p.strip() for p in re.split(r"[,|/]+", loc) if p and p.strip()]
    for p in reversed(parts):
        cand = _normalize_country_token(p)
        if cand:
            return cand

    # US state code/name embedded in location text (e.g., "Nashville, TN", "Boston-MA").
    if loc:
        upper_loc = f" {loc.upper().replace('-', ' ').replace('/', ' ')} "
        for code in _US_STATE_CODES:
            if f" {code} " in upper_loc:
                return "United States"
        lower_loc = f" {loc_l.replace('-', ' ').replace('/', ' ')} "
        for st_name in _US_STATE_NAMES:
            if f" {st_name} " in lower_loc:
                return "United States"

    # Fallback: direct mention anywhere in location text.
    for alias, canonical in sorted(_COUNTRY_ALIASES.items(), key=lambda kv: len(kv[0]), reverse=True):
        if re.search(rf"\b{re.escape(alias)}\b", loc_l):
            return canonical

    # Fallback from US state clue.
    state_upper = state_s.upper()
    if state_upper in _US_STATE_CODES or state_key in _US_STATE_NAMES:
        return "United States"

    return ""


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

_CLEARANCE_LEVEL_PATTERNS: list[tuple[str, str]] = [
    ("TS/SCI", r"\b(ts\/sci|top\s*secret\s*/\s*sci|top\s*secret\s*sci)\b"),
    ("Top Secret", r"\btop\s*secret\b"),
    ("Secret", r"\bsecret\s*clearance\b"),
    ("Public Trust", r"\bpublic\s*trust\b"),
    ("Confidential", r"\bconfidential\s*clearance\b"),
]


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
# 9.5 SHIFTS / SCHEDULES
# ─────────────────────────────────────────────────────────────────────────────

_SHIFT_PATTERNS: list[tuple[str, str]] = [
    ("Day shift", r"\b(day\s*shift|daytime|monday\s*to\s*friday|m-f)\b"),
    ("Night shift", r"\b(night\s*shift|overnight|graveyard)\b"),
    ("Weekend", r"\b(weekend|weekends)\b"),
    ("Rotational", r"\b(rotating|rotation|rotational)\b"),
    ("On-call", r"\b(on[\s-]*call|pager\s*duty|after[\s-]*hours)\b"),
    ("Flexible", r"\b(flexible\s*schedule|flexible\s*hours|flex\s*time)\b"),
]

_SCHEDULE_TYPE_PATTERNS: list[tuple[str, str]] = [
    ("full_time", r"\bfull[\s-]*time\b"),
    ("part_time", r"\bpart[\s-]*time\b"),
    ("contract", r"\b(contract|contractor|1099)\b"),
    ("internship", r"\b(intern|internship)\b"),
    ("shift", r"\b(day\s*shift|night\s*shift|swing\s*shift|rotating)\b"),
]


# ─────────────────────────────────────────────────────────────────────────────
# 9.6 ENCOURAGED TO APPLY
# ─────────────────────────────────────────────────────────────────────────────

_ENCOURAGED_PATTERNS: list[tuple[str, str]] = [
    ("Veterans", r"\b(veterans?\s*(are\s*)?(encouraged|welcome)|military\s*transition)\b"),
    ("Women", r"\b(women\s*(are\s*)?(encouraged|welcome))\b"),
    ("People with disabilities", r"\b(disabilit(y|ies)|reasonable\s*accommodation|pwd)\b"),
    ("Career changers", r"\b(career\s*changer|non[\s-]*traditional\s*background)\b"),
    ("Recent graduates", r"\b(new\s*grad|recent\s*graduate|entry\s*level\s*candidates?)\b"),
    ("Underrepresented groups", r"\b(underrepresented|diverse\s*backgrounds?|dei|equal\s*opportunity)\b"),
]


# ─────────────────────────────────────────────────────────────────────────────
# 9.7 DEPARTMENT NORMALIZATION
# ─────────────────────────────────────────────────────────────────────────────

_DEPARTMENT_PATTERNS: list[tuple[str, str]] = [
    ("Engineering", r"\b(engineering|software|developer|platform|infrastructure|sre|devops)\b"),
    ("Data", r"\b(data|analytics|bi\b|machine\s*learning|ai)\b"),
    ("Product", r"\b(product|program\s*management|technical\s*program)\b"),
    ("Design", r"\b(design|ux|ui|visual)\b"),
    ("Sales", r"\b(sales|account\s*executive|business\s*development|sdr|bdr)\b"),
    ("Marketing", r"\b(marketing|growth|seo|sem|brand|content)\b"),
    ("Customer Success", r"\b(customer\s*success|customer\s*support|customer\s*experience|cx\b)\b"),
    ("Finance", r"\b(finance|accounting|fp&a|controller|treasury)\b"),
    ("HR", r"\b(human\s*resources|people\s*ops|talent\s*acquisition|recruit)\b"),
    ("Legal", r"\b(legal|counsel|attorney|compliance|privacy)\b"),
    ("Operations", r"\b(operations|supply\s*chain|logistics|procurement)\b"),
]


# ─────────────────────────────────────────────────────────────────────────────
# 9.8 KEYWORD NORMALIZATION
# ─────────────────────────────────────────────────────────────────────────────

_TITLE_STOPWORDS = {
    "the", "and", "for", "with", "from", "into", "your", "our", "you",
    "job", "role", "team", "manager", "engineer", "developer", "specialist",
    "senior", "junior", "lead", "principal", "staff", "ii", "iii", "iv",
}


def _extract_title_keywords(title: str, skill_hits: list[str]) -> list[str]:
    title_words = []
    for token in re.findall(r"[a-zA-Z][a-zA-Z0-9.+#-]{2,}", title.lower()):
        if token in _TITLE_STOPWORDS:
            continue
        title_words.append(token)
    merged = title_words[:8] + [s.lower() for s in skill_hits[:8]]
    out: list[str] = []
    seen: set[str] = set()
    for item in merged:
        key = item.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out[:12]


def _normalize_department(raw_department: str, title: str, category: str) -> str:
    src = f"{raw_department or ''} {title or ''} {category or ''}".lower()
    for name, pattern in _DEPARTMENT_PATTERNS:
        if re.search(pattern, src):
            return name
    return (raw_department or "").strip()[:128]

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

_LICENSE_PATTERNS: dict[str, str] = {
    "Driver's License": r"\b(driver'?s?\s*license|valid\s*license)\b",
    "RN License": r"\b(rn|registered\s*nurse)\s*license\b",
    "PE License": r"\b(professional\s*engineer|pe\s*license)\b",
    "Medical License": r"\b(medical\s*license|board\s*certified)\b",
    "Bar License": r"\b(bar\s*admission|licensed\s*attorney)\b",
    "Teaching Credential": r"\b(teaching\s*credential|teaching\s*license)\b",
    "CPA License": r"\b(cpa\s*license|licensed\s*cpa)\b",
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

def extract_sections(description: str) -> dict[str, str]:
    """
    Extract requirements and responsibilities from a job description.

    Looks for section headers (Requirements, Qualifications, Responsibilities,
    What You'll Do, etc.) and returns the text under each section.

    Returns:
        {
          "requirements": "...",       # empty str if not found
          "responsibilities": "...",   # empty str if not found
          "benefits": "...",           # empty str if not found
        }
    """
    if not description:
        return {"requirements": "", "responsibilities": "", "benefits": ""}

    plain = _strip_html(description)

    # Section header patterns (case-insensitive)
    _REQ_HEADERS = re.compile(
        r"(?:^|\n)\s*(?:##?\s*)?"
        r"(?:requirements?|qualifications?|required\s+skills?|what\s+you(?:'ll|'d|\s+will)\s+bring"
        r"|what\s+we(?:'re|\s+are)\s+looking\s+for|minimum\s+qualifications?"
        r"|preferred\s+qualifications?|you\s+(?:have|bring|possess))"
        r"\s*[:\-–]?\s*\n",
        re.I | re.M,
    )
    _RESP_HEADERS = re.compile(
        r"(?:^|\n)\s*(?:##?\s*)?"
        r"(?:responsibilities?|what\s+you(?:'ll|'d|\s+will)\s+do|role\s+overview"
        r"|key\s+responsibilities?|your\s+(?:role|responsibilities?|impact)"
        r"|duties|the\s+role|about\s+the\s+role|what\s+you(?:'ll|'d|\s+will)\s+(?:own|work|be))"
        r"\s*[:\-–]?\s*\n",
        re.I | re.M,
    )
    _BENEFITS_HEADERS = re.compile(
        r"(?:^|\n)\s*(?:##?\s*)?"
        r"(?:benefits?|perks?|what\s+we\s+offer|we\s+offer|compensation|what\s+you(?:'ll|'d|\s+will)\s+get)"
        r"\s*[:\-–]?\s*\n",
        re.I | re.M,
    )
    # Any section-looking header to detect where the current section ends
    _ANY_SECTION = re.compile(
        r"(?:^|\n)\s*(?:##?\s*)?[A-Z][A-Za-z ]{3,40}\s*[:\-–]\s*\n",
        re.M,
    )

    def _extract_after(header_pattern: re.Pattern, text: str) -> str:
        m = header_pattern.search(text)
        if not m:
            return ""
        start = m.end()
        # Find next section header after our match
        next_section = _ANY_SECTION.search(text, start)
        end = next_section.start() if next_section else min(start + 2000, len(text))
        section_text = text[start:end].strip()
        # Only return if it looks like real content (≥40 chars, has letters)
        if len(section_text) >= 40 and re.search(r"[a-zA-Z]{4}", section_text):
            return section_text[:2000]
        return ""

    return {
        "requirements":    _extract_after(_REQ_HEADERS, plain),
        "responsibilities": _extract_after(_RESP_HEADERS, plain),
        "benefits":        _extract_after(_BENEFITS_HEADERS, plain),
    }


def _quality_score(job: dict) -> float:
    score = 0.0
    for field, weight in _QUALITY_CHECKS:
        val = job.get(field)
        if val and val not in ("UNKNOWN", "", None, 0):
            score += weight
    return round(min(score, 1.0), 2)


def _confidence_from_value(value) -> float:
    if value in (None, "", [], {}, False):
        return 0.0
    if isinstance(value, bool):
        return 0.75 if value else 0.4
    if isinstance(value, (int, float)):
        return 0.8
    if isinstance(value, list):
        return min(1.0, 0.45 + (0.12 * min(len(value), 4)))
    text = str(value).strip()
    if not text:
        return 0.0
    if len(text) < 4:
        return 0.5
    if len(text) < 15:
        return 0.7
    return 0.9


def _resume_ready_score(data: dict) -> float:
    checks: list[tuple[str, float]] = [
        ("description", 0.20),
        ("title", 0.12),
        ("country", 0.06),
        ("state", 0.04),
        ("salary_raw", 0.08),
        ("employment_type", 0.07),
        ("experience_level", 0.07),
        ("years_required", 0.05),
        ("education_required", 0.05),
        ("skills", 0.08),
        ("certifications", 0.04),
        ("languages_required", 0.03),
        ("clearance_level", 0.03),
        ("travel_required", 0.03),
        ("benefits_list", 0.03),
        ("job_category", 0.04),
        ("department_normalized", 0.03),
        ("company_name", 0.03),
    ]
    score = 0.0
    for field, weight in checks:
        if _confidence_from_value(data.get(field)) > 0:
            score += weight
    return round(min(score, 1.0), 3)


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
    raw_title = job.get("title") or ""
    title = clean_job_text(raw_title, max_len=512)
    normalized_title = normalize_job_title(raw_title or title)
    content_meta = clean_job_content(job.get("description") or "", max_len=50000)
    description = content_meta["clean_text"]

    # Auto-extract requirements/responsibilities from description if not already set.
    # Harvesters that don't parse these sections get them for free here.
    _existing_req = (job.get("requirements") or "").strip()
    _existing_resp = (job.get("responsibilities") or "").strip()
    if not _existing_req or not _existing_resp:
        _sections = extract_sections(job.get("description") or "")
        if not _existing_req:
            _existing_req = _sections.get("requirements", "")
        if not _existing_resp:
            _existing_resp = _sections.get("responsibilities", "")

    requirements = clean_job_text(_existing_req, max_len=20000)
    _raw_benefits = (job.get("benefits") or "").strip() or _sections.get("benefits", "")
    benefits = clean_job_text(_raw_benefits, max_len=10000)
    location_raw = clean_job_text(job.get("location_raw") or "", max_len=512)
    detected_country = infer_country_from_location(
        location_raw=location_raw,
        state=(job.get("state") or ""),
        country=(job.get("country") or ""),
    )

    # Build clean plain-text versions of each section
    title_c = title.lower()
    desc_c = description.lower()
    req_c = requirements.lower()
    ben_c = benefits.lower()

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
    travel_pct_min: Optional[int] = None
    travel_pct_max: Optional[int] = None
    m = _TRAVEL_RE.search(full_c)
    if m:
        if m.group(1):
            travel = f"up to {m.group(1)}%"
            travel_pct_max = int(m.group(1))
        elif m.group(2):
            travel = f"up to {m.group(2)}%"
            travel_pct_max = int(m.group(2))
        elif m.group(3):
            travel = m.group(3).lower()
    m_range = re.search(r"\b(\d{1,2})\s*[-–to]+\s*(\d{1,2})\s*%\s*travel\b", full_c)
    if m_range:
        travel_pct_min = int(m_range.group(1))
        travel_pct_max = int(m_range.group(2))
        travel = f"{travel_pct_min}-{travel_pct_max}%"

    # ── 11.5 Shift schedule ──────────────────────────────────────────────────
    shift_schedule = ""
    for label, pattern in _SHIFT_PATTERNS:
        if re.search(pattern, full_c):
            shift_schedule = label
            break
    schedule_type = ""
    for label, pattern in _SCHEDULE_TYPE_PATTERNS:
        if re.search(pattern, full_c):
            schedule_type = label
            break
    weekend_required: Optional[bool] = None
    if re.search(r"\b(weekend|weekends|required\s*on\s*weekends?)\b", full_c):
        weekend_required = True
    elif re.search(r"\b(no\s*weekends?|weekdays?\s*only)\b", full_c):
        weekend_required = False
    hours_hint = ""
    m_hours = re.search(r"\b(\d{1,2}\s*(am|pm)\s*[-–to]+\s*\d{1,2}\s*(am|pm)|\d{1,2}\s*hour\s*shifts?)\b", full_c)
    if m_hours:
        hours_hint = m_hours.group(1)[:64]
    shift_details = ", ".join(
        part for part in [shift_schedule, "weekend" if weekend_required else "", hours_hint] if part
    )[:255]

    # ── 12. Certifications ────────────────────────────────────────────────────
    certs: list[str] = []
    for name, pattern in _CERT_PATTERNS.items():
        if re.search(pattern, full_c):
            certs.append(name)
    licenses: list[str] = []
    for name, pattern in _LICENSE_PATTERNS.items():
        if re.search(pattern, full_c):
            licenses.append(name)

    # ── 13. Benefits list ─────────────────────────────────────────────────────
    benefits_found: list[str] = []
    for name, pattern in BENEFIT_PATTERNS.items():
        if re.search(pattern, full_c, re.IGNORECASE):
            benefits_found.append(name)

    # ── 14. Job category ──────────────────────────────────────────────────────
    category = ""
    _cat_title_match = False
    _cat_desc_match = False
    dept = (job.get("department") or "").lower()
    title_dept = f"{title_c} {dept}"
    for name, pattern in _CATEGORY_PATTERNS:
        if re.search(pattern, title_dept):
            category = name
            _cat_title_match = True
            break
    if not category:
        for name, pattern in _CATEGORY_PATTERNS:
            if re.search(pattern, desc_c):
                category = name
                _cat_desc_match = True
                break
    elif category:
        # Also check desc to see if they agree (boosts confidence)
        for name, pattern in _CATEGORY_PATTERNS:
            if re.search(pattern, desc_c):
                if name == category:
                    _cat_desc_match = True
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

    # ── 15.5 Encouraged to apply ─────────────────────────────────────────────
    encouraged: list[str] = []
    for label, pattern in _ENCOURAGED_PATTERNS:
        if re.search(pattern, full_c):
            encouraged.append(label)

    # ── 15.6 Title keywords ──────────────────────────────────────────────────
    title_keywords = _extract_title_keywords(title, all_skills)

    # ── 15.7 Department normalization ────────────────────────────────────────
    department_normalized = _normalize_department(job.get("department") or "", title, category)

    # ── 16. Word count ────────────────────────────────────────────────────────
    word_count = len(description.split())

    # ── 17. Quality score ─────────────────────────────────────────────────────
    quality = _quality_score(job)
    clearance_level = ""
    for level, pattern in _CLEARANCE_LEVEL_PATTERNS:
        if re.search(pattern, full_raw):
            clearance_level = level
            break
    if not clearance_level and clearance:
        clearance_level = "General clearance required"

    field_values_for_conf = {
        "job_category": category,
        "education_required": education,
        "years_required": years_min,
        "years_required_max": years_max,
        "employment_type": job.get("employment_type") or "",
        "experience_level": job.get("experience_level") or "",
        "clearance_level": clearance_level,
        "clearance_required": clearance,
        "travel_required": travel,
        "travel_pct_min": travel_pct_min,
        "travel_pct_max": travel_pct_max,
        "schedule_type": schedule_type,
        "shift_schedule": shift_schedule,
        "weekend_required": weekend_required,
        "licenses_required": licenses,
        "certifications": certs,
        "languages_required": langs,
        "encouraged_to_apply": encouraged,
        "job_keywords": title_keywords,
        "normalized_title": normalized_title,
    }
    field_confidence = {k: round(_confidence_from_value(v), 3) for k, v in field_values_for_conf.items()}
    field_provenance = {k: "rule_regex_v2" for k in field_values_for_conf}
    non_zero = [v for v in field_confidence.values() if v > 0]
    # Legacy avg-completeness score — kept for backward compatibility but no longer
    # used in gating; replaced by category_confidence below.
    classification_confidence = round(sum(non_zero) / len(non_zero), 3) if non_zero else 0.0

    # Category-specific confidence: how certain are we about *which* category this job is.
    # title+desc agree → 0.97, title-only → 0.92, desc-only → 0.72, no match → 0.0
    if category and _cat_title_match and _cat_desc_match:
        category_confidence = 0.97
    elif category and _cat_title_match:
        category_confidence = 0.92
    elif category and _cat_desc_match:
        category_confidence = 0.72
    else:
        category_confidence = 0.0

    classification_source = "rules"
    classification_provenance = {
        "engine": "rule_regex_v2",
        "signals_count": len(non_zero),
        "text_basis": "title+description+requirements+benefits",
        "category_match": (
            "title+desc" if (_cat_title_match and _cat_desc_match)
            else "title" if _cat_title_match
            else "desc" if _cat_desc_match
            else "none"
        ),
    }
    resume_score = _resume_ready_score({
        "description": description,
        "title": normalized_title or title,
        "country": detected_country,
        "state": job.get("state") or "",
        "salary_raw": job.get("salary_raw") or "",
        "employment_type": job.get("employment_type") or "",
        "experience_level": job.get("experience_level") or "",
        "years_required": years_min,
        "education_required": education,
        "skills": all_skills,
        "certifications": certs,
        "languages_required": langs,
        "clearance_level": clearance_level,
        "travel_required": travel,
        "benefits_list": benefits_found,
        "job_category": category,
        "department_normalized": department_normalized,
        "company_name": job.get("company_name") or "",
    })

    return {
        # Skills
        "skills":               all_skills,
        "tech_stack":           tech_stack,
        "job_category":         category,
        "normalized_title":     normalized_title,
        "title_keywords":       title_keywords,
        # Experience
        "years_required":       years_min,
        "years_required_max":   years_max,
        "education_required":   education,
        # Legal / visa
        "visa_sponsorship":     visa_sponsorship,
        "work_authorization":   work_authorization,
        "clearance_required":   clearance,
        "clearance_level":      clearance_level,
        # Compensation extras
        "salary_equity":        salary_equity,
        "signing_bonus":        signing_bonus,
        "relocation_assistance": relocation,
        # Work conditions
        "travel_required":      travel,
        "travel_pct_min":       travel_pct_min,
        "travel_pct_max":       travel_pct_max,
        "schedule_type":        schedule_type,
        "shift_schedule":       shift_schedule,
        "shift_details":        shift_details,
        "hours_hint":           hours_hint,
        "weekend_required":     weekend_required,
        # Structured lists
        "certifications":       certs,
        "licenses_required":    licenses,
        "benefits_list":        benefits_found,
        "languages_required":   langs,
        "encouraged_to_apply":  encouraged,
        "job_keywords":         title_keywords,
        "department_normalized": department_normalized,
        "country":              detected_country,
        # Quality
        "word_count":           word_count,
        "quality_score":        quality,
        "jd_quality_score":     content_meta["jd_quality_score"],
        "classification_confidence":  classification_confidence,
        "category_confidence":        category_confidence,
        "classification_source":      classification_source,
        "enrichment_version":         "v3",
        "classification_provenance":  classification_provenance,
        "field_confidence":     field_confidence,
        "field_provenance":     field_provenance,
        "resume_ready_score":   resume_score,
        "description_clean":    description,
        "description_raw_html": content_meta["raw_html"],
        "has_html_content":     content_meta["has_html_content"],
        "cleaning_version":     content_meta["cleaning_version"],
        # Extracted sections (populated even for harvesters that don't parse them)
        "requirements":         requirements,
        "responsibilities":     _existing_resp,
    }
