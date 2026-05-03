"""
Department classification engine — 4-tier pipeline.

Tier 1: Keyword rules on normalized title          (confidence 0.95 / 0.80)
Tier 2: parsed_jd.role_domain reuse               (confidence 0.85)
Tier 3: BAAI/bge-small-en-v1.5 cosine similarity  (confidence = cosine score)
Tier 4: LLM via existing LLMConfig                (confidence 0.90, source "llm")

Edge cases covered (#11-14, 21-25 from audit):
- Consulting-specific titles (SAP FICO, Salesforce, Oracle DBA) → IT
- "Staffing Recruiter" / "Technical Recruiter" → HR
- "Pre-Sales Engineer" → Sales
- "Consultant" alone → scan description for tech keywords
- Title normalization: lowercase, strip emoji, strip HTML, strip punctuation
- Multi-dept title: confidence capped at 0.70
- company_obj.industry as tiebreaker when confidence < 0.70
- Empty description → skip text-based tiers
- manual source → never overwrite
"""
from __future__ import annotations

import os
import re
import unicodedata
from pathlib import Path
from functools import lru_cache
from typing import Optional

import yaml

from .country import strip_html

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ── Role domain → department mapping (reuse parsed_jd) ───────────────────────
_ROLE_DOMAIN_MAP: dict[str, str] = {
    "software engineering": "software_dev",
    "software development": "software_dev",
    "web development": "software_dev",
    "mobile development": "software_dev",
    "data science": "data_analytics",
    "data engineering": "data_analytics",
    "machine learning": "data_analytics",
    "artificial intelligence": "data_analytics",
    "business intelligence": "data_analytics",
    "devops": "devops_cloud",
    "cloud": "devops_cloud",
    "infrastructure": "devops_cloud",
    "cybersecurity": "security",
    "security": "security",
    "information security": "security",
    "it support": "it_support",
    "help desk": "it_support",
    "qa": "qa_testing",
    "quality assurance": "qa_testing",
    "testing": "qa_testing",
    "database": "systems_network",
    "systems": "systems_network",
    "networking": "systems_network",
    "it management": "it_management",
    "engineering management": "it_management",
    "architecture": "it_management",
    "healthcare it": "healthcare_it",
    "sales": "sales",
    "marketing": "marketing",
    "human resources": "hr",
    "finance": "finance",
    "accounting": "finance",
    "operations": "operations",
    "legal": "legal",
    "compliance": "legal",
    "customer success": "customer_success",
    "design": "design",
    "ux": "design",
    "ui": "design",
    "administrative": "admin",
    "civil engineering": "civil_eng",
    "construction": "civil_eng",
    "healthcare": "healthcare",
    "clinical": "healthcare",
    "nursing": "healthcare",
    "management": "management",
}

# Tech keywords for "Consultant" alone disambiguation
_TECH_KEYWORDS = re.compile(
    r"\b(python|java|javascript|typescript|react|angular|vue|node|aws|azure|gcp|"
    r"kubernetes|docker|terraform|sql|database|api|microservices|cloud|devops|"
    r"machine learning|data science|cybersecurity|sap|salesforce|oracle|"
    r"servicenow|sharepoint|dynamics|power bi|tableau|snowflake|spark|hadoop|"
    r"linux|networking|firewall|ehr|epic|cerner|hl7|fhir)\b",
    re.I,
)

# ── Emoji / special char normalization ───────────────────────────────────────

def _normalize_title(title: str) -> str:
    # Strip HTML (edge case: title sometimes has markup)
    title = strip_html(title)
    # Remove emoji and non-ASCII symbols
    title = "".join(c for c in title if not unicodedata.category(c).startswith("So"))
    # Replace separators with space
    title = re.sub(r"[/_\-–—•|·]", " ", title)
    # Remove trailing/leading punctuation from each word
    title = re.sub(r"[^\w\s]", " ", title)
    # Collapse whitespace
    title = re.sub(r"\s+", " ", title).strip().lower()
    return title


# ── YAML rule loader ─────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_rules() -> dict[str, list[str]]:
    path = _DATA_DIR / "dept_rules.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=1)
def _load_anchors() -> dict[str, list[str]]:
    path = _DATA_DIR / "dept_anchors.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ── Tier 1: Keyword rules ────────────────────────────────────────────────────

def _rules_classify(norm_title: str) -> tuple[str, float]:
    """Returns (department, confidence). Confidence 0.95 for exact title match, 0.80 for keyword."""
    rules = _load_rules()
    matches: list[tuple[str, float]] = []

    for dept, keywords in rules.items():
        for kw in keywords:
            if kw in norm_title:
                # Exact full-title match → highest confidence
                conf = 0.95 if kw == norm_title else 0.80
                matches.append((dept, conf))
                break  # first match per dept

    if not matches:
        return "", 0.0

    # Sort by confidence desc
    matches.sort(key=lambda x: x[1], reverse=True)
    top_dept, top_conf = matches[0]

    # Multi-department titles → cap confidence
    if len(matches) > 1:
        top_conf = min(top_conf, 0.70)

    return top_dept, top_conf


# ── Tier 2: parsed_jd.role_domain reuse ─────────────────────────────────────

def _role_domain_classify(role_domain: str) -> tuple[str, float]:
    if not role_domain:
        return "", 0.0
    rd = role_domain.lower().strip()
    for key, dept in _ROLE_DOMAIN_MAP.items():
        if key in rd:
            return dept, 0.85
    return "", 0.0


# ── Tier 3: Embedding cosine similarity ─────────────────────────────────────

@lru_cache(maxsize=1)
def _load_anchor_embeddings():
    """Lazy-load model and pre-compute anchor embeddings. Returns (model, dept_list, matrix)."""
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore
        import numpy as np  # type: ignore

        model_name = os.environ.get("DEPT_EMBED_MODEL", "BAAI/bge-small-en-v1.5")
        model = SentenceTransformer(model_name)
        anchors = _load_anchors()

        dept_names: list[str] = []
        anchor_texts: list[str] = []
        dept_slice: list[tuple[int, int]] = []  # (start, end) index per dept

        idx = 0
        for dept, phrases in anchors.items():
            dept_names.append(dept)
            dept_slice.append((idx, idx + len(phrases)))
            anchor_texts.extend(phrases)
            idx += len(phrases)

        matrix = model.encode(anchor_texts, normalize_embeddings=True, show_progress_bar=False)
        return model, dept_names, dept_slice, matrix

    except ImportError:
        return None, None, None, None
    except Exception:
        return None, None, None, None


def _embedding_classify(text: str) -> tuple[str, float]:
    """Returns (department, confidence=cosine_score)."""
    if not text or not text.strip():
        return "", 0.0

    try:
        import numpy as np  # type: ignore
        model, dept_names, dept_slice, matrix = _load_anchor_embeddings()
        if model is None:
            return "", 0.0

        vec = model.encode([text], normalize_embeddings=True, show_progress_bar=False)[0]
        scores = matrix @ vec  # cosine similarity (normalized vectors)

        # Per-department: max similarity across its anchor phrases
        dept_scores: list[tuple[str, float]] = []
        for i, dept in enumerate(dept_names):
            start, end = dept_slice[i]
            dept_scores.append((dept, float(scores[start:end].max())))

        dept_scores.sort(key=lambda x: x[1], reverse=True)
        best_dept, best_score = dept_scores[0]

        if best_score < 0.40:  # too uncertain → don't classify
            return "", best_score

        return best_dept, best_score

    except Exception:
        return "", 0.0


# ── Tier 4: LLM ──────────────────────────────────────────────────────────────

_DEPT_CHOICES_STR = (
    "software_dev, data_analytics, devops_cloud, security, it_support, qa_testing, "
    "systems_network, it_management, healthcare_it, management, sales, marketing, hr, "
    "finance, operations, legal, customer_success, design, admin, civil_eng, healthcare, other"
)


def _llm_classify(title: str, description: str) -> tuple[str, float]:
    """Batch-friendly LLM classification. Returns (department, 0.90)."""
    try:
        from core.llm import call_llm  # type: ignore
    except ImportError:
        return "", 0.0

    desc_snippet = strip_html(description or "")[:300]
    prompt = (
        f"Classify this job into exactly ONE department code.\n"
        f"Title: {title}\n"
        f"Description snippet: {desc_snippet}\n\n"
        f"Valid codes: {_DEPT_CHOICES_STR}\n\n"
        f"Reply with ONLY the code, nothing else."
    )
    try:
        result = call_llm(prompt, max_tokens=20, temperature=0)
        code = (result or "").strip().lower().replace('"', "").replace("'", "")
        valid = set(_DEPT_CHOICES_STR.replace(" ", "").split(","))
        if code in valid:
            return code, 0.90
    except Exception:
        pass
    return "", 0.0


# ── Company industry tiebreaker ───────────────────────────────────────────────

_INDUSTRY_DEPT_MAP: dict[str, str] = {
    "hospital": "healthcare",
    "healthcare": "healthcare",
    "medical": "healthcare",
    "pharmaceutical": "healthcare",
    "finance": "finance",
    "banking": "finance",
    "investment": "finance",
    "insurance": "finance",
    "legal": "legal",
    "law": "legal",
    "construction": "civil_eng",
    "real estate": "civil_eng",
    "civil": "civil_eng",
}


def _industry_tiebreak(dept: str, confidence: float, company_industry: str) -> tuple[str, float]:
    """Boost confidence using company industry when classification is uncertain."""
    if confidence >= 0.70 or not company_industry:
        return dept, confidence
    ind = company_industry.lower()
    for key, mapped_dept in _INDUSTRY_DEPT_MAP.items():
        if key in ind:
            # If the classified dept is IT and company is healthcare → healthcare_it
            if mapped_dept == "healthcare" and dept in (
                "software_dev", "data_analytics", "devops_cloud", "systems_network", "it_support"
            ):
                return "healthcare_it", 0.75
            # If company is finance and role is data → finance dept
            if mapped_dept == "finance" and dept == "data_analytics":
                return "finance", 0.72
            break
    return dept, confidence


# ── Main entry point ──────────────────────────────────────────────────────────

def classify_department(
    title: str,
    description: str = "",
    role_domain: str = "",
    company_industry: str = "",
    *,
    use_llm: bool = True,
    llm_threshold: float = 0.45,
) -> tuple[str, float, str]:
    """
    Returns (department_code, confidence, source).
    source: "rules" | "role_domain" | "embedding" | "llm" | ""
    """
    if not title:
        return "other", 0.10, "rules"

    norm = _normalize_title(title)
    desc_clean = strip_html(description or "")

    # Handle "Consultant" alone — scan description for tech keywords first
    is_bare_consultant = norm in ("consultant", "it consultant", "senior consultant", "associate consultant")
    if is_bare_consultant and _TECH_KEYWORDS.search(desc_clean[:500]):
        # Treat as software_dev tentatively — embedding will refine
        norm = "software consultant"

    # ── Tier 1: Rules on title ──
    dept, conf = _rules_classify(norm)
    if dept and conf >= 0.80:
        dept, conf = _industry_tiebreak(dept, conf, company_industry)
        return dept, conf, "rules"

    # ── Tier 2: parsed_jd.role_domain ──
    rd_dept, rd_conf = _role_domain_classify(role_domain)
    if rd_dept and rd_conf > conf:
        dept, conf = rd_dept, rd_conf

    if dept and conf >= 0.75:
        dept, conf = _industry_tiebreak(dept, conf, company_industry)
        return dept, conf, "role_domain"

    # ── Tier 3: Embeddings ──
    embed_input = f"{title}. {desc_clean[:150]}" if desc_clean else title
    em_dept, em_conf = _embedding_classify(embed_input)
    if em_dept and em_conf > conf:
        dept, conf = em_dept, em_conf

    if dept and conf >= 0.55:
        dept, conf = _industry_tiebreak(dept, conf, company_industry)
        return dept, conf, "embedding"

    # ── Tier 4: LLM (only if confidence still low) ──
    if use_llm and conf < llm_threshold:
        llm_dept, llm_conf = _llm_classify(title, description)
        if llm_dept:
            dept, conf = llm_dept, llm_conf
            dept, conf = _industry_tiebreak(dept, conf, company_industry)
            return dept, conf, "llm"

    # Return best so far (even if below threshold)
    if not dept:
        dept = "other"
        conf = 0.10

    dept, conf = _industry_tiebreak(dept, conf, company_industry)
    return dept, conf, "embedding" if em_dept else ("role_domain" if rd_dept else "rules")
