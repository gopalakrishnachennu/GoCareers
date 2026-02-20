import json
import re
from typing import Dict, List, Tuple

from .services import LLMService
from .services import extract_keywords


def _normalize_line(line: str) -> str:
    return re.sub(r"\s+", " ", line.strip())


def _parse_skills_block(text: str) -> Dict[str, List[str]]:
    """
    Parse a SKILLS block of key:value lines.
    Returns {category: [items]}.
    """
    lines = [_normalize_line(l) for l in (text or "").splitlines() if _normalize_line(l)]
    out: Dict[str, List[str]] = {}
    for line in lines:
        if line.upper() == "SKILLS":
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        items = [i.strip() for i in value.split(",") if i.strip()]
        if items:
            out[key] = items
    return out


def _skills_only_from_jd(skills: Dict[str, List[str]], jd_text: str) -> Dict[str, List[str]]:
    jd_lower = (jd_text or "").lower()
    jd_terms = set(extract_keywords(jd_text or "", max_keywords=400))
    cleaned: Dict[str, List[str]] = {}
    for category, items in skills.items():
        kept = []
        for item in items:
            item_l = item.lower()
            # keep if item (or its tokens) appear in JD
            tokens = set(extract_keywords(item, max_keywords=40))
            if item_l in jd_lower or tokens & jd_terms:
                kept.append(item)
        if kept:
            cleaned[category] = kept
    return cleaned


def _format_skills_block(skills: Dict[str, List[str]]) -> str:
    lines = ["SKILLS"]
    for category, items in skills.items():
        lines.append(f"{category}: {', '.join(items)}")
    return "\n".join(lines)


def generate_skills_from_jd(job) -> str:
    jd_text = job.description or ""
    if not jd_text.strip():
        return "SKILLS\nSkills: Not provided."

    llm = LLMService()
    if not llm.client:
        # Fallback: simple JD keyword list
        terms = extract_keywords(jd_text, max_keywords=40)
        if not terms:
            return "SKILLS\nSkills: Not provided."
        return "SKILLS\nSkills: " + ", ".join(terms)

    system_prompt = (
        "You are an ATS resume assistant. Output ONLY plain text. "
        "Return ONLY the SKILLS section using key:value lines, no bullets."
    )
    user_prompt = (
        "From the JD below, generate a SKILLS section that looks human-written and ATS-friendly.\n"
        "Rules:\n"
        "- ONLY include skills explicitly stated in the JD. Do not invent.\n"
        "- Use 6–12 categories max.\n"
        "- Use key:value format with comma-separated items.\n"
        "- No bullets, no markdown.\n"
        "- Categories should reflect the JD emphasis (e.g., Cloud Platforms, IaC, CI/CD & DevOps Tools, Containers & Orchestration, Scripting & Automation, Monitoring & Logging, Security & Compliance, Ops & Governance, Databases, Documentation Tools).\n"
        "- Drop any category with fewer than 2 strong items.\n"
        "\nJOB DESCRIPTION:\n"
        f"{jd_text}\n"
    )

    content, _, error = llm.generate_with_prompts(job, None, system_prompt, user_prompt)
    if error or not content:
        terms = extract_keywords(jd_text, max_keywords=40)
        if not terms:
            return "SKILLS\nSkills: Not provided."
        return "SKILLS\nSkills: " + ", ".join(terms)

    parsed = _parse_skills_block(content)
    if not parsed:
        terms = extract_keywords(jd_text, max_keywords=40)
        if not terms:
            return "SKILLS\nSkills: Not provided."
        return "SKILLS\nSkills: " + ", ".join(terms)

    cleaned = _skills_only_from_jd(parsed, jd_text)
    if not cleaned:
        terms = extract_keywords(jd_text, max_keywords=40)
        if not terms:
            return "SKILLS\nSkills: Not provided."
        return "SKILLS\nSkills: " + ", ".join(terms)

    return _format_skills_block(cleaned)
