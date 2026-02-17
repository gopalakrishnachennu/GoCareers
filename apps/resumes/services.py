import openai
import time
import re
import json
import logging
import datetime
from django.utils.html import strip_tags
from django.utils import timezone
from django.db.models import Sum
from docx import Document
from io import BytesIO
from prompts_app.services import get_active_prompt_for_job
from prompts_app.models import Prompt
from django.utils.html import strip_tags
from core.models import LLMConfig, LLMUsageLog
from core.security import decrypt_value
from core.llm_services import calculate_cost

logger = logging.getLogger("apps.resumes")

SKILL_BULLET_PREFIXES = [
    "cloud platforms",
    "iac",
    "ci/cd",
    "ci/cd & devops tools",
    "containers",
    "containers & orchestration",
    "scripting",
    "scripting & automation",
    "monitoring",
    "monitoring & logging",
    "security",
    "security & compliance",
    "ops",
    "ops & governance",
    "databases",
    "documentation tools",
]

FILLER_PHRASES = [
    "using reliability and automation practices to support stable delivery and measurable outcomes.",
    "while documenting procedures, collaborating with stakeholders, and improving operational readiness.",
    "with attention to security controls, troubleshooting rigor, and continuous improvement goals.",
    "to improve monitoring coverage, reduce manual effort, and strengthen operational consistency.",
    "by aligning changes with compliance expectations, change control, and service support needs.",
]


DEFAULT_SYSTEM_PROMPT = (
    "You are a professional resume writer specializing in consulting and IT staffing. "
    "Generate a polished, ATS-optimized resume tailored to the specific job description. "
    "Use structured Markdown with these sections:\n"
    "## Professional Summary\n"
    "## Key Skills\n"
    "## Relevant Experience\n"
    "## Notable Projects\n"
    "## Education & Certifications\n\n"
    "Be specific, quantify achievements where possible, and align the resume language "
    "with the job description keywords."
)

STOPWORDS = {
    "the","and","for","with","that","this","from","your","you","our","are","was","were","will","shall","can",
    "able","ability","have","has","had","not","but","use","using","used","into","over","under","across","per",
    "to","of","in","on","at","by","as","or","an","a","is","it","we","they","their","them","he","she","his","her",
    "be","been","being","if","then","than","also","such","other","more","most","less","least","any","all","each",
    "including","include","includes","within","without","via","etc","etc."
}


def extract_keywords(text, max_keywords=200):
    if not text:
        return []
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9+./#_-]{1,}", text.lower())
    keywords = []
    for t in tokens:
        if len(t) < 3:
            continue
        if t in STOPWORDS:
            continue
        keywords.append(t)
    # preserve order, unique
    seen = set()
    uniq = []
    for k in keywords:
        if k in seen:
            continue
        seen.add(k)
        uniq.append(k)
        if len(uniq) >= max_keywords:
            break
    return uniq


def score_ats(jd_text, resume_text):
    if not jd_text or not resume_text:
        return 0
    keywords = extract_keywords(jd_text)
    if not keywords:
        return 0
    content = resume_text.lower()
    matched = [k for k in keywords if k in content]
    score = int((len(matched) / len(keywords)) * 100)
    if score > 100:
        score = 100
    return score


def validate_resume(content):
    errors = []
    warnings = []
    if not content:
        errors.append("Resume content is empty.")
        return errors, warnings

    lines = [l.rstrip() for l in content.splitlines()]
    text = "\n".join(lines)

    required_headings = [
        "Professional Summary",
        "Core Skills",
        "Professional Experience",
        "Education",
    ]

    # Exact heading checks
    for h in required_headings:
        if h not in text:
            errors.append(f"Missing required section heading: {h}.")

    # Enforce section order
    heading_positions = {h: text.find(h) for h in required_headings if h in text}
    if len(heading_positions) == len(required_headings):
        if not (heading_positions["Professional Summary"] <
                heading_positions["Core Skills"] <
                heading_positions["Professional Experience"] <
                heading_positions["Education"]):
            errors.append("Section order must be: Professional Summary, Core Skills, Professional Experience, Education.")

    # Bullet checks in Professional Experience
    if "Professional Experience" in text:
        exp_block = text.split("Professional Experience", 1)[1]
        for h in ["Education"]:
            if h in exp_block:
                exp_block = exp_block.split(h, 1)[0]
        lines_exp = [line.rstrip() for line in exp_block.splitlines()]
        bullets = [line for line in lines_exp if line.strip().startswith("- ")]
        if len(bullets) < 6:
            errors.append("Professional Experience must include at least 6 bullet points.")

        # Word count per bullet (>= 22 words)
        for i, b in enumerate(bullets, start=1):
            words = [w for w in re.findall(r"[A-Za-z0-9']+", b) if w]
            if len(words) < 22:
                errors.append(f"Bullet {i} in Professional Experience has fewer than 22 words.")
                break

        # Detect role headers (single-line or two-line with dates)
        header_pattern = re.compile(
            r"^("
            r".+,\s+.+,\s+.+\(\d{4}\s*[–-]\s*(Present|\d{4})\)\s*"
            r"|"
            r".+\s+\|\s+.+\s+\|\s+\d{4}\s*[–-]\s*(Present|\d{4})\s*"
            r"|"
            r".+\s+[–-]\s+.+\s+[–-]\s+\d{4}\s*[–-]\s*(Present|\d{4})\s*"
            r"|"
            r".+\s+—\s+.+\s+—\s+\d{4}\s*[–-]\s*(Present|\d{4})\s*"
            r")$"
        )
        roles = []
        i = 0
        while i < len(lines_exp):
            line = lines_exp[i].strip()
            if not line or line.startswith("- "):
                i += 1
                continue
            if header_pattern.match(line):
                roles.append({"start": i})
                i += 1
                continue
            if i + 1 < len(lines_exp):
                next_line = lines_exp[i + 1].strip()
                if next_line and not next_line.startswith("- ") and re.search(r"\d{4}", next_line):
                    roles.append({"start": i})
                    i += 2
                    continue
            i += 1

        if not roles:
            errors.append("Role headers must follow format: Title, Company, (YYYY–YYYY/Present).")
        else:
            for idx, role in enumerate(roles):
                start = role["start"]
                end = roles[idx + 1]["start"] if idx + 1 < len(roles) else len(lines_exp)
                role_lines = lines_exp[start:end]
                role_bullets = [l for l in role_lines if l.strip().startswith("- ")]
                if idx == 0:
                    if not (7 <= len(role_bullets) <= 10):
                        errors.append(f"Most recent role must have 7–10 bullets (found {len(role_bullets)}).")
                        break
                else:
                    if len(role_bullets) != 6:
                        errors.append(f"Role {idx + 1} must have exactly 6 bullets (found {len(role_bullets)}).")
                        break
    else:
        errors.append("Professional Experience section missing or not detected.")

    # Summary length check (70–120 words)
    if "Professional Summary" in text:
        summary_block = text.split("Professional Summary", 1)[1]
        for h in ["Core Skills", "Professional Experience", "Education"]:
            if h in summary_block:
                summary_block = summary_block.split(h, 1)[0]
        summary_words = re.findall(r"[A-Za-z0-9']+", summary_block)
        if len(summary_words) < 70 or len(summary_words) > 120:
            errors.append("Professional Summary must be 70–120 words.")
    else:
        errors.append("Professional Summary section missing or not detected.")

    return errors, warnings


def _find_section_bounds(text, heading, headings):
    start = text.find(heading)
    if start == -1:
        return None
    # find next heading after start
    after = text[start + len(heading):]
    next_positions = []
    for h in headings:
        if h == heading:
            continue
        idx = after.find(h)
        if idx != -1:
            next_positions.append(idx)
    end = start + len(heading) + (min(next_positions) if next_positions else len(after))
    return start, end


def extract_section(content, heading, headings):
    bounds = _find_section_bounds(content, heading, headings)
    if not bounds:
        return ""
    start, end = bounds
    return content[start:end].strip()


def replace_section(content, heading, headings, new_section):
    bounds = _find_section_bounds(content, heading, headings)
    if not bounds:
        return content
    start, end = bounds
    return (content[:start] + new_section.strip() + "\n\n" + content[end:]).strip()


def _format_month_year(dt):
    if not dt:
        return ""
    try:
        return dt.strftime('%b %Y')
    except Exception:
        return str(dt)


def _build_header_line(job, consultant):
    name = consultant.user.get_full_name() or consultant.user.username
    location = job.location or "Not provided."
    phone = consultant.phone or "Not provided."
    email = consultant.user.email or "Not provided."
    return f"{name} | {location} | {phone} | {email}"


def _normalize_match_text(text):
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())

def _clean_bullet_line(line):
    if not line:
        return ""
    line = line.strip()
    line = re.sub(r"^[-•*]\s+", "", line)
    line = re.sub(r"^\d+[\.\)]\s+", "", line)
    line = re.sub(r"^\*\*", "", line)
    line = re.sub(r"\*\*$", "", line)
    return line.strip()


def _is_skill_bullet(line):
    if not line:
        return False
    normalized = _clean_bullet_line(line).lower()
    for prefix in SKILL_BULLET_PREFIXES:
        if normalized.startswith(prefix + ":"):
            return True
    return False


def _bullet_word_count(line):
    return len([w for w in re.findall(r"[A-Za-z0-9']+", line) if w])

def _normalize_bullet_for_dedupe(line):
    if not line:
        return ""
    text = _clean_bullet_line(line).lower()
    text = re.sub(r"[^a-z0-9\s]+", "", text)
    words = [w for w in text.split() if w and w not in STOPWORDS]
    return " ".join(words)


def _dedupe_bullets(lines):
    seen = set()
    out = []
    for b in lines:
        norm = _normalize_bullet_for_dedupe(b)
        if not norm:
            continue
        if norm in seen:
            continue
        seen.add(norm)
        out.append(b)
    return out


def _avoid_job_specific_keywords(text, job):
    if not text:
        return text
    bad = []
    if job:
        if job.title:
            bad.extend(job.title.lower().split())
        if job.location:
            bad.extend(re.split(r"[,\s]+", job.location.lower()))
        if job.company:
            bad.extend(job.company.lower().split())
    bad = {w for w in bad if w and w not in STOPWORDS}
    if not bad:
        return text
    words = text.split()
    cleaned = [w for w in words if w.lower().strip(",.") not in bad]
    return " ".join(cleaned)


def _expand_bullet_to_min_words(line, job, min_words=22):
    if not line:
        return line
    words = _bullet_word_count(line)
    if words >= min_words:
        return line
    keywords = extract_keywords(job.description or "", max_keywords=8)
    tail = ", ".join(keywords[:4]) if keywords else "reliability, automation, monitoring, and compliance"
    fillers = [
        f"using {tail} practices to support stable delivery and measurable outcomes.",
        *FILLER_PHRASES,
    ]
    idx = 0
    out = line.rstrip(".")
    while _bullet_word_count(out) < min_words and idx < len(fillers):
        out = f"{out}, {fillers[idx]}"
        idx += 1
    return _avoid_job_specific_keywords(out, job)


def _total_experience_years_display(consultant):
    experiences = list(consultant.experience.all())
    if not experiences:
        return None
    starts = [e.start_date for e in experiences if e.start_date]
    if not starts:
        return None
    earliest = min(starts)
    latest = None
    current = any(e.is_current for e in experiences)
    for e in experiences:
        if e.is_current:
            latest = datetime.date.today()
            break
    if not latest:
        ends = [e.end_date for e in experiences if e.end_date]
        latest = max(ends) if ends else datetime.date.today()
    months = (latest.year - earliest.year) * 12 + (latest.month - earliest.month)
    years = max(0, months // 12)
    if current:
        return f"{years}+"
    return str(years)


def _ensure_summary_years(summary_text, years_display):
    if not summary_text or not years_display:
        return summary_text
    lines = summary_text.splitlines()
    updated = []
    replaced = False
    pattern = re.compile(r"\b\d+\+?\s+years?\b", re.IGNORECASE)
    for line in lines:
        if not replaced and pattern.search(line):
            updated.append(pattern.sub(f"{years_display} years", line))
            replaced = True
        else:
            updated.append(line)
    if not replaced and updated:
        updated[0] = f"{updated[0].rstrip('.')} with over {years_display} years of experience."
    return "\n".join(updated)


def _extract_bullets_for_role(content, title, company):
    if not content or not title or not company:
        return []
    lines = [l.rstrip() for l in content.splitlines()]
    title_l = title.lower()
    company_l = company.lower()
    title_n = _normalize_match_text(title)
    company_n = _normalize_match_text(company)
    start_idx = -1
    for i, line in enumerate(lines):
        low = line.lower()
        norm = _normalize_match_text(line)
        if title_n and company_n and title_n in norm and company_n in norm:
            start_idx = i
            break
    if start_idx == -1 and title_n:
        for i, line in enumerate(lines):
            if title_n in _normalize_match_text(line):
                start_idx = i
                break
    if start_idx == -1 and company_n:
        for i, line in enumerate(lines):
            if company_n in _normalize_match_text(line):
                start_idx = i
                break
    if start_idx == -1:
        return []
    bullets = []
    for j in range(start_idx + 1, len(lines)):
        line = lines[j].strip()
        if not line:
            if bullets:
                break
            continue
        if re.match(r"^[-•*]\s+", line):
            bullets.append(re.sub(r"^[-•*]\s+", "", line).strip())
            continue
        if re.match(r"^\d+[\.\)]\s+", line):
            bullets.append(re.sub(r"^\d+[\.\)]\s+", "", line).strip())
            continue
        if line.lower().startswith("responsibilities:"):
            remainder = line.split(":", 1)[1].strip()
            if remainder:
                bullets.append(remainder)
            continue
        if re.match(r"^[A-Za-z].*\\d{4}", line):
            break
        if line.lower().startswith(("professional experience", "education", "certifications", "core skills", "professional summary")):
            break
    return bullets


def _build_experience_section(consultant, source_content=None, bullets_map=None):
    lines = ["Professional Experience"]
    experiences = list(consultant.experience.all())
    if not experiences:
        lines.append("No experience listed.")
        return "\n".join(lines)
    bullets_map = bullets_map or {}
    ordered = [e for e, _, _ in _target_counts_for_experiences(experiences)]
    for e in ordered:
        start = _format_month_year(e.start_date)
        end = "Present" if e.is_current else _format_month_year(e.end_date)
        lines.append(f"{e.title}, {e.company}")
        lines.append(f"{start} – {end}".strip())
        if e.description:
            for item in [x.strip() for x in e.description.splitlines() if x.strip()]:
                lines.append(f"- {item}")
        else:
            key = f"{_normalize_match_text(e.title)}||{_normalize_match_text(e.company)}"
            bullets = bullets_map.get(key, [])
            if not bullets and source_content:
                bullets = _extract_bullets_for_role(source_content, e.title, e.company)
            if not bullets:
                logger.warning("No bullets found for role: %s @ %s", e.title, e.company)
            for b in bullets:
                lines.append(f"- {b}")
        lines.append("")
    return "\n".join(lines).strip()


def _fallback_bullets_for_role(job, count):
    keywords = extract_keywords(job.description or "", max_keywords=12)
    kw = ", ".join(keywords[:4]) if keywords else "cloud infrastructure and automation"
    kw = _avoid_job_specific_keywords(kw, job)
    generic = [
        f"Assisted with {kw} initiatives aligned to project requirements, delivery milestones, operational standards, and cross functional expectations for reliability and support.",
        "Supported monitoring, troubleshooting, and incident response across development and production environments, reducing downtime through consistent analysis and clear escalation paths.",
        "Contributed to CI/CD automation and infrastructure as code workflows to improve deployment consistency, reduce manual errors, and speed release cycles safely.",
        "Documented procedures, configurations, and runbooks to improve operational readiness, knowledge transfer, onboarding efficiency, and ongoing maintenance practices for teams.",
        "Collaborated with stakeholders to resolve support tickets, communicate system status updates, and capture requirements for future automation and stability improvements.",
        "Applied security and compliance practices, including access controls, least privilege, and network segmentation awareness to reduce risk and support audits.",
    ]
    if count <= 0:
        return []
    if count <= len(generic):
        return generic[:count]
    # Repeat deterministically if more bullets are needed
    out = []
    idx = 0
    while len(out) < count:
        out.append(generic[idx % len(generic)])
        idx += 1
    return out


def _target_counts_for_experiences(experiences):
    items = list(experiences)
    if not items:
        return []
    def _sort_key(e):
        end_date = e.end_date or datetime.date.min
        start_date = e.start_date or datetime.date.min
        return (1 if e.is_current else 0, end_date, start_date)
    items_sorted = sorted(items, key=_sort_key, reverse=True)
    targets = []
    for idx, e in enumerate(items_sorted):
        if idx == 0:
            targets.append((e, 7, 10))
        else:
            targets.append((e, 6, 6))
    return targets


def generate_experience_bullets_with_counts(job, consultant, roles_needed, system_prompt=None):
    if not roles_needed:
        return {}
    llm = LLMService()
    if not llm.client:
        logger.warning("LLM client unavailable, using keyword fallback bullets.")
        bullets_map = {}
        for r in roles_needed:
            key = f"{_normalize_match_text(r['title'])}||{_normalize_match_text(r['company'])}"
            bullets_map[key] = _fallback_bullets_for_role(job, r['count'])
        return bullets_map

    base_resume = consultant.base_resume_text or ""
    jd = job.description or ""
    user_prompt = (
        "Generate responsibilities bullets for the roles below.\n"
        "Use ONLY the job description and base resume text as sources.\n"
        "Do NOT invent companies, titles, dates, certifications, or education.\n"
        "Return valid JSON ONLY in this format:\n"
        "{\"roles\":[{\"title\":\"\",\"company\":\"\",\"count\":0,\"bullets\":[\"...\"]}]}\n\n"
        f"ROLES:\n{json.dumps(roles_needed)}\n\n"
        f"JOB DESCRIPTION:\n{jd}\n\n"
        f"BASE RESUME:\n{base_resume}\n"
    )
    system_prompt = system_prompt or "You are a resume assistant. Return only JSON, no prose."
    content, _, error = llm.generate_with_prompts(job, consultant, system_prompt, user_prompt)
    if error or not content:
        logger.warning("Bullet generation failed: %s", error or "empty response")
        return {}
    logger.debug("Bullet generation raw response length: %s", len(content))
    try:
        data = json.loads(content)
    except Exception:
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if not match:
            logger.warning("Bullet JSON parse failed, no JSON found")
            return {}
        try:
            data = json.loads(match.group(0))
        except Exception:
            logger.warning("Bullet JSON parse failed after extraction")
            return {}
    roles_out = data.get("roles") if isinstance(data, dict) else None
    if not roles_out:
        return {}
    bullets_map = {}
    for r in roles_out:
        title = (r.get("title") or "").strip()
        company = (r.get("company") or "").strip()
        bullets = [b.strip() for b in (r.get("bullets") or []) if str(b).strip()]
        if not title or not company or not bullets:
            continue
        key = f"{_normalize_match_text(title)}||{_normalize_match_text(company)}"
        bullets_map[key] = bullets
    return bullets_map


def build_experience_bullets_map(job, consultant, source_content):
    bullets_map = {}
    needs = []
    targets = _target_counts_for_experiences(consultant.experience.all())
    for e, min_count, max_count in targets:
        base_bullets = []
        if e.description:
            for item in [x.strip() for x in e.description.splitlines() if x.strip()]:
                cleaned = _clean_bullet_line(item)
                if cleaned and not _is_skill_bullet(cleaned):
                    base_bullets.append(cleaned)
        else:
            base_bullets = _extract_bullets_for_role(source_content, e.title, e.company) if source_content else []
            base_bullets = [b for b in base_bullets if b and not _is_skill_bullet(b)]
        if max_count and len(base_bullets) > max_count:
            base_bullets = base_bullets[:max_count]
        base_bullets = [_expand_bullet_to_min_words(b, job, 22) for b in base_bullets]
        base_bullets = _dedupe_bullets(base_bullets)
        key = f"{_normalize_match_text(e.title)}||{_normalize_match_text(e.company)}"
        bullets_map[key] = base_bullets
        if len(base_bullets) < min_count:
            needs.append({
                "title": e.title,
                "company": e.company,
                "count": min_count - len(base_bullets),
            })

    if needs:
        logger.warning("Missing experience bullets detected (enforce counts). roles=%s", [f"{n['title']} @ {n['company']}" for n in needs])
        generated_map = generate_experience_bullets_with_counts(job, consultant, needs)
        for n in needs:
            key = f"{_normalize_match_text(n['title'])}||{_normalize_match_text(n['company'])}"
            existing = bullets_map.get(key, [])
            generated = generated_map.get(key, [])
            if generated:
                for b in generated:
                    if b not in existing:
                        existing.append(b)
            bullets_map[key] = existing

    # Final trim to max counts
    for e, min_count, max_count in targets:
        key = f"{_normalize_match_text(e.title)}||{_normalize_match_text(e.company)}"
        items = bullets_map.get(key, [])
        if len(items) < min_count:
            items.extend(_fallback_bullets_for_role(job, min_count - len(items)))
        if max_count and len(items) > max_count:
            bullets_map[key] = items[:max_count]
        else:
            bullets_map[key] = items
        bullets_map[key] = [_expand_bullet_to_min_words(b, job, 22) for b in bullets_map[key]]
        bullets_map[key] = _dedupe_bullets(bullets_map[key])
    return bullets_map



def _build_education_section(consultant):
    lines = ["Education"]
    educations = consultant.education.all()
    if not educations:
        lines.append("No education listed.")
        return "\n".join(lines)
    for e in educations:
        start = _format_month_year(e.start_date)
        end = _format_month_year(e.end_date) if e.end_date else "Present"
        lines.append(f"- {e.degree} in {e.field_of_study} at {e.institution} ({start}–{end})")
    return "\n".join(lines)


def _build_certifications_section(consultant):
    certs = consultant.certifications.all()
    if not certs:
        return ""
    lines = ["Certifications"]
    for c in certs:
        lines.append(f"- {c.name}")
    return "\n".join(lines)


def _remove_section(content, heading, headings):
    bounds = _find_section_bounds(content, heading, headings)
    if not bounds:
        return content
    start, end = bounds
    return (content[:start] + content[end:]).strip()


def normalize_generated_resume(content, job, consultant, bullets_map=None):
    if not content:
        return content

    text = re.sub(r"^ATS Relevance Score:.*$", "", content, flags=re.IGNORECASE | re.MULTILINE).strip()
    headings = ["Header", "Professional Summary", "Core Skills", "Professional Experience", "Certifications", "Education"]

    header_line = _build_header_line(job, consultant)
    if "Header" in text:
        text = replace_section(text, "Header", headings, f"Header\n{header_line}")
    else:
        first_idx = min([idx for idx in (text.find(h) for h in headings) if idx != -1], default=-1)
        if first_idx != -1:
            text = f"{header_line}\n\n" + text[first_idx:]
        else:
            text = f"{header_line}\n\n{text}"

    if bullets_map is None:
        bullets_map = build_experience_bullets_map(job, consultant, text)
    exp_section = _build_experience_section(consultant, source_content=text, bullets_map=bullets_map)
    if "Professional Experience" in text:
        text = replace_section(text, "Professional Experience", headings, exp_section)
    else:
        text = f"{text}\n\n{exp_section}"

    edu_section = _build_education_section(consultant)
    if "Education" in text:
        text = replace_section(text, "Education", headings, edu_section)
    else:
        text = f"{text}\n\n{edu_section}"

    cert_section = _build_certifications_section(consultant)
    if cert_section:
        if "Certifications" in text:
            text = replace_section(text, "Certifications", headings, cert_section)
        else:
            text = f"{text}\n\n{cert_section}"
    else:
        text = _remove_section(text, "Certifications", headings)

    years_display = _total_experience_years_display(consultant)
    if years_display and "Professional Summary" in text:
        summary = extract_section(text, "Professional Summary", headings)
        if summary:
            fixed_summary = _ensure_summary_years(summary, years_display)
            if fixed_summary and fixed_summary != summary:
                text = replace_section(text, "Professional Summary", headings, fixed_summary)
                logger.info("Professional Summary updated with total experience years=%s", years_display)

    return text.strip()
SECTION_KEYS = {
    "name",
    "email",
    "phone",
    "jd_location",
    "professional_summary",
    "skills",
    "base_resume",
    "experience",
    "education",
    "jd_description",
}


def build_user_prompt_from_sections(job, consultant, sections, template_layout=None):
    selected = set(sections or [])
    selected = selected.intersection(SECTION_KEYS)

    parts = [
        "STRICT DATA RULES:",
        "- Use the provided profile data exactly for name, contact, experience titles/companies/dates, education, and certifications.",
        "- Do NOT invent or replace people, companies, dates, degrees, or certifications.",
        "- If certifications are not provided, do NOT add a Certifications section.",
        "- Bullet counts: most recent role must have 7–10 bullets; all other roles must have exactly 6 bullets.",
        "",
    ]
    contact_name = consultant.user.get_full_name() or consultant.user.username
    contact_email = consultant.user.email or "Not provided."
    contact_phone = consultant.phone or "Not provided."

    if template_layout:
        parts.append("TEMPLATE LAYOUT (fixed vs AI sections):")
        parts.append(json.dumps(template_layout))
        parts.append("")

    if "name" in selected:
        parts.append(f"Name: {contact_name}")
    if "email" in selected:
        parts.append(f"Email: {contact_email}")
    if "phone" in selected:
        parts.append(f"Phone: {contact_phone}")
    if "jd_location" in selected:
        parts.append(f"Location (use JD location): {job.location or 'Not provided.'}")

    years_display = _total_experience_years_display(consultant)
    if years_display:
        parts.append(f"Total Experience (use exactly): {years_display} years")

    if "professional_summary" in selected:
        parts.append("Professional Summary: Generate a concise summary using the prompt rules and the inputs below.")

    if "skills" in selected:
        skills = consultant.skills or []
        if skills:
            parts.append(f"Core Skills (from profile): {', '.join(skills)}")
        else:
            parts.append("Core Skills: Generate a categorized skills list based on JD and consultant profile.")

    if "base_resume" in selected:
        base_resume_text = consultant.base_resume_text or "Not provided."
        parts.append("Base Resume:")
        parts.append(base_resume_text)

    if "experience" in selected:
        parts.append("Experience:")
        experiences = list(consultant.experience.all())
        ordered = [e for e, _, _ in _target_counts_for_experiences(experiences)]
        if experiences:
            for e in ordered:
                start = e.start_date.strftime('%Y') if e.start_date else ''
                end = "Present" if e.is_current else (e.end_date.strftime('%Y') if e.end_date else '')
                role_line = f"{e.title}, {e.company}, ({start}–{end})"
                if e.description:
                    role_line += f"\n  Responsibilities: {e.description}"
                else:
                    role_line += (
                        "\n  Responsibilities: Generate bullets using only the JD and base resume."
                        " The first role listed is most recent and needs 7–10 bullets; all other roles need exactly 6."
                        " Keep the role title, company, and dates exactly as provided."
                    )
                parts.append(role_line)
        else:
            parts.append("- No experience listed.")

    if "education" in selected:
        parts.append("Education:")
        educations = consultant.education.all()
        if educations:
            for e in educations:
                start = e.start_date.strftime('%Y') if e.start_date else ''
                end = e.end_date.strftime('%Y') if e.end_date else 'Present'
                parts.append(f"- {e.degree} in {e.field_of_study} at {e.institution} ({start}–{end})")
        else:
            parts.append("- No education listed.")

    if "jd_description" in selected:
        parts.append("--- JOB DESCRIPTION ---")
        parts.append(job.description or "Not provided.")

    return "\n".join(parts).strip()

def get_system_prompt_text(job, consultant, prompt_override=None):
    if prompt_override:
        if prompt_override.system_text:
            return prompt_override.system_text
        if prompt_override.description:
            return strip_tags(prompt_override.description)
    # Force resume-specific prompt when available
    resume_prompt = Prompt.objects.filter(name='resume-2').first()
    if resume_prompt:
        if resume_prompt.system_text:
            return resume_prompt.system_text
        if resume_prompt.description:
            return strip_tags(resume_prompt.description)
    prompt = get_active_prompt_for_job(job, consultant)
    if prompt:
        if prompt.system_text:
            return prompt.system_text
        if prompt.description:
            return strip_tags(prompt.description)
    return DEFAULT_SYSTEM_PROMPT


def build_input_summary(job, consultant):
    experiences = []
    for exp in consultant.experience.all():
        experiences.append({
            'title': exp.title,
            'company': exp.company,
            'start_year': exp.start_date.strftime('%Y') if exp.start_date else '',
            'end_year': '' if exp.is_current or not exp.end_date else exp.end_date.strftime('%Y'),
            'is_current': exp.is_current,
        })

    educations = []
    for edu in consultant.education.all():
        educations.append({
            'degree': edu.degree,
            'field_of_study': edu.field_of_study,
            'institution': edu.institution,
            'start_year': edu.start_date.strftime('%Y') if edu.start_date else '',
            'end_year': edu.end_date.strftime('%Y') if edu.end_date else 'Present',
        })

    return {
        'job_title': job.title,
        'job_company': job.company,
        'job_location': job.location or 'Not provided.',
        'job_description': job.description,
        'consultant_name': consultant.user.get_full_name() or consultant.user.username,
        'consultant_email': consultant.user.email or 'Not provided.',
        'consultant_phone': consultant.phone or 'Not provided.',
        'base_resume_text': consultant.base_resume_text or '',
        'experience': experiences,
        'education': educations,
    }


class LLMService:
    def __init__(self):
        config = LLMConfig.load()
        self.config = config
        self.api_key = decrypt_value(config.encrypted_api_key)
        if self.api_key and not self.api_key.startswith('sk-your') and config.generation_enabled:
            self.client = openai.OpenAI(api_key=self.api_key)
        else:
            self.client = None

    def _build_prompt(self, job, consultant, prompt_override=None):
        """Build the user prompt from template or default."""
        # Gather contact info
        contact_name = consultant.user.get_full_name() or consultant.user.username
        contact_email = consultant.user.email or "Not provided."
        contact_phone = consultant.phone or "Not provided."
        base_resume_text = consultant.base_resume_text or "Not provided."

        # Gather experience summary
        experiences = consultant.experience.all()
        exp_summary = "\n".join(
            f"- {e.title} at {e.company} ({e.start_date.strftime('%Y')}–{'Present' if e.is_current else e.end_date.strftime('%Y') if e.end_date else ''})"
            for e in experiences
        ) or "No experience listed."

        # Gather education summary
        educations = consultant.education.all()
        edu_summary = "\n".join(
            f"- {e.degree} in {e.field_of_study} at {e.institution} ({e.start_date.strftime('%Y')}–{e.end_date.strftime('%Y') if e.end_date else 'Present'})"
            for e in educations
        ) or "No education listed."

        # Gather certifications
        certs = consultant.certifications.all()
        cert_summary = ", ".join(c.name for c in certs) or "None listed."

        input_summary = (
            f"Job: {job.title} @ {job.company}\n"
            f"Job Location: {job.location or 'Not provided.'}\n"
            f"Consultant: {contact_name}\n"
            f"Email: {contact_email}\n"
            f"Phone: {contact_phone}\n"
            f"Experience:\n{exp_summary}\n"
            f"Education:\n{edu_summary}\n"
            f"Certifications: {cert_summary}\n"
        )

        prompt = prompt_override or get_active_prompt_for_job(job, consultant)
        if prompt:
            try:
                template_text = prompt.template_text
                base = template_text.format(
                    job_title=job.title,
                    company=job.company,
                    job_description=job.description,
                    consultant_name=consultant.user.get_full_name() or consultant.user.username,
                    consultant_bio=consultant.bio or "Not provided.",
                    consultant_skills=", ".join(consultant.skills) if consultant.skills else "Not provided.",
                    experience_summary=exp_summary,
                    certifications=cert_summary,
                    base_resume_text=base_resume_text,
                    input_summary=input_summary,
                )
                return (
                    f"{base}\n\n"
                    f"--- JOB DESCRIPTION ---\n"
                    f"{job.description or 'Not provided.'}\n"
                )
            except (KeyError, IndexError):
                pass  # Fall through to default

        return (
            f"Consultant Name: {contact_name}\n"
            f"Consultant Email: {contact_email}\n"
            f"Consultant Phone: {contact_phone}\n"
            f"Bio: {consultant.bio or 'Not provided.'}\n"
            f"Summary: {consultant.bio or 'Not provided.'}\n"
            f"Skills: {', '.join(consultant.skills) if consultant.skills else 'Not provided.'}\n"
            f"Base Resume:\n{base_resume_text}\n"
            f"Experience:\n{exp_summary}\n"
            f"Education:\n{edu_summary}\n"
            f"Certifications: {cert_summary}\n\n"
            f"Input Summary:\n{input_summary}\n"
            f"--- TARGET JOB ---\n"
            f"Title: {job.title}\n"
            f"Company: {job.company}\n"
            f"Job Location: {job.location or 'Not provided.'}\n"
            f"Location Rule: Consider roles within 15–30 miles of the job location.\n"
            f"Description:\n{job.description}\n"
            f"\nRequired Resume Sections: Summary, Skills, Experience, Education\n"
        )

    def generate_resume_content(self, job, consultant, actor=None, prompt_override=None):
        """Generate resume content. Returns (content, tokens_used, error)."""
        prompt_text = self._build_prompt(job, consultant, prompt_override=prompt_override)
        system_prompt = get_system_prompt_text(job, consultant, prompt_override=prompt_override)

        if not self.client:
            mock = (
                f"## Professional Summary\n\n"
                f"Results-driven professional with expertise in "
                f"{', '.join(consultant.skills[:3]) if consultant.skills else 'various technologies'}. "
                f"Seeking the **{job.title}** position at **{job.company}**.\n\n"
                f"## Key Skills\n\n"
            )
            if consultant.skills:
                for skill in consultant.skills:
                    mock += f"- {skill}\n"
            else:
                mock += "- Skills not listed\n"

            mock += (
                f"\n## Relevant Experience\n\n"
                f"*(Experience details from profile)*\n\n"
                f"## Notable Projects\n\n"
                f"*(Projects aligned with {job.title} role)*\n\n"
                f"---\n*Mock resume — install a valid OpenAI API key for real GPT-4o generation.*"
            )
            return mock, 0, None

        if self.config.monthly_token_cap:
            month_start = timezone.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            total_month_tokens = LLMUsageLog.objects.filter(created_at__gte=month_start).aggregate(
                total=Sum('total_tokens')
            )['total'] or 0
            if total_month_tokens >= self.config.monthly_token_cap and self.config.auto_disable_on_cap:
                self.config.generation_enabled = False
                self.config.save()
                return None, 0, "Monthly token cap reached. Generation disabled."

        request_payload = {
            "model": self.config.active_model or "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt_text},
            ],
            "temperature": float(self.config.temperature),
            "max_tokens": self.config.max_output_tokens,
        }

        try:
            start = time.time()
            response = self.client.chat.completions.create(
                model=request_payload["model"],
                messages=request_payload["messages"],
                temperature=request_payload["temperature"],
                max_tokens=request_payload["max_tokens"],
            )
            latency_ms = int((time.time() - start) * 1000)
            content = response.choices[0].message.content
            prompt_tokens = response.usage.prompt_tokens if response.usage else 0
            completion_tokens = response.usage.completion_tokens if response.usage else 0
            tokens = response.usage.total_tokens if response.usage else 0
            costs = calculate_cost(self.config.active_model, prompt_tokens, completion_tokens)
            LLMUsageLog.objects.create(
                model_name=self.config.active_model,
                system_prompt=system_prompt,
                user_prompt=prompt_text,
                request_payload=request_payload,
                response_text=content or "",
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=tokens,
                cost_input=costs['input'],
                cost_output=costs['output'],
                cost_total=costs['total'],
                latency_ms=latency_ms,
                success=True,
                job=job,
                consultant=consultant,
                actor=actor,
            )
            return content, tokens, None
        except Exception as e:
            LLMUsageLog.objects.create(
                model_name=self.config.active_model,
                success=False,
                error_message=str(e),
                system_prompt=system_prompt,
                user_prompt=prompt_text,
                request_payload=request_payload,
                job=job,
                consultant=consultant,
                actor=actor,
            )
            return None, 0, str(e)

    def generate_with_prompts(self, job, consultant, system_prompt, user_prompt, actor=None):
        """Generate resume content using explicit prompts. Returns (content, tokens_used, error)."""
        if not self.client:
            mock = (
                f"## Professional Summary\n\n"
                f"Results-driven professional with expertise in "
                f"{', '.join(consultant.skills[:3]) if consultant.skills else 'various technologies'}. "
                f"Seeking the **{job.title}** position at **{job.company}**.\n\n"
                f"## Key Skills\n\n"
            )
            if consultant.skills:
                for skill in consultant.skills:
                    mock += f"- {skill}\n"
            else:
                mock += "- Skills not listed\n"

            mock += (
                f"\n## Relevant Experience\n\n"
                f"*(Experience details from profile)*\n\n"
                f"## Notable Projects\n\n"
                f"*(Projects aligned with {job.title} role)*\n\n"
                f"---\n*Mock resume — install a valid OpenAI API key for real GPT-4o generation.*"
            )
            return mock, 0, None

        if self.config.monthly_token_cap:
            month_start = timezone.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            total_month_tokens = LLMUsageLog.objects.filter(created_at__gte=month_start).aggregate(
                total=Sum('total_tokens')
            )['total'] or 0
            if total_month_tokens >= self.config.monthly_token_cap and self.config.auto_disable_on_cap:
                self.config.generation_enabled = False
                self.config.save()
                return None, 0, "Monthly token cap reached. Generation disabled."

        request_payload = {
            "model": self.config.active_model or "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": float(self.config.temperature),
            "max_tokens": self.config.max_output_tokens,
        }

        try:
            start = time.time()
            response = self.client.chat.completions.create(
                model=request_payload["model"],
                messages=request_payload["messages"],
                temperature=request_payload["temperature"],
                max_tokens=request_payload["max_tokens"],
            )
            latency_ms = int((time.time() - start) * 1000)
            content = response.choices[0].message.content
            prompt_tokens = response.usage.prompt_tokens if response.usage else 0
            completion_tokens = response.usage.completion_tokens if response.usage else 0
            tokens = response.usage.total_tokens if response.usage else 0
            costs = calculate_cost(self.config.active_model, prompt_tokens, completion_tokens)
            LLMUsageLog.objects.create(
                model_name=self.config.active_model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                request_payload=request_payload,
                response_text=content or "",
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=tokens,
                cost_input=costs['input'],
                cost_output=costs['output'],
                cost_total=costs['total'],
                latency_ms=latency_ms,
                success=True,
                job=job,
                consultant=consultant,
                actor=actor,
            )
            return content, tokens, None
        except Exception as e:
            LLMUsageLog.objects.create(
                model_name=self.config.active_model,
                success=False,
                error_message=str(e),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                request_payload=request_payload,
                job=job,
                consultant=consultant,
                actor=actor,
            )
            return None, 0, str(e)


class DocxService:
    def create_docx(self, content):
        """Convert markdown-ish text content into a simple DOCX document."""
        doc = Document()

        for line in content.split('\n'):
            stripped = line.strip()
            if stripped.startswith('## '):
                doc.add_heading(stripped[3:], level=2)
            elif stripped.startswith('# '):
                doc.add_heading(stripped[2:], level=1)
            elif stripped.startswith('- '):
                doc.add_paragraph(stripped[2:], style='List Bullet')
            elif stripped:
                doc.add_paragraph(stripped)

        buffer = BytesIO()
        doc.save(buffer)
        buffer.seek(0)
        return buffer
