from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from django.db.models import Q

from harvest.jd_gate import evaluate_raw_job_resume_gate

from .models import Job


REASON_MISSING_URL = "MISSING_URL"
REASON_INACTIVE_POSTING = "INACTIVE_POSTING"
REASON_COMPANY_UNRESOLVED = "COMPANY_UNRESOLVED"
REASON_PLATFORM_MISMATCH = "PLATFORM_MISMATCH"
REASON_DUPLICATE_RISK = "DUPLICATE_RISK"
REASON_JD_TOO_WEAK = "JD_TOO_WEAK"
REASON_BLACKLISTED_COMPANY = "BLACKLISTED_COMPANY"
REASON_OK = "ELIGIBLE"


@dataclass
class GateResult:
    passed: bool
    reason_code: str
    reasons: list[str]
    checks: dict[str, bool]
    data_quality_score: float
    trust_score: float
    candidate_fit_score: float
    vet_priority_score: float
    lane: str
    status: str


def _clamp01(value: float) -> float:
    if value < 0:
        return 0.0
    if value > 1:
        return 1.0
    return float(value)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _is_title_meaningful(title: str) -> bool:
    t = (title or "").strip().lower()
    if len(t) < 4:
        return False
    return t not in {"job", "position", "opening", "opportunity", "role", "vacancy"}


def _has_clean_jd(raw_job) -> bool:
    return bool(evaluate_raw_job_resume_gate(raw_job).usable)


def _platform_tenant_match(raw_job) -> bool:
    url = (raw_job.original_url or "").strip()
    if not url:
        return False
    label = getattr(raw_job, "platform_label", None)
    if not label or not label.platform_id:
        return True

    host = (urlparse(url).netloc or "").lower()
    patterns = [p.lower() for p in (label.platform.url_patterns or []) if p]
    pattern_ok = True if not patterns else any(p in host or p in url.lower() for p in patterns)

    tenant = (label.tenant_id or "").strip().lower()
    if not tenant:
        return pattern_ok
    return pattern_ok and (tenant in url.lower())


def _duplicate_risk(raw_job) -> bool:
    # Fast dedupe: same url hash, or tight company+title+location match in live/pool.
    if raw_job.url_hash and Job.objects.filter(url_hash=raw_job.url_hash, is_archived=False).exists():
        return True

    title = (raw_job.normalized_title or raw_job.title or "").strip()
    company = (raw_job.company_name or (raw_job.company.name if raw_job.company_id else "") or "").strip()
    if not (title and company):
        return False

    base = Job.objects.filter(is_archived=False, company__iexact=company, title__iexact=title)
    loc = (raw_job.location_raw or "").strip()
    if loc:
        base = base.filter(Q(location__iexact=loc) | Q(location=""))
    return base.exists()


def evaluate_raw_job_gate(raw_job) -> GateResult:
    checks = {
        "active_posting": bool(raw_job.is_active),
        "valid_source_url": bool((raw_job.original_url or "").strip()),
        "tenant_platform_match": _platform_tenant_match(raw_job),
        "dedupe_passed": not _duplicate_risk(raw_job),
        "clean_jd_present": _has_clean_jd(raw_job),
        "company_resolved": bool(raw_job.company_id),
    }

    company = getattr(raw_job, "company", None)
    if company and getattr(company, "is_blacklisted", False):
        checks["company_resolved"] = False

    reasons: list[str] = []
    if not checks["valid_source_url"]:
        reasons.append(REASON_MISSING_URL)
    if not checks["active_posting"]:
        reasons.append(REASON_INACTIVE_POSTING)
    if not checks["company_resolved"]:
        reasons.append(
            REASON_BLACKLISTED_COMPANY
            if company and getattr(company, "is_blacklisted", False)
            else REASON_COMPANY_UNRESOLVED
        )
    if not checks["tenant_platform_match"]:
        reasons.append(REASON_PLATFORM_MISMATCH)
    if not checks["dedupe_passed"]:
        reasons.append(REASON_DUPLICATE_RISK)
    if not checks["clean_jd_present"]:
        reasons.append(REASON_JD_TOO_WEAK)

    hard_passed = not reasons

    # Multi-score model
    jd_quality = _as_float(raw_job.jd_quality_score, 0.0)
    quality = _as_float(raw_job.quality_score, 0.0)
    has_salary = bool(raw_job.salary_min or raw_job.salary_max or (raw_job.salary_raw or "").strip())
    has_location = bool((raw_job.city or "").strip() or (raw_job.state or "").strip() or (raw_job.country or "").strip() or (raw_job.location_raw or "").strip())
    has_experience = bool(raw_job.years_required is not None or raw_job.years_required_max is not None or raw_job.experience_level != "UNKNOWN")
    has_skills = bool(raw_job.tech_stack or raw_job.skills or raw_job.job_keywords)
    meaningful_title = _is_title_meaningful(raw_job.title)

    completeness_parts = [has_salary, has_location, has_experience, has_skills, meaningful_title]
    completeness = sum(1 for x in completeness_parts if x) / len(completeness_parts)
    data_quality = _clamp01((0.50 * jd_quality) + (0.25 * quality) + (0.25 * completeness))

    # Trust: source reliability + classification confidence + platform confidence + html penalty
    source_bonus = 0.45 if (raw_job.platform_slug or "").lower() in {
        "workday", "greenhouse", "lever", "ashby", "icims", "smartrecruiters", "bamboohr", "dayforce", "workable", "jobvite", "jarvis"
    } else 0.3
    cls_conf = _clamp01(_as_float(raw_job.classification_confidence, 0.0))
    platform_conf = 0.5
    if raw_job.platform_label_id:
        platform_conf_map = {"HIGH": 1.0, "MEDIUM": 0.7, "LOW": 0.4, "UNKNOWN": 0.5}
        platform_conf = platform_conf_map.get((raw_job.platform_label.confidence or "UNKNOWN").upper(), 0.5)
    html_penalty = 0.15 if raw_job.has_html_content else 0.0
    trust = _clamp01((0.40 * source_bonus) + (0.40 * cls_conf) + (0.20 * platform_conf) - html_penalty)

    # Candidate-fit readiness completeness
    has_commitment = raw_job.employment_type != "UNKNOWN"
    has_benefits = bool(raw_job.benefits_list or (raw_job.benefits or "").strip())
    has_language = bool(raw_job.languages_required)
    has_department = bool((raw_job.department_normalized or raw_job.department or "").strip())
    fit_parts = [has_salary, has_location, has_experience, has_skills, has_commitment, has_benefits, has_language, has_department]
    fit = _clamp01(sum(1 for x in fit_parts if x) / len(fit_parts))

    vet_priority = _clamp01((0.45 * data_quality) + (0.35 * trust) + (0.20 * fit))

    if not hard_passed:
        lane = "BLOCKED"
        status = "BLOCKED"
    elif vet_priority >= 0.82 and data_quality >= 0.72 and trust >= 0.70:
        lane = "AUTO"
        status = "ELIGIBLE"
    else:
        lane = "HUMAN"
        status = "REVIEW"

    return GateResult(
        passed=hard_passed,
        reason_code=REASON_OK if hard_passed else reasons[0],
        reasons=reasons,
        checks=checks,
        data_quality_score=round(data_quality, 4),
        trust_score=round(trust, 4),
        candidate_fit_score=round(fit, 4),
        vet_priority_score=round(vet_priority, 4),
        lane=lane,
        status=status,
    )


def apply_gate_result_to_job(job: Job, gate: GateResult) -> None:
    job.hard_gate_passed = bool(gate.passed)
    job.gate_status = gate.status
    job.vet_lane = gate.lane
    job.pipeline_reason_code = gate.reason_code
    job.pipeline_reason_detail = "; ".join(gate.reasons[:6]) if gate.reasons else ""
    job.hard_gate_failures = gate.reasons
    job.hard_gate_checks = gate.checks
    job.data_quality_score = gate.data_quality_score
    job.trust_score = gate.trust_score
    job.candidate_fit_score = gate.candidate_fit_score
    job.vet_priority_score = gate.vet_priority_score


def evaluate_job_gate(job: Job) -> GateResult:
    title_ok = _is_title_meaningful(job.title)
    desc = (job.description or "").strip()
    words = len(desc.split())
    min_words = 80
    min_chars = 400
    has_clean_jd = words >= min_words and len(desc) >= min_chars
    has_url = bool((job.original_link or "").strip())
    company_ok = bool(job.company_obj_id) and not (
        job.company_obj and getattr(job.company_obj, "is_blacklisted", False)
    )
    dedupe_passed = not (
        job.url_hash
        and Job.objects.filter(url_hash=job.url_hash, is_archived=False).exclude(pk=job.pk).exists()
    )
    checks = {
        "active_posting": bool(job.original_link_is_live),
        "valid_source_url": has_url,
        "tenant_platform_match": True,
        "dedupe_passed": dedupe_passed,
        "clean_jd_present": has_clean_jd,
        "company_resolved": company_ok,
    }
    reasons: list[str] = []
    if not has_url:
        reasons.append(REASON_MISSING_URL)
    if not job.original_link_is_live:
        reasons.append(REASON_INACTIVE_POSTING)
    if not company_ok:
        reasons.append(
            REASON_BLACKLISTED_COMPANY
            if job.company_obj and getattr(job.company_obj, "is_blacklisted", False)
            else REASON_COMPANY_UNRESOLVED
        )
    if not dedupe_passed:
        reasons.append(REASON_DUPLICATE_RISK)
    if not has_clean_jd:
        reasons.append(REASON_JD_TOO_WEAK)

    hard_passed = not reasons
    val = _clamp01(_as_float(job.validation_score, 0.0) / 100.0)
    quality = _clamp01(_as_float(job.quality_score, 0.0))
    data_quality = _clamp01((0.6 * quality) + (0.4 * val))
    trust = _clamp01((0.6 if job.original_link_is_live else 0.2) + (0.2 if dedupe_passed else 0.0) + (0.2 if company_ok else 0.0))
    fit = _clamp01(sum(1 for x in [title_ok, has_clean_jd, bool((job.location or "").strip()), bool((job.salary_range or "").strip())] if x) / 4.0)
    vet_priority = _clamp01((0.45 * data_quality) + (0.35 * trust) + (0.20 * fit))

    if not hard_passed:
        lane = "BLOCKED"
        status = "BLOCKED"
    elif vet_priority >= 0.82 and data_quality >= 0.72 and trust >= 0.70:
        lane = "AUTO"
        status = "ELIGIBLE"
    else:
        lane = "HUMAN"
        status = "REVIEW"

    return GateResult(
        passed=hard_passed,
        reason_code=REASON_OK if hard_passed else reasons[0],
        reasons=reasons,
        checks=checks,
        data_quality_score=round(data_quality, 4),
        trust_score=round(trust, 4),
        candidate_fit_score=round(fit, 4),
        vet_priority_score=round(vet_priority, 4),
        lane=lane,
        status=status,
    )
