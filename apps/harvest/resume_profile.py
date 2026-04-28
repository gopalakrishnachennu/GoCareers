from __future__ import annotations

from dataclasses import dataclass, asdict


@dataclass
class ResumeJobProfile:
    job_id: int
    title: str
    normalized_title: str
    keywords: list[str]
    department: str
    job_category: str
    commitment: str
    experience_level: str
    years_required_min: int | None
    years_required_max: int | None
    education_required: str
    licenses_required: list[str]
    certifications: list[str]
    security_clearance_required: bool
    security_clearance_level: str
    languages_required: list[str]
    schedule_type: str
    shift_details: str
    weekend_required: bool | None
    travel_requirement: str
    travel_pct_min: int | None
    travel_pct_max: int | None
    salary_min: float | None
    salary_max: float | None
    salary_currency: str
    salary_period: str
    salary_raw: str
    country: str
    state: str
    city: str
    location_raw: str
    benefits: list[str]
    encouraged_to_apply: list[str]
    description_clean: str
    description_quality_score: float | None
    classification_confidence: float | None
    resume_ready_score: float | None
    company_name: str
    company_industry: str
    company_stage: str
    company_funding: str
    company_size: str
    company_employee_count_band: str
    company_founding_year: int | None


def _safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_resume_job_profile(raw_job) -> dict:
    """Canonical contract used by resume generation pipelines."""
    profile = ResumeJobProfile(
        job_id=raw_job.pk,
        title=raw_job.title or "",
        normalized_title=raw_job.normalized_title or raw_job.title or "",
        keywords=(raw_job.title_keywords or raw_job.job_keywords or [])[:20],
        department=raw_job.department_normalized or raw_job.department or "",
        job_category=raw_job.job_category or "",
        commitment=raw_job.employment_type or "",
        experience_level=raw_job.experience_level or "",
        years_required_min=raw_job.years_required,
        years_required_max=raw_job.years_required_max,
        education_required=raw_job.education_required or "",
        licenses_required=raw_job.licenses_required or [],
        certifications=raw_job.certifications or [],
        security_clearance_required=bool(raw_job.clearance_required),
        security_clearance_level=raw_job.clearance_level or "",
        languages_required=raw_job.languages_required or [],
        schedule_type=raw_job.schedule_type or "",
        shift_details=raw_job.shift_details or raw_job.shift_schedule or "",
        weekend_required=raw_job.weekend_required,
        travel_requirement=raw_job.travel_required or "",
        travel_pct_min=raw_job.travel_pct_min,
        travel_pct_max=raw_job.travel_pct_max,
        salary_min=_safe_float(raw_job.salary_min),
        salary_max=_safe_float(raw_job.salary_max),
        salary_currency=raw_job.salary_currency or "",
        salary_period=raw_job.salary_period or "",
        salary_raw=raw_job.salary_raw or "",
        country=raw_job.country or "",
        state=raw_job.state or "",
        city=raw_job.city or "",
        location_raw=raw_job.location_raw or "",
        benefits=raw_job.benefits_list or [],
        encouraged_to_apply=raw_job.encouraged_to_apply or [],
        description_clean=raw_job.description_clean or raw_job.description or "",
        description_quality_score=raw_job.jd_quality_score,
        classification_confidence=raw_job.classification_confidence,
        resume_ready_score=raw_job.resume_ready_score,
        company_name=raw_job.company_name or (raw_job.company.name if raw_job.company_id else ""),
        company_industry=raw_job.company_industry or "",
        company_stage=raw_job.company_stage or "",
        company_funding=raw_job.company_funding or "",
        company_size=raw_job.company_size or "",
        company_employee_count_band=raw_job.company_employee_count_band or "",
        company_founding_year=raw_job.company_founding_year,
    )
    return asdict(profile)
