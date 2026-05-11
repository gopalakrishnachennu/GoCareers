"""Bridge manually-created Jobs into the RawJob evidence/scope pipeline."""

from __future__ import annotations

import hashlib
import logging
from typing import Literal

from django.db import transaction
from django.utils import timezone

from companies.models import Company
from harvest.enrichments import extract_enrichments
from harvest.location_resolver import evaluate_rawjob_scope, extract_location_candidates
from harvest.models import RawJob, RawJobPayloadSnapshot
from harvest.normalizer import compute_content_hash, compute_url_hash
from harvest.payload_archive import capture_rawjob_payload_snapshot
from harvest.services.enrichment_input import build_enrichment_input
from jobs.classifier.department import classify_department
from jobs.marketing_role_routing import assign_marketing_roles_to_job
from jobs.models import Job

logger = logging.getLogger(__name__)

ManualIngestSource = Literal["manual", "bulk_upload"]

_MANUAL_PLATFORM_SLUGS = {"manual", "bulk_upload"}


def _safe_company_name(job: Job) -> str:
    return (job.company or "").strip()[:255] or f"Manual Job {job.pk}"


def _company_for_job(job: Job) -> Company:
    if job.company_obj_id and job.company_obj:
        return job.company_obj
    company, _ = Company.objects.get_or_create(name=_safe_company_name(job))
    return company


def _source_slug(source: str) -> str:
    return "bulk_upload" if source == "bulk_upload" else "manual"


def _source_url_hash(job: Job) -> str:
    if (job.original_link or "").strip():
        return compute_url_hash(job.original_link)
    return hashlib.sha256(f"manual-job:{job.pk}".encode("utf-8")).hexdigest()


def _employment_type(job: Job) -> str:
    value = (job.job_type or "").strip().upper()
    valid = {choice[0] for choice in RawJob.EmploymentType.choices}
    return value if value in valid else RawJob.EmploymentType.UNKNOWN


def _manual_payload(job: Job, *, source: str) -> dict:
    return {
        "ingest_source": source,
        "job_id": job.pk,
        "title": job.title,
        "company": job.company,
        "company_obj_id": job.company_obj_id,
        "location": job.location,
        "description": job.description,
        "salary_range": job.salary_range,
        "original_link": job.original_link,
        "job_type": job.job_type,
        "job_source": job.job_source,
        "status": job.status,
        "stage": job.stage,
        "captured_at": timezone.now().isoformat(),
    }


def _raw_defaults(job: Job, company: Company, *, source: str, url_hash: str) -> dict:
    payload = _manual_payload(job, source=source)
    platform_slug = _source_slug(source)
    location_candidates = extract_location_candidates(
        location_raw=job.location or "",
        city="",
        state="",
        country=job.country or "",
        vendor_location_block="",
        raw_payload=payload,
    )
    defaults = {
        "company": company,
        "external_id": f"{platform_slug}-job:{job.pk}",
        "content_hash": compute_content_hash(company.pk, job.title or "", job.location or ""),
        "original_url": (job.original_link or "")[:1024],
        "apply_url": (job.original_link or "")[:1024],
        "title": (job.title or "")[:512],
        "company_name": company.name[:256],
        "location_raw": (job.location or "")[:512],
        "location_candidates": location_candidates,
        "country": (job.country or "")[:128],
        "employment_type": _employment_type(job),
        "salary_raw": (job.salary_range or "")[:256],
        "description": job.description or "",
        "description_clean": job.description or "",
        "has_description": bool((job.description or "").strip()),
        "platform_slug": platform_slug,
        "vendor_job_identification": f"job:{job.pk}",
        "raw_payload": payload,
        "sync_status": RawJob.SyncStatus.SYNCED,
        "is_active": job.status != Job.Status.CLOSED and not job.is_archived,
    }
    enrichment_input = build_enrichment_input(
        defaults,
        overrides={"description": (job.description or "")[:20000]},
        company_name=company.name,
    )
    defaults.update(extract_enrichments(enrichment_input))
    scope_probe = RawJob(url_hash=url_hash, **defaults)
    defaults.update(evaluate_rawjob_scope(scope_probe, use_provider=None, save=False))
    defaults["sync_status"] = RawJob.SyncStatus.SYNCED
    return defaults


def _update_existing_raw(raw_job: RawJob, defaults: dict) -> RawJob:
    can_replace_source_fields = (raw_job.platform_slug or "") in _MANUAL_PLATFORM_SLUGS or not raw_job.platform_slug
    payload = dict(raw_job.raw_payload or {})
    payload["manual_job_bridge"] = defaults.get("raw_payload") or {}
    defaults = dict(defaults)
    defaults["raw_payload"] = payload

    fields_to_update = ["sync_status", "is_active", "raw_payload"]
    if can_replace_source_fields:
        fields_to_update.extend([
            "company", "external_id", "content_hash", "original_url", "apply_url",
            "title", "company_name", "location_raw", "country", "employment_type",
            "salary_raw", "description", "description_clean", "has_description",
            "platform_slug", "vendor_job_identification",
            "country_code", "country_confidence", "country_source", "country_codes",
            "location_candidates", "scope_status", "scope_reason", "is_priority",
            "last_scope_evaluated_at",
            "skills", "tech_stack", "job_category", "normalized_title", "title_keywords",
            "years_required", "years_required_max", "education_required",
            "visa_sponsorship", "work_authorization", "clearance_required", "clearance_level",
            "salary_equity", "signing_bonus", "relocation_assistance",
            "travel_required", "travel_pct_min", "travel_pct_max",
            "schedule_type", "shift_schedule", "shift_details", "hours_hint", "weekend_required",
            "certifications", "licenses_required", "benefits_list", "languages_required",
            "encouraged_to_apply", "job_keywords", "department_normalized",
            "word_count", "quality_score", "jd_quality_score",
            "classification_confidence", "category_confidence", "classification_source",
            "enrichment_version", "classification_provenance", "field_confidence",
            "field_provenance", "resume_ready_score", "description_raw_html",
            "has_html_content", "cleaning_version", "requirements", "responsibilities",
            "job_domain", "job_domain_candidates", "domain_version",
        ])

    for field in dict.fromkeys(fields_to_update):
        if field in defaults:
            setattr(raw_job, field, defaults[field])
    raw_job.save(update_fields=list(dict.fromkeys(fields_to_update)) + ["updated_at"])
    return raw_job


def ensure_rawjob_for_job(
    job: Job,
    *,
    source: ManualIngestSource = "manual",
) -> RawJob | None:
    """
    Create/link a RawJob evidence row for a manually-created or bulk-uploaded Job.

    This keeps manual ingestion visible in the same scope, payload, enrichment,
    and lineage tooling used by ATS/Jarvis ingestion while preserving the existing
    direct Job creation workflow.
    """
    if not job.pk:
        return None

    with transaction.atomic():
        job = (
            Job.objects.select_for_update()
            .select_related("company_obj", "posted_by", "source_raw_job")
            .get(pk=job.pk)
        )
        company = _company_for_job(job)
        url_hash = _source_url_hash(job)
        if not url_hash:
            return None

        defaults = _raw_defaults(job, company, source=source, url_hash=url_hash)
        raw_job = job.source_raw_job
        if raw_job is None:
            raw_job = RawJob.objects.filter(url_hash=url_hash).first()

        if raw_job is None:
            raw_job = RawJob.objects.create(url_hash=url_hash, **defaults)
        else:
            raw_job = _update_existing_raw(raw_job, defaults)

        capture_rawjob_payload_snapshot(
            raw_job,
            payload=defaults.get("raw_payload") or {},
            payload_kind=RawJobPayloadSnapshot.PayloadKind.API_RESPONSE,
            source_url=job.original_link or "",
            platform_slug=raw_job.platform_slug or _source_slug(source),
            source_metadata={
                "ingest": source,
                "job_id": job.pk,
                "bridge": "manual_job_bridge",
            },
        )

        job_updates = []
        if job.source_raw_job_id != raw_job.pk:
            job.source_raw_job = raw_job
            job_updates.append("source_raw_job")
        if job.url_hash != url_hash:
            job.url_hash = url_hash
            job_updates.append("url_hash")
        if not job.company_obj_id:
            job.company_obj = company
            job.company = company.name
            job_updates.extend(["company_obj", "company"])
        if raw_job.country and job.country != raw_job.country:
            job.country = raw_job.country
            job_updates.append("country")

        role_domain = raw_job.job_domain or (raw_job.job_domain_candidates or [""])[0]
        dept, conf, dept_source = classify_department(
            title=job.title or "",
            description=job.description or "",
            role_domain=role_domain,
            company_industry=company.industry or "",
            use_llm=False,
        )
        if dept and (not job.department or job.department_source != "manual"):
            job.department = dept
            job.department_confidence = round(conf, 4)
            job.department_source = dept_source
            job.classified_at = timezone.now()
            job.needs_reclassification = False
            job_updates.extend([
                "department", "department_confidence", "department_source",
                "classified_at", "needs_reclassification",
            ])

        if job_updates:
            job.save(update_fields=list(dict.fromkeys(job_updates)) + ["updated_at"])

        try:
            assign_marketing_roles_to_job(job, raw_job=raw_job)
        except Exception:
            logger.exception("Failed to assign marketing roles for manual job bridge %s", job.pk)

    return raw_job
