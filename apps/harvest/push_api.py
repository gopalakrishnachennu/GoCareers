"""
Local Harvesting Agent — Production-side receive API.

Three endpoints (all protected by Bearer token = settings.HARVEST_PUSH_SECRET):

  GET  /harvest/api/push/labels/   → Export company+label list for local harvester
  POST /harvest/api/push/jobs/     → Receive enriched RawJobs from local agent
  GET  /harvest/api/push/status/   → Stats on pushed vs synced jobs
"""

import json
import logging
from datetime import date, datetime
import hashlib

from django.conf import settings
from django.core.cache import cache
from django.db import IntegrityError, transaction
from django.db.models import Q
from django.http import JsonResponse
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt

from .normalizer import compute_url_hash

logger = logging.getLogger("harvest.push_api")


# ── Auth ──────────────────────────────────────────────────────────────────────

def _check_token(request) -> bool:
    """Validate Authorization: Bearer <HARVEST_PUSH_SECRET>."""
    secret = getattr(settings, "HARVEST_PUSH_SECRET", "").strip()
    if not secret:
        logger.error("push_api: HARVEST_PUSH_SECRET not set — all push requests will be rejected")
        return False
    auth = request.META.get("HTTP_AUTHORIZATION", "")
    if not auth.startswith("Bearer "):
        return False
    return auth[len("Bearer "):].strip() == secret


def _unauth():
    return JsonResponse({"error": "Unauthorized — check HARVEST_PUSH_SECRET"}, status=401)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_date(val) -> "date | None":
    if not val:
        return None
    if isinstance(val, date):
        return val
    try:
        return date.fromisoformat(str(val)[:10])
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> "float | None":
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> "int | None":
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_list(val) -> list:
    if isinstance(val, list):
        return val
    if isinstance(val, tuple):
        return list(val)
    if val in (None, ""):
        return []
    return [str(val)]


def _resolve_company(company_name: str):
    """Find or create a Company stub by name. Returns Company or None."""
    if not company_name:
        return None
    try:
        import re

        from companies.models import Company
        raw_name = re.sub(r"\s+", " ", company_name).strip(" -_,.")
        company = Company.objects.filter(Q(name__iexact=raw_name) | Q(alias__iexact=raw_name)).first()
        if company:
            return company

        def _name_key(v: str) -> str:
            txt = (v or "").strip().lower().replace("&", " and ")
            txt = re.sub(r"[^a-z0-9]+", " ", txt)
            toks = [t for t in txt.split() if t]
            stop = {
                "the", "inc", "incorporated", "llc", "ltd", "ltda", "corp", "corporation",
                "co", "company", "group", "holdings", "plc", "gmbh", "sa", "bv", "srl",
                "pte", "and", "of", "for", "a", "an", "do", "de", "da",
            }
            reduced = [t for t in toks if t not in stop] or toks
            return " ".join(reduced)

        lookup_key = _name_key(raw_name)
        lookup_compact = lookup_key.replace(" ", "")
        if lookup_key:
            token_q = Q()
            for tok in lookup_key.split()[:3]:
                token_q |= Q(name__icontains=tok) | Q(alias__icontains=tok)
            candidates = Company.objects.filter(token_q) if token_q else Company.objects.all()
            best = None
            for cand in candidates.only("id", "name", "alias").order_by("name")[:300]:
                for cname in (cand.name, cand.alias):
                    cand_key = _name_key(cname)
                    if not cand_key:
                        continue
                    if cand_key == lookup_key or cand_key.replace(" ", "") == lookup_compact:
                        if best is None or len(cand.name) < len(best.name):
                            best = cand
                        break
            if best:
                return best

        company, _ = Company.objects.get_or_create(name=raw_name, defaults={"name": raw_name})
        return company
    except Exception:
        logger.exception("push_api: company lookup failed for %r", company_name)
        return None


def _resolve_platform(slug: str):
    """Find JobBoardPlatform by slug. Returns instance or None."""
    if not slug:
        return None
    try:
        from harvest.models import JobBoardPlatform
        return JobBoardPlatform.objects.filter(slug=slug).first()
    except Exception:
        return None


def _trigger_pipeline():
    """Fire sync-to-pool task via Celery (best-effort, non-blocking)."""
    try:
        from harvest.tasks import sync_harvested_to_pool_task
        sync_harvested_to_pool_task.delay(max_jobs=500)
    except Exception:
        logger.warning("push_api: could not queue sync_harvested_to_pool_task", exc_info=True)


def _invalidate_rawjobs_dashboard_cache() -> None:
    try:
        cache.delete("rawjobs_dashboard_stats")
        cache.delete("rawjobs_expired_missing_jd")
    except Exception:
        pass


# ── View 1: Export labels ─────────────────────────────────────────────────────

@method_decorator(csrf_exempt, name="dispatch")
class ExportLabelsView(View):
    """
    GET /harvest/api/push/labels/
    Returns the list of verified company+platform labels.
    The local harvesting agent uses this to know WHAT to harvest.

    Query params:
      platform  — filter by platform slug (optional)
      limit     — max results (default 2000)
    """

    def get(self, request):
        if not _check_token(request):
            return _unauth()

        from harvest.models import CompanyPlatformLabel

        qs = (
            CompanyPlatformLabel.objects
            .select_related("company", "platform")
            .filter(
                platform__is_enabled=True,
                company__isnull=False,
            )
            .exclude(tenant_id="")
        )

        platform_filter = request.GET.get("platform", "").strip()
        if platform_filter:
            qs = qs.filter(platform__slug=platform_filter)

        try:
            limit = min(int(request.GET.get("limit", 2000)), 5000)
        except (TypeError, ValueError):
            limit = 2000

        qs = qs[:limit]

        labels = []
        for lbl in qs:
            labels.append({
                "company_name": lbl.company.name,
                "career_url": lbl.custom_career_url or lbl.company.career_site_url or "",
                "domain": lbl.company.domain or "",
                "platform_slug": lbl.platform.slug if lbl.platform else "",
                "platform_api_type": lbl.platform.api_type if lbl.platform else "",
                "tenant_id": lbl.tenant_id,
                "is_verified": lbl.is_verified,
                "confidence": lbl.confidence,
            })

        return JsonResponse({"labels": labels, "count": len(labels)})


# ── View 2: Receive pushed jobs ───────────────────────────────────────────────

@method_decorator(csrf_exempt, name="dispatch")
class PushJobsView(View):
    """
    POST /harvest/api/push/jobs/
    Receive a batch of enriched RawJobs from the local harvesting agent.

    Body (JSON):
      {
        "jobs": [...],           # list of serialised RawJob dicts (max 1000)
        "trigger_pipeline": true # queue sync-to-pool after insert (default true)
      }

    Response:
      {"received": N, "created": N, "skipped": N, "errors": N}
    """

    MAX_BATCH = 1000

    def post(self, request):
        if not _check_token(request):
            return _unauth()

        try:
            payload = json.loads(request.body)
        except (json.JSONDecodeError, ValueError):
            return JsonResponse({"error": "Invalid JSON body"}, status=400)

        jobs = payload.get("jobs", [])
        if not isinstance(jobs, list):
            return JsonResponse({"error": "'jobs' must be a JSON array"}, status=400)
        if len(jobs) > self.MAX_BATCH:
            return JsonResponse(
                {"error": f"Max {self.MAX_BATCH} jobs per request, got {len(jobs)}"},
                status=400,
            )

        trigger_pipeline = bool(payload.get("trigger_pipeline", True))

        created = skipped = errors = 0

        # Cache platform lookups to avoid N+1
        _platform_cache: dict[str, object] = {}

        def get_platform(slug):
            if slug not in _platform_cache:
                _platform_cache[slug] = _resolve_platform(slug)
            return _platform_cache[slug]

        from harvest.models import RawJob
        from .enrichments import clean_job_content, clean_job_text, extract_enrichments
        from .location_resolver import evaluate_rawjob_scope, extract_location_candidates

        for job_data in jobs:
            try:
                original_url = job_data.get("original_url", "").strip()
                url_hash = compute_url_hash(original_url)
                if not url_hash:
                    # Fallback to supplied hash only when URL is missing.
                    url_hash = job_data.get("url_hash", "").strip()
                if not url_hash:
                    errors += 1
                    continue

                if RawJob.objects.filter(url_hash=url_hash).exists():
                    skipped += 1
                    continue
                base_url = original_url.split("?", 1)[0].strip()
                if base_url and RawJob.objects.filter(original_url__startswith=base_url).exists():
                    skipped += 1
                    continue
                legacy_hash = hashlib.sha256(original_url.strip().encode("utf-8")).hexdigest() if original_url else ""
                if legacy_hash and legacy_hash != url_hash and RawJob.objects.filter(url_hash=legacy_hash).exists():
                    skipped += 1
                    continue

                company = _resolve_company(job_data.get("company_name", ""))
                if company is None:
                    logger.warning(
                        "push_api: skipping job %s — cannot resolve company %r",
                        url_hash, job_data.get("company_name"),
                    )
                    errors += 1
                    continue

                platform_slug = job_data.get("platform_slug", "").strip()
                platform = get_platform(platform_slug)
                external_id = str(job_data.get("external_id", "")).strip()[:512]
                desc_meta = clean_job_content(job_data.get("description", ""), max_len=50000)
                description = desc_meta["clean_text"]
                requirements = clean_job_text(job_data.get("requirements", ""), max_len=20000)
                benefits = clean_job_text(job_data.get("benefits", ""), max_len=10000)
                enriched = extract_enrichments({
                    "title": job_data.get("title") or "",
                    "description": description,
                    "requirements": requirements,
                    "benefits": benefits,
                    "department": job_data.get("department") or "",
                    "location_raw": job_data.get("location_raw") or "",
                    "employment_type": job_data.get("employment_type") or "",
                    "experience_level": job_data.get("experience_level") or "",
                    "salary_raw": job_data.get("salary_raw") or "",
                    "company_name": job_data.get("company_name") or "",
                    "country": job_data.get("country") or "",
                    "state": job_data.get("state") or "",
                    "posted_date": _parse_date(job_data.get("posted_date")),
                })
                location_candidates = (
                    _safe_list(job_data.get("location_candidates"))
                    or extract_location_candidates(
                        location_raw=job_data.get("location_raw") or "",
                        city=job_data.get("city") or "",
                        state=job_data.get("state") or "",
                        country=job_data.get("country") or "",
                        vendor_location_block=job_data.get("vendor_location_block") or "",
                        raw_payload=job_data.get("raw_payload") or {},
                    )
                )

                # Secondary dedupe guard: same company+platform+external_id.
                if external_id and RawJob.objects.filter(
                    company=company,
                    platform_slug=platform_slug[:64],
                    external_id=external_id,
                ).exists():
                    skipped += 1
                    continue

                rj = RawJob(
                    company=company,
                    job_platform=platform,
                    url_hash=url_hash,
                    external_id=external_id,
                    original_url=original_url[:1024],
                    apply_url=str(job_data.get("apply_url", ""))[:1024],
                    title=str(job_data.get("title", ""))[:512],
                    company_name=str(job_data.get("company_name", ""))[:256],
                    department=str(job_data.get("department", ""))[:256],
                    team=str(job_data.get("team", ""))[:256],
                    location_raw=str(job_data.get("location_raw", ""))[:512],
                    city=str(job_data.get("city", ""))[:128],
                    state=str(job_data.get("state", ""))[:128],
                    country=str(job_data.get("country", ""))[:128],
                    location_candidates=location_candidates,
                    country_codes=_safe_list(job_data.get("country_codes")),
                    postal_code=str(job_data.get("postal_code", ""))[:32],
                    location_type=job_data.get("location_type") or RawJob.LocationType.UNKNOWN,
                    is_remote=bool(job_data.get("is_remote", False)),
                    employment_type=job_data.get("employment_type") or RawJob.EmploymentType.UNKNOWN,
                    experience_level=job_data.get("experience_level") or RawJob.ExperienceLevel.UNKNOWN,
                    salary_min=_safe_float(job_data.get("salary_min")),
                    salary_max=_safe_float(job_data.get("salary_max")),
                    salary_currency=str(job_data.get("salary_currency", "USD"))[:8],
                    salary_period=str(job_data.get("salary_period", ""))[:16],
                    salary_raw=str(job_data.get("salary_raw", ""))[:256],
                    description=description,
                    description_clean=(job_data.get("description_clean") or enriched.get("description_clean") or description)[:50000],
                    description_raw_html=(job_data.get("description_raw_html") or desc_meta.get("raw_html") or "")[:120000],
                    has_html_content=bool(job_data.get("has_html_content", desc_meta.get("has_html_content", False))),
                    cleaning_version=str(job_data.get("cleaning_version") or desc_meta.get("cleaning_version") or "v2")[:20],
                    requirements=requirements,
                    responsibilities=str(job_data.get("responsibilities", ""))[:20000],
                    benefits=benefits,
                    posted_date=_parse_date(job_data.get("posted_date")),
                    closing_date=_parse_date(job_data.get("closing_date")),
                    platform_slug=platform_slug[:64],
                    vendor_job_identification=str(job_data.get("vendor_job_identification", ""))[:128],
                    vendor_job_category=str(job_data.get("vendor_job_category", ""))[:128],
                    vendor_degree_level=str(job_data.get("vendor_degree_level", ""))[:128],
                    vendor_job_schedule=str(job_data.get("vendor_job_schedule", ""))[:128],
                    vendor_job_shift=str(job_data.get("vendor_job_shift", ""))[:128],
                    vendor_location_block=str(job_data.get("vendor_location_block", ""))[:512],
                    raw_payload=job_data.get("raw_payload") or {},
                    # Enrichment (pre-computed on local machine)
                    skills=job_data.get("skills") or enriched.get("skills") or [],
                    tech_stack=job_data.get("tech_stack") or enriched.get("tech_stack") or [],
                    job_category=str(job_data.get("job_category", "") or enriched.get("job_category", ""))[:64],
                    job_domain=str(job_data.get("job_domain", "") or enriched.get("job_domain", ""))[:120],
                    job_domain_candidates=_safe_list(job_data.get("job_domain_candidates") or enriched.get("job_domain_candidates")),
                    domain_version=str(job_data.get("domain_version", "") or enriched.get("domain_version", ""))[:16],
                    normalized_title=str(job_data.get("normalized_title", "") or enriched.get("normalized_title", ""))[:255],
                    years_required=job_data.get("years_required", enriched.get("years_required")),
                    years_required_max=job_data.get("years_required_max", enriched.get("years_required_max")),
                    education_required=str(job_data.get("education_required", "") or enriched.get("education_required", ""))[:12],
                    visa_sponsorship=job_data.get("visa_sponsorship", enriched.get("visa_sponsorship")),
                    work_authorization=str(job_data.get("work_authorization", "") or enriched.get("work_authorization", ""))[:64],
                    clearance_required=bool(job_data.get("clearance_required", enriched.get("clearance_required", False))),
                    clearance_level=str(job_data.get("clearance_level", "") or enriched.get("clearance_level", ""))[:64],
                    salary_equity=bool(job_data.get("salary_equity", enriched.get("salary_equity", False))),
                    signing_bonus=bool(job_data.get("signing_bonus", enriched.get("signing_bonus", False))),
                    relocation_assistance=bool(job_data.get("relocation_assistance", enriched.get("relocation_assistance", False))),
                    travel_required=str(job_data.get("travel_required", "") or enriched.get("travel_required", ""))[:64],
                    travel_pct_min=_safe_int(job_data.get("travel_pct_min", enriched.get("travel_pct_min"))),
                    travel_pct_max=_safe_int(job_data.get("travel_pct_max", enriched.get("travel_pct_max"))),
                    schedule_type=str(job_data.get("schedule_type", "") or enriched.get("schedule_type", ""))[:32],
                    shift_schedule=str(job_data.get("shift_schedule", "") or enriched.get("shift_schedule", ""))[:128],
                    shift_details=str(job_data.get("shift_details", "") or enriched.get("shift_details", ""))[:255],
                    hours_hint=str(job_data.get("hours_hint", "") or enriched.get("hours_hint", ""))[:64],
                    weekend_required=job_data.get("weekend_required", enriched.get("weekend_required")),
                    certifications=job_data.get("certifications") or enriched.get("certifications") or [],
                    licenses_required=job_data.get("licenses_required") or enriched.get("licenses_required") or [],
                    benefits_list=job_data.get("benefits_list") or enriched.get("benefits_list") or [],
                    languages_required=job_data.get("languages_required") or enriched.get("languages_required") or [],
                    encouraged_to_apply=job_data.get("encouraged_to_apply") or enriched.get("encouraged_to_apply") or [],
                    job_keywords=job_data.get("job_keywords") or enriched.get("job_keywords") or [],
                    title_keywords=job_data.get("title_keywords") or enriched.get("title_keywords") or [],
                    department_normalized=str(job_data.get("department_normalized", "") or enriched.get("department_normalized", ""))[:128],
                    word_count=int(job_data.get("word_count", enriched.get("word_count", 0)) or 0),
                    quality_score=_safe_float(job_data.get("quality_score", enriched.get("quality_score"))),
                    jd_quality_score=_safe_float(job_data.get("jd_quality_score", enriched.get("jd_quality_score"))),
                    classification_confidence=_safe_float(job_data.get("classification_confidence", enriched.get("classification_confidence"))),
                    category_confidence=_safe_float(job_data.get("category_confidence", enriched.get("category_confidence"))),
                    classification_source=str(job_data.get("classification_source", "") or enriched.get("classification_source", ""))[:16],
                    classification_provenance=job_data.get("classification_provenance") or enriched.get("classification_provenance") or {},
                    field_confidence=job_data.get("field_confidence") or enriched.get("field_confidence") or {},
                    field_provenance=job_data.get("field_provenance") or enriched.get("field_provenance") or {},
                    resume_ready_score=_safe_float(job_data.get("resume_ready_score", enriched.get("resume_ready_score"))),
                    company_industry=(job_data.get("company_industry") or (company.industry if company else "") or "")[:255],
                    company_stage=(job_data.get("company_stage") or (company.funding_stage if company else "") or "")[:64],
                    company_funding=(job_data.get("company_funding") or (company.funding_amount if company else "") or "")[:128],
                    company_size=(job_data.get("company_size") or (company.size_band if company else "") or (company.headcount_range if company else "") or "")[:64],
                    company_employee_count_band=(job_data.get("company_employee_count_band") or (company.employee_count_band if company else "") or (company.headcount_range if company else "") or "")[:64],
                    company_founding_year=_safe_int(job_data.get("company_founding_year")) or (company.founding_year if company else None),
                    sync_status=RawJob.SyncStatus.PENDING,
                    is_active=True,
                )
                scope_updates = evaluate_rawjob_scope(rj, use_provider=False, save=False)
                for field, value in scope_updates.items():
                    setattr(rj, field, value)
                rj.save()
                created += 1

            except IntegrityError:
                # Race condition: another worker created the same url_hash between our check and save
                skipped += 1
            except Exception:
                logger.exception(
                    "push_api: unexpected error saving job %r", job_data.get("url_hash")
                )
                errors += 1

        if created > 0:
            _invalidate_rawjobs_dashboard_cache()
        if trigger_pipeline and created > 0:
            _trigger_pipeline()

        logger.info(
            "push_api: batch complete — received=%d created=%d skipped=%d errors=%d",
            len(jobs), created, skipped, errors,
        )

        return JsonResponse({
            "received": len(jobs),
            "created": created,
            "skipped": skipped,
            "errors": errors,
        })


# ── View 3: Push status ───────────────────────────────────────────────────────

@method_decorator(csrf_exempt, name="dispatch")
class PushStatusView(View):
    """
    GET /harvest/api/push/status/
    Returns RawJob counts broken down by platform_slug and sync_status.
    Useful for monitoring local-agent push progress from the prod side.
    """

    def get(self, request):
        if not _check_token(request):
            return _unauth()

        from django.db.models import Count
        from harvest.models import RawJob

        by_platform = list(
            RawJob.objects
            .values("platform_slug", "sync_status")
            .annotate(count=Count("id"))
            .order_by("platform_slug", "sync_status")
        )

        totals = {
            "total": RawJob.objects.count(),
            "pending": RawJob.objects.filter(sync_status=RawJob.SyncStatus.PENDING).count(),
            "synced": RawJob.objects.filter(sync_status=RawJob.SyncStatus.SYNCED).count(),
            "skipped": RawJob.objects.filter(sync_status=RawJob.SyncStatus.SKIPPED).count(),
            "failed": RawJob.objects.filter(sync_status=RawJob.SyncStatus.FAILED).count(),
        }

        return JsonResponse({
            **totals,
            "by_platform_status": by_platform,
            "timestamp": timezone.now().isoformat(),
        })
