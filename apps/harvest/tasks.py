import logging
import time
from datetime import timedelta

from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from django.core.cache import cache
from django.db import connection, models, transaction
from django.db.models import F, IntegerField, Q, Value
from django.db.models.functions import Coalesce, Length, Mod, Trim
from django.utils import timezone

from core.task_progress import update_task_progress

# ─── Harvest compliance constants ────────────────────────────────────────────
# Delay between processing each company within a platform run.
# Applies on top of the per-request delay inside each harvester.
INTER_COMPANY_DELAY_API = 1.5        # seconds — API platforms (GH, Lever, Ashby, Workday)
INTER_COMPANY_DELAY_SCRAPE = 5.0     # seconds — HTML scrape platforms
HTML_SCRAPE_PLATFORMS = {"html_scrape", "icims", "taleo", "jobvite", "ultipro",
                         "applicantpro", "applytojob", "theapplicantmanager",
                         "zoho", "recruitee", "breezy", "teamtailor"}

# Circuit breaker — skip a company after this many consecutive fetch failures
MAX_CONSECUTIVE_FAILURES = 3

logger = logging.getLogger(__name__)

# JD backfill parallel workers: locks older than this are treated as stale (worker crash).
BACKFILL_LOCK_STALE_MINUTES = 45
BACKFILL_MAX_PARALLEL = 8


def _invalidate_rawjobs_dashboard_cache() -> None:
    """Ensure Raw Jobs KPI cards refresh quickly after writes."""
    try:
        cache.delete("rawjobs_dashboard_stats")
        cache.delete("rawjobs_expired_missing_jd")
    except Exception:
        pass


def _company_snapshot_fields(company) -> dict:
    """Denormalized company fields copied onto RawJob for fast filtering."""
    if not company:
        return {
            "company_industry": "",
            "company_stage": "",
            "company_funding": "",
            "company_size": "",
            "company_employee_count_band": "",
            "company_founding_year": None,
        }
    return {
        "company_industry": (getattr(company, "industry", "") or "")[:255],
        "company_stage": (getattr(company, "funding_stage", "") or "")[:64],
        "company_funding": (getattr(company, "funding_amount", "") or "")[:128],
        "company_size": (
            (getattr(company, "size_band", "") or "")
            or (getattr(company, "headcount_range", "") or "")
        )[:64],
        "company_employee_count_band": (
            (getattr(company, "employee_count_band", "") or "")
            or (getattr(company, "headcount_range", "") or "")
        )[:64],
        "company_founding_year": getattr(company, "founding_year", None),
    }
def _backfill_inter_job_delay_sec() -> float:
    """Pause between JD fetches in a chunk; Jarvis per-host/global limits handle burst control."""
    from django.conf import settings

    return float(getattr(settings, "HARVEST_BACKFILL_INTER_JOB_DELAY_SEC", 0.05))


def _backfill_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, list):
        return "\n".join(_backfill_str(v) for v in val if v)
    if isinstance(val, dict):
        return str(val.get("text") or val.get("content") or val.get("name") or "")
    return str(val)


def _supports_select_for_update_skip_locked() -> bool:
    return getattr(connection.features, "supports_select_for_update_skip_locked", False)


def _backfill_eligible_queryset(platform_slug: str | None):
    """Rows that still need a JD and are not actively claimed (unless lock is stale).

    Uses the indexed has_description column - avoids a full-table annotation scan.
    The has_description field is kept in sync by save() and by all backfill update
    paths, so it's safe to rely on here.
    """
    from .models import RawJob

    stale_before = timezone.now() - timedelta(minutes=BACKFILL_LOCK_STALE_MINUTES)
    q = (
        RawJob.objects.filter(has_description=False)
        .exclude(original_url="")
        .exclude(original_url__isnull=True)
        .filter(
            Q(jd_backfill_locked_at__isnull=True)
            | Q(jd_backfill_locked_at__lt=stale_before),
        )
    )
    if platform_slug:
        q = q.filter(platform_slug=platform_slug)
    return q


def _claim_backfill_job_batch(
    claim_size: int,
    platform_slug: str | None,
    *,
    shard_index: int = 0,
    shard_count: int = 1,
) -> list:
    """
    Claim up to *claim_size* rows for JD backfill.

    - **PostgreSQL**: ``SELECT … FOR UPDATE SKIP LOCKED`` — parallel chunks may
      all use ``shard_count=1`` and compete for the next rows.
    - **SQLite / no SKIP LOCKED**: use ``shard_index`` + ``shard_count`` with
      ``MOD(pk, shard_count) = shard_index`` so parallel chunks never claim the
      same primary key (safe without row-level skip locked).
    """
    from .models import RawJob

    eligible = _backfill_eligible_queryset(platform_slug)
    sc = max(1, int(shard_count))
    si = int(shard_index) % sc
    if sc > 1:
        eligible = (
            eligible.annotate(
                _bk_shard=Mod(F("pk"), Value(sc, output_field=IntegerField())),
            )
            .filter(_bk_shard=si)
        )

    with transaction.atomic():
        if _supports_select_for_update_skip_locked():
            locked = list(
                eligible.select_for_update(skip_locked=True, of=("self",))
                .order_by("pk")[:claim_size]
            )
        else:
            locked = list(eligible.select_for_update().order_by("pk")[:claim_size])
        if not locked:
            return []
        ids = [j.pk for j in locked]
        now = timezone.now()
        RawJob.objects.filter(pk__in=ids).update(jd_backfill_locked_at=now)
    return list(RawJob.objects.filter(pk__in=ids).order_by("pk"))


@shared_task(bind=True, name="harvest.backfill_platform_labels_from_jobs")
def backfill_platform_labels_from_jobs_task(self):
    """
    Scan all job original_link URLs, detect ATS platform from URL patterns,
    and create/update CompanyPlatformLabel records — no HTTP requests needed.

    Runs after every bulk job import so new companies get labeled immediately.
    """
    from jobs.models import Job
    from companies.models import Company
    from .models import JobBoardPlatform, CompanyPlatformLabel
    from .detectors import URL_PATTERNS, extract_tenant

    update_task_progress(self, current=0, total=0, message="Loading job URLs…")

    platforms = {p.slug: p for p in JobBoardPlatform.objects.filter(is_enabled=True)}
    company_best: dict = {}

    all_jobs = list(
        Job.objects.exclude(original_link="")
        .filter(company_obj__isnull=False)
        .values("company_obj_id", "original_link")
    )
    total_jobs = len(all_jobs)

    update_task_progress(self, current=0, total=total_jobs, message=f"Scanning {total_jobs} job URLs…")

    for idx, job in enumerate(all_jobs, start=1):
        cid = job["company_obj_id"]
        if cid in company_best:
            continue
        raw_url = job["original_link"]
        url = raw_url.lower()
        for slug, patterns in URL_PATTERNS.items():
            for pattern in patterns:
                if pattern in url:
                    company_best[cid] = {
                        "slug": slug,
                        "tenant_id": extract_tenant(slug, raw_url),
                    }
                    break
            if cid in company_best:
                break

        if idx % 200 == 0:
            update_task_progress(
                self,
                current=idx,
                total=total_jobs,
                message=f"Scanned {idx}/{total_jobs} URLs · {len(company_best)} platforms found…",
            )

    matches = len(company_best)
    update_task_progress(self, current=total_jobs, total=total_jobs,
                         message=f"URL scan done — labeling {matches} companies…")

    created = updated = 0
    now = timezone.now()
    items = list(company_best.items())

    for i, (company_id, info) in enumerate(items, start=1):
        platform = platforms.get(info["slug"])
        if not platform:
            continue
        try:
            company = Company.objects.get(pk=company_id)
        except Company.DoesNotExist:
            continue

        _, was_created = CompanyPlatformLabel.objects.update_or_create(
            company=company,
            defaults={
                "platform": platform,
                "confidence": "HIGH",
                "detection_method": "URL_PATTERN",
                "tenant_id": info["tenant_id"],
                "detected_at": now,
                "last_checked_at": now,
            },
        )
        if was_created:
            created += 1
        else:
            updated += 1

        if i % 50 == 0:
            update_task_progress(
                self,
                current=i,
                total=matches,
                message=f"Labeled {i}/{matches} companies ({created} new, {updated} updated)…",
            )

    logger.info(f"backfill_platform_labels_from_jobs: {created} created, {updated} updated")
    return {"created": created, "updated": updated}


@shared_task(bind=True, max_retries=2, name="harvest.detect_company_platforms")
def detect_company_platforms_task(
    self,
    batch_size: int = 200,
    force_recheck: bool = False,
    triggered_user_id: int | None = None,
):
    """
    Run 3-step platform detection for companies without labels (or stale ones).
    Step 1: URL Pattern → Step 2: HTTP HEAD → Step 3: HTML Parse
    Phase 5: audit goes to PipelineEvent instead of HarvestRun.
    """
    from django.contrib.auth import get_user_model
    from jobs.models import PipelineEvent

    from companies.models import Company
    from .models import JobBoardPlatform, CompanyPlatformLabel
    from .detectors import run_detection_pipeline, extract_tenant

    stale_threshold = timezone.now() - timedelta(days=7)

    if force_recheck:
        company_ids = list(Company.objects.values_list("id", flat=True)[:batch_size])
    else:
        stale_ids = list(
            CompanyPlatformLabel.objects.filter(
                last_checked_at__lt=stale_threshold,
                detection_method__in=["UNDETECTED", "HTML_PARSE"],
            ).values_list("company_id", flat=True)
        )
        unlabeled_ids = list(
            Company.objects.exclude(platform_label__isnull=False).values_list("id", flat=True)
        )
        company_ids = list(set(stale_ids + unlabeled_ids))[:batch_size]

    if not company_ids:
        logger.info("No companies need platform detection.")
        return {"detected": 0, "total": 0}

    companies = Company.objects.filter(id__in=company_ids).order_by("id")
    company_list = list(companies)
    total_n = len(company_list)
    detected = 0
    errors: list[str] = []

    update_task_progress(self, current=0, total=total_n, message="Starting platform detection…")

    try:
        for idx, company in enumerate(company_list, start=1):
            try:
                slug, confidence, method = run_detection_pipeline(company)

                platform = None
                tenant_id = ""
                if slug:
                    platform = JobBoardPlatform.objects.filter(slug=slug, is_enabled=True).first()
                    url = company.career_site_url or company.website or ""
                    tenant_id = extract_tenant(slug, url)

                CompanyPlatformLabel.objects.update_or_create(
                    company=company,
                    defaults={
                        "platform": platform,
                        "confidence": confidence,
                        "detection_method": method,
                        "detected_at": timezone.now() if slug else None,
                        "last_checked_at": timezone.now(),
                        "tenant_id": tenant_id,
                    },
                )
                if slug:
                    detected += 1
            except Exception as e:
                msg = f"Company {company.id}: {e}"
                logger.error("Detection failed: %s", msg)
                errors.append(msg[:300])

            time.sleep(2.0)
            update_task_progress(
                self, current=idx, total=total_n,
                message=f"{idx}/{total_n} · {(company.name or str(company.pk))[:60]}",
            )

        status = "SUCCESS" if not errors else ("PARTIAL" if detected else "FAILED")
        PipelineEvent.record(
            task_name="harvest.detect_company_platforms",
            celery_id=self.request.id or "",
            status=PipelineEvent.Status.SUCCESS if status == "SUCCESS" else PipelineEvent.Status.FAILED,
            meta={"detected": detected, "total": len(company_ids), "errors": errors[:10]},
        )
        logger.info("Detection done: %s/%s detected.", detected, len(company_ids))
        return {"detected": detected, "total": len(company_ids)}

    except Exception as e:
        logger.exception("detect_company_platforms_task failed: %s", e)
        PipelineEvent.record(
            task_name="harvest.detect_company_platforms",
            celery_id=self.request.id or "",
            status=PipelineEvent.Status.FAILED,
            error=str(e)[:2000],
            meta={"detected": detected, "total": len(company_ids)},
        )
        raise


@shared_task(bind=True, max_retries=2, name="harvest.harvest_jobs")
def harvest_jobs_task(
    self,
    platform_slug: str | None = None,
    since_hours: int = 24,
    max_companies: int = 50,
    triggered_by: str = "SCHEDULED",
    triggered_user_id: int | None = None,
):
    """Harvest jobs from all enabled platforms → write directly to RawJob (Phase 5)."""
    import hashlib as _hashlib
    from django.contrib.auth import get_user_model
    from jobs.models import PipelineEvent

    from .models import JobBoardPlatform, CompanyPlatformLabel, RawJob
    from .harvesters import get_harvester
    from .normalizer import normalize_job_data
    from .rate_limiter import throttle as _throttle
    from .enrichments import clean_job_content, clean_job_text, extract_enrichments

    tb = triggered_by if triggered_by in ("SCHEDULED", "MANUAL") else "SCHEDULED"

    qs = JobBoardPlatform.objects.filter(is_enabled=True)
    if platform_slug:
        qs = qs.filter(slug=platform_slug)

    total_new = total_dup = total_fail = 0

    for platform in qs:
        labels_qs = CompanyPlatformLabel.objects.filter(
            platform=platform,
            detection_method__in=["URL_PATTERN", "HTTP_HEAD", "HTML_PARSE", "MANUAL"],
        ).select_related("company")[:max_companies]

        labels_list = list(labels_qs)
        if not labels_list:
            continue

        harvester = get_harvester(platform.slug)
        if harvester is None:
            continue
        is_scraper = platform.slug in HTML_SCRAPE_PLATFORMS
        inter_delay = INTER_COMPANY_DELAY_SCRAPE if is_scraper else INTER_COMPANY_DELAY_API

        jobs_new = jobs_dup = jobs_fail = 0
        errors: list[str] = []
        consecutive_failures = 0

        total_l = len(labels_list)
        update_task_progress(self, current=0, total=total_l, message=f"Harvest {platform.name}: starting…")

        for i, label in enumerate(labels_list, start=1):
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.warning("[HARVEST] Circuit breaker on %s after %d failures", platform.name, consecutive_failures)
                break

            company = label.company
            tenant_id = label.tenant_id or ""
            _throttle(platform.slug)
            try:
                raw_jobs = harvester.fetch_jobs(company, tenant_id, since_hours=since_hours)
                if not raw_jobs:
                    consecutive_failures += 1
                else:
                    consecutive_failures = 0

                for raw in raw_jobs:
                    try:
                        normalized = normalize_job_data(raw, platform, company, harvest_run=None)
                        original_url = normalized.get("original_url", "")
                        url_hash = normalized.get("url_hash", "")
                        if not original_url or not url_hash:
                            continue
                        desc_meta = clean_job_content(normalized.get("description_text", ""), max_len=50000)
                        description = desc_meta["clean_text"]
                        requirements = clean_job_text(normalized.get("requirements_text", ""), max_len=20000)
                        benefits = clean_job_text(normalized.get("benefits_text", ""), max_len=10000)
                        enriched = extract_enrichments({
                            "title": normalized.get("title", ""),
                            "description": description,
                            "description_clean": (enriched.get("description_clean") or description)[:50000],
                            "description_raw_html": (desc_meta.get("raw_html") or "")[:120000],
                            "has_html_content": bool(desc_meta.get("has_html_content")),
                            "cleaning_version": (desc_meta.get("cleaning_version") or "v2")[:20],
                            "requirements": requirements,
                            "benefits": benefits,
                            "department": normalized.get("department", ""),
                            "location_raw": normalized.get("location", ""),
                            "employment_type": normalized.get("job_type", "UNKNOWN"),
                            "experience_level": "UNKNOWN",
                            "salary_raw": normalized.get("salary_raw", ""),
                            "company_name": normalized.get("company_name", company.name),
                            "posted_date": normalized.get("posted_date"),
                        })

                        # Map HarvestedJob-shaped dict → RawJob fields
                        rj_defaults = {
                            "company": company,
                            "job_platform": platform,
                            "platform_slug": platform.slug,
                            "external_id": normalized.get("external_id", "")[:512],
                            "original_url": original_url[:1024],
                            "title": normalized.get("title", "")[:512],
                            "company_name": normalized.get("company_name", company.name)[:256],
                            "location_raw": normalized.get("location", "")[:512],
                            "is_remote": normalized.get("is_remote", False) or False,
                            "employment_type": normalized.get("job_type", "UNKNOWN"),
                            "department": normalized.get("department", "")[:256],
                            "salary_min": normalized.get("salary_min"),
                            "salary_max": normalized.get("salary_max"),
                            "salary_currency": normalized.get("salary_currency", "USD")[:8],
                            "salary_raw": normalized.get("salary_raw", "")[:256],
                            "description": description,
                            "requirements": requirements,
                            "benefits": benefits,
                            "posted_date": normalized.get("posted_date"),
                            "raw_payload": normalized.get("raw_payload", {}),
                            "sync_status": "PENDING",
                            "is_active": True,
                            **_company_snapshot_fields(company),
                            **enriched,
                        }
                        _, created = RawJob.objects.update_or_create(
                            url_hash=url_hash,
                            defaults=rj_defaults,
                        )
                        if created:
                            jobs_new += 1
                        else:
                            jobs_dup += 1
                    except Exception as e:
                        jobs_fail += 1
                        errors.append(str(e)[:200])

            except Exception as e:
                jobs_fail += 1
                consecutive_failures += 1
                errors.append(f"Company {company.id} ({company.name}): {str(e)[:150]}")

            time.sleep(inter_delay)
            update_task_progress(self, current=i, total=total_l, message=f"{platform.name}: {i}/{total_l}")

        total_new += jobs_new; total_dup += jobs_dup; total_fail += jobs_fail

        # Audit via PipelineEvent instead of HarvestRun
        status = "SUCCESS" if not errors else ("PARTIAL" if jobs_new > 0 else "FAILED")
        PipelineEvent.record(
            task_name="harvest.harvest_jobs",
            celery_id=self.request.id or "",
            status=PipelineEvent.Status.SUCCESS if status == "SUCCESS" else PipelineEvent.Status.FAILED,
            meta={"platform": platform.slug, "new": jobs_new, "dup": jobs_dup, "fail": jobs_fail,
                  "errors": errors[:10], "trigger": tb},
        )
        platform.last_harvested_at = timezone.now()
        platform.save(update_fields=["last_harvested_at"])
        logger.info("Harvest %s: +%d new, %d dup, %d fail", platform.name, jobs_new, jobs_dup, jobs_fail)

    return {"new": total_new, "dup": total_dup, "fail": total_fail}


@shared_task(bind=True, name="harvest.check_portal_health")
def check_portal_health_task(self, label_pk: int):
    """
    HTTP-check a single career portal URL and update portal_alive + portal_last_verified.
    Called individually per label — queue many at once via verify_all_portals_task.
    """
    import requests
    from .models import CompanyPlatformLabel

    try:
        label = CompanyPlatformLabel.objects.select_related("platform").get(pk=label_pk)
    except CompanyPlatformLabel.DoesNotExist:
        return

    from .career_url import build_career_url
    url = build_career_url(
        label.platform.slug if label.platform else "",
        label.tenant_id or "",
    )
    if not url:
        return

    alive = False
    try:
        resp = requests.head(
            url,
            timeout=12,
            allow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; GoCareers-PortalBot/1.0; "
                    "+https://chennu.co)"
                )
            },
        )
        # Treat 2xx and 3xx (after redirect) as alive; 4xx/5xx as down
        if resp.status_code >= 400:
            # Some ATS block HEAD — retry with GET (just first bytes)
            resp = requests.get(
                url,
                timeout=15,
                stream=True,
                allow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; GoCareers-PortalBot/1.0)"
                    )
                },
            )
            resp.close()
        alive = resp.status_code < 400
    except Exception:
        alive = False

    label.portal_alive = alive
    label.portal_last_verified = timezone.now()
    label.save(update_fields=["portal_alive", "portal_last_verified"])


@shared_task(bind=True, name="harvest.verify_all_portals")
def verify_all_portals_task(self):
    """
    Queue HTTP health checks for all CompanyPlatformLabels that have a career URL.
    Each check runs asynchronously via check_portal_health_task.
    """
    from .models import CompanyPlatformLabel

    update_task_progress(self, current=0, total=0, message="Queuing portal health checks…")

    label_pks = list(
        CompanyPlatformLabel.objects.filter(
            platform__isnull=False,
        ).exclude(tenant_id="").exclude(tenant_id__isnull=True)
        .values_list("pk", flat=True)
    )

    total = len(label_pks)
    update_task_progress(self, current=0, total=total, message=f"Queuing {total} checks…")

    for i, pk in enumerate(label_pks, start=1):
        check_portal_health_task.apply_async(
            args=[pk],
            countdown=i * 0.3,   # stagger by 0.3s each to avoid hammering
        )
        if i % 50 == 0:
            update_task_progress(
                self, current=i, total=total,
                message=f"Queued {i}/{total} checks…",
            )

    update_task_progress(self, current=total, total=total,
                         message=f"✅ All {total} portal checks queued!")
    return {"queued": total}


@shared_task(
    bind=True,
    name="harvest.fetch_raw_jobs_for_company",
    max_retries=2,
    default_retry_delay=60,
    # soft_time_limit / time_limit / rate_limit are all read from HarvestEngineConfig
    # at task startup — see the body of the function.  These decorator-level values
    # are the safe fallbacks used only if the DB read fails.
    soft_time_limit=480,
    time_limit=600,
    rate_limit="6/m",
)
def fetch_raw_jobs_for_company_task(
    self,
    label_pk: int,
    batch_id: int = None,
    triggered_by: str = "MANUAL",
    max_jobs: int | None = None,
    since_hours: int | None = None,
    fetch_all: bool = False,
):
    """
    Fetch ALL jobs for a single CompanyPlatformLabel and upsert into RawJob.
    Creates a CompanyFetchRun audit record. Updates FetchBatch counters if batch_id given.
    """
    import hashlib
    import requests
    from datetime import date

    from .models import CompanyPlatformLabel, CompanyFetchRun, FetchBatch, HarvestEngineConfig, RawJob
    from .harvesters import get_harvester
    from .normalizer import compute_url_hash

    # ── Read live config from DB — overrides decorator-level fallbacks ────────
    try:
        _cfg = HarvestEngineConfig.get()
        # Apply the current rate limit to THIS worker's slot for this task type.
        # This ensures the DB value is always honoured even after a live GUI change.
        self.rate_limit = f"{_cfg.task_rate_limit}/m"
        _soft_limit = _cfg.task_soft_time_limit_secs
        _hard_limit = _soft_limit + 120
    except Exception:
        _soft_limit = 480
        _hard_limit = 600

    # ── Load label ────────────────────────────────────────────────────────────
    try:
        label = CompanyPlatformLabel.objects.select_related("platform", "company").get(pk=label_pk)
    except CompanyPlatformLabel.DoesNotExist:
        logger.warning("fetch_raw_jobs_for_company_task: label %s not found", label_pk)
        return

    batch = None
    if batch_id:
        batch = FetchBatch.objects.filter(pk=batch_id).first()

    # ── Create run record ─────────────────────────────────────────────────────
    run = CompanyFetchRun.objects.create(
        label=label,
        batch=batch,
        status=CompanyFetchRun.Status.RUNNING,
        task_id=self.request.id or "",
        started_at=timezone.now(),
        triggered_by=triggered_by,
    )
    try:
        self.update_state(
            state="PROGRESS",
            meta={"percent": 5, "message": "Starting company fetch…"},
        )
    except Exception:
        pass

    # ── Guard: no tenant or no platform ──────────────────────────────────────
    if not label.platform or not label.tenant_id:
        run.status = CompanyFetchRun.Status.SKIPPED
        run.error_type = CompanyFetchRun.ErrorType.NO_TENANT
        run.error_message = "No platform or tenant_id configured."
        run.completed_at = timezone.now()
        run.save(update_fields=["status", "error_type", "error_message", "completed_at"])
        if batch:
            FetchBatch.objects.filter(pk=batch.pk).update(
                failed_companies=models.F("failed_companies") + 1
            )
        return

    harvester = get_harvester(label.platform.slug)
    if harvester is None:
        run.status = CompanyFetchRun.Status.SKIPPED
        run.error_type = CompanyFetchRun.ErrorType.PLATFORM_ERROR
        run.error_message = f"No harvester for platform slug: {label.platform.slug}"
        run.completed_at = timezone.now()
        run.save(update_fields=["status", "error_type", "error_message", "completed_at"])
        if batch:
            FetchBatch.objects.filter(pk=batch.pk).update(
                failed_companies=models.F("failed_companies") + 1
            )
        return

    # ── Fetch ─────────────────────────────────────────────────────────────────
    # Scraper platforms (HTML-based) can't filter by date — always fetch all.
    # API platforms support since_hours for incremental fetches (default: 25h window).
    # test_mode passes max_jobs — skip full pagination to stay fast + respectful.
    # adp uses HTMLScrapeHarvester (not a JSON API) — include in scraper set
    # for proper rate-limiting and fetch_all behavior
    SCRAPER_SLUGS = {"jobvite", "icims", "taleo", "applicantpro", "applytojob",
                     "theapplicantmanager", "zoho", "breezy", "teamtailor", "adp"}
    platform_slug_val = label.platform.slug if label.platform else ""
    is_scraper_platform = platform_slug_val in SCRAPER_SLUGS

    # fetch_all logic:
    #   fetch_all=True  → always paginate through ALL pages, ignore since_hours filter
    #                     (used by FETCH ALL button for initial/full crawl)
    #   fetch_all=False → use since_hours window (fast daily incremental)
    #   scrapers        → always fetch_all (no date filter exists in HTML)
    #   test mode       → fetch_all so we get real data, but capped by max_jobs
    use_fetch_all = fetch_all or is_scraper_platform or (max_jobs is not None)
    effective_since_hours = since_hours if since_hours is not None else 25

    # Phase 3: honor PlatformConfig.inter_request_delay_ms before each fetch.
    from .rate_limiter import throttle as _throttle
    _throttle(label.platform.slug)
    try:
        self.update_state(
            state="PROGRESS",
            meta={"percent": 12, "message": "Connecting to company board…"},
        )
    except Exception:
        pass

    try:
        if is_scraper_platform:
            # HTML scrapers have no date filter — always fetch everything
            raw_jobs = harvester.fetch_jobs(
                label.company,
                label.tenant_id,
                fetch_all=True,
            )
        elif use_fetch_all:
            # Full crawl: get ALL jobs from this company, all pages, ignore time filter
            raw_jobs = harvester.fetch_jobs(
                label.company,
                label.tenant_id,
                fetch_all=True,
            )
        else:
            # Incremental: only jobs updated in the last N hours (fast daily run)
            raw_jobs = harvester.fetch_jobs(
                label.company,
                label.tenant_id,
                since_hours=effective_since_hours,
                fetch_all=False,
            )
        # Capture API-reported total (even when we only fetched a subset)
        run.jobs_total_available = getattr(harvester, "last_total_available", 0) or len(raw_jobs)
        run.jobs_found = len(raw_jobs)
        run.save(update_fields=["jobs_total_available", "jobs_found"])
        try:
            self.update_state(
                state="PROGRESS",
                meta={
                    "percent": 30 if raw_jobs else 95,
                    "message": f"Discovered {len(raw_jobs)} jobs. Processing…",
                    "jobs_found": len(raw_jobs),
                },
            )
        except Exception:
            pass
    except requests.exceptions.Timeout as exc:
        run.status = CompanyFetchRun.Status.FAILED
        run.error_type = CompanyFetchRun.ErrorType.TIMEOUT
        run.error_message = str(exc)[:500]
        run.completed_at = timezone.now()
        run.save(update_fields=["status", "error_type", "error_message", "completed_at"])
        if batch:
            FetchBatch.objects.filter(pk=batch.pk).update(
                failed_companies=models.F("failed_companies") + 1
            )
        return
    except requests.exceptions.HTTPError as exc:
        run.status = CompanyFetchRun.Status.FAILED
        run.error_type = CompanyFetchRun.ErrorType.HTTP_ERROR
        run.error_message = str(exc)[:500]
        run.completed_at = timezone.now()
        run.save(update_fields=["status", "error_type", "error_message", "completed_at"])
        if batch:
            FetchBatch.objects.filter(pk=batch.pk).update(
                failed_companies=models.F("failed_companies") + 1
            )
        return
    except SoftTimeLimitExceeded:
        # Task hit the 8-minute soft limit — mark as PARTIAL so the run is visible
        # in the monitor and doesn't retry (it was already too slow once).
        run.status = CompanyFetchRun.Status.PARTIAL
        run.error_message = "Soft time limit exceeded (8 min) — task killed gracefully."
        run.completed_at = timezone.now()
        run.save(update_fields=["status", "error_message", "completed_at"])
        if batch:
            FetchBatch.objects.filter(pk=batch.pk).update(
                failed_companies=models.F("failed_companies") + 1
            )
        logger.warning("fetch_raw_jobs_for_company_task: soft time limit hit for label %s", label_pk)
        return
    except Exception as exc:
        run.status = CompanyFetchRun.Status.FAILED
        run.error_type = CompanyFetchRun.ErrorType.PARSE_ERROR
        run.error_message = str(exc)[:500]
        run.completed_at = timezone.now()
        run.save(update_fields=["status", "error_type", "error_message", "completed_at"])
        if batch:
            FetchBatch.objects.filter(pk=batch.pk).update(
                failed_companies=models.F("failed_companies") + 1
            )
        logger.exception("fetch_raw_jobs_for_company_task failed for label %s: %s", label_pk, exc)
        return

    # ── Upsert jobs ───────────────────────────────────────────────────────────
    jobs_new = jobs_updated = jobs_duplicate = jobs_failed = 0
    upsert_errors: list[str] = []
    new_raw_job_pks: list[int] = []  # PKs of freshly-created RawJobs for auto-pipeline

    # In test mode, cap to max_jobs so we don't write hundreds of rows
    if max_jobs and len(raw_jobs) > max_jobs:
        raw_jobs = raw_jobs[:max_jobs]

    total_jobs = len(raw_jobs)
    from .enrichments import clean_job_content, clean_job_text, extract_enrichments
    for idx, job_dict in enumerate(raw_jobs, start=1):
        try:
            original_url = (job_dict.get("original_url") or "").strip()
            if not original_url:
                jobs_failed += 1
                continue

            url_hash = compute_url_hash(original_url)
            if not url_hash:
                jobs_failed += 1
                continue
            external_id = (job_dict.get("external_id") or "").strip()[:512]

            # Parse posted_date
            posted_date = None
            posted_raw = job_dict.get("posted_date_raw", "")
            if posted_raw:
                try:
                    # Handle ISO format: 2024-01-15T00:00:00Z or 2024-01-15
                    posted_date = date.fromisoformat(
                        posted_raw[:10].replace("Z", "")
                    )
                except Exception:
                    pass

            # Parse closing_date
            closing_date = None
            closing_raw = job_dict.get("closing_date", "")
            if closing_raw:
                try:
                    closing_date = date.fromisoformat(closing_raw[:10])
                except Exception:
                    pass

            desc_meta = clean_job_content(job_dict.get("description") or "", max_len=50000)
            description = desc_meta["clean_text"]
            requirements = clean_job_text(job_dict.get("requirements") or "", max_len=20000)
            benefits = clean_job_text(job_dict.get("benefits") or "", max_len=10000)
            enriched = extract_enrichments({
                "title": job_dict.get("title") or "",
                "description": description,
                "requirements": requirements,
                "benefits": benefits,
                "department": job_dict.get("department") or "",
                "location_raw": job_dict.get("location_raw") or "",
                "employment_type": job_dict.get("employment_type") or "",
                "experience_level": job_dict.get("experience_level") or "",
                "salary_raw": job_dict.get("salary_raw") or "",
                "company_name": job_dict.get("company_name") or label.company.name,
                "country": job_dict.get("country") or "",
                "state": job_dict.get("state") or "",
                "posted_date": posted_date,
            })

            defaults = {
                "company": label.company,
                "platform_label": label,
                "job_platform": label.platform,
                "external_id": external_id,
                "original_url": original_url[:1024],
                "apply_url": (job_dict.get("apply_url") or "")[:1024],
                "title": (job_dict.get("title") or "")[:512],
                "company_name": (job_dict.get("company_name") or label.company.name)[:256],
                "department": (job_dict.get("department") or "")[:256],
                "team": (job_dict.get("team") or "")[:256],
                "location_raw": (job_dict.get("location_raw") or "")[:512],
                "city": (job_dict.get("city") or "")[:128],
                "state": (job_dict.get("state") or "")[:128],
                "country": (job_dict.get("country") or "")[:128],
                "location_type": job_dict.get("location_type", "UNKNOWN"),
                "is_remote": bool(job_dict.get("is_remote", False)),
                "employment_type": job_dict.get("employment_type", "UNKNOWN"),
                "experience_level": job_dict.get("experience_level", "UNKNOWN"),
                "salary_min": job_dict.get("salary_min"),
                "salary_max": job_dict.get("salary_max"),
                "salary_currency": (job_dict.get("salary_currency") or "USD")[:8],
                "salary_period": (job_dict.get("salary_period") or "")[:16],
                "salary_raw": (job_dict.get("salary_raw") or "")[:256],
                "description": description,
                "description_clean": (enriched.get("description_clean") or description)[:50000],
                "description_raw_html": (desc_meta.get("raw_html") or "")[:120000],
                "has_html_content": bool(desc_meta.get("has_html_content")),
                "cleaning_version": (desc_meta.get("cleaning_version") or "v2")[:20],
                "requirements": requirements,
                "benefits": benefits,
                "posted_date": posted_date,
                "closing_date": closing_date,
                "platform_slug": (label.platform.slug if label.platform else "")[:64],
                "raw_payload": job_dict.get("raw_payload") or {},
                "is_active": True,
                **_company_snapshot_fields(label.company),
                **enriched,
            }

            # ── Dedup guard (ATS external_id): same label+external_id = same job ──
            # Some platforms append tracker params or alternate paths to the same job.
            # We treat external_id as stable identity when present and update in place.
            existing_by_external = None
            if external_id:
                ext_q = RawJob.objects.filter(
                    company=label.company,
                    external_id=external_id,
                )
                platform_slug_match = (label.platform.slug if label.platform else "")[:64]
                if platform_slug_match:
                    ext_q = ext_q.filter(
                        Q(platform_label=label)
                        | Q(job_platform=label.platform)
                        | Q(platform_slug=platform_slug_match)
                        | Q(job_platform__isnull=True)
                    )
                existing_by_external = (
                    ext_q
                    .order_by("pk")
                    .first()
                )
            if existing_by_external:
                if existing_by_external.sync_status == "SYNCED":
                    jobs_duplicate += 1
                    continue
                hash_owned_elsewhere = (
                    RawJob.objects.filter(url_hash=url_hash)
                    .exclude(pk=existing_by_external.pk)
                    .values_list("pk", flat=True)
                    .first()
                )
                if hash_owned_elsewhere:
                    jobs_duplicate += 1
                    continue
                for field, val in defaults.items():
                    setattr(existing_by_external, field, val)
                existing_by_external.url_hash = url_hash
                existing_by_external.save()
                jobs_updated += 1
                continue

            # ── Query-variant reconciliation: same path, tracker query changed ──
            base_url = original_url.split("?", 1)[0].strip()
            if base_url:
                variant_q = RawJob.objects.filter(
                    company=label.company,
                    original_url__startswith=base_url,
                )
                platform_slug_match = (label.platform.slug if label.platform else "")[:64]
                if platform_slug_match:
                    variant_q = variant_q.filter(
                        Q(platform_label=label)
                        | Q(job_platform=label.platform)
                        | Q(platform_slug=platform_slug_match)
                        | Q(job_platform__isnull=True)
                    )
                variant_row = (
                    variant_q
                    .order_by("pk")
                    .first()
                )
                if variant_row and variant_row.url_hash != url_hash:
                    if variant_row.sync_status == "SYNCED":
                        jobs_duplicate += 1
                        continue
                    hash_owned_elsewhere = (
                        RawJob.objects.filter(url_hash=url_hash)
                        .exclude(pk=variant_row.pk)
                        .values_list("pk", flat=True)
                        .first()
                    )
                    if hash_owned_elsewhere:
                        jobs_duplicate += 1
                        continue
                    for field, val in defaults.items():
                        setattr(variant_row, field, val)
                    variant_row.url_hash = url_hash
                    variant_row.save()
                    jobs_updated += 1
                    continue

            # ── Legacy hash reconciliation: migrate old non-canonical hash in place ──
            legacy_hash = hashlib.sha256(original_url.encode("utf-8")).hexdigest()
            if legacy_hash and legacy_hash != url_hash:
                legacy_row = RawJob.objects.filter(url_hash=legacy_hash).order_by("pk").first()
                if legacy_row:
                    if legacy_row.sync_status == "SYNCED":
                        jobs_duplicate += 1
                        continue
                    hash_owned_elsewhere = (
                        RawJob.objects.filter(url_hash=url_hash)
                        .exclude(pk=legacy_row.pk)
                        .values_list("pk", flat=True)
                        .first()
                    )
                    if hash_owned_elsewhere:
                        jobs_duplicate += 1
                        continue
                    for field, val in defaults.items():
                        setattr(legacy_row, field, val)
                    legacy_row.url_hash = url_hash
                    legacy_row.save()
                    jobs_updated += 1
                    continue

            # ── Dedup guard: never overwrite a SYNCED job ────────────────────
            # If this URL is already in the pool (sync_status=SYNCED), there is
            # nothing to do — don't reset its sync status or overwrite its data.
            existing_synced = RawJob.objects.filter(
                url_hash=url_hash, sync_status="SYNCED"
            ).values_list("pk", flat=True).first()
            if existing_synced:
                jobs_duplicate += 1
                continue

            obj, created = RawJob.objects.update_or_create(
                url_hash=url_hash,
                defaults=defaults,
            )
            if created:
                jobs_new += 1
                new_raw_job_pks.append(obj.pk)
            else:
                jobs_updated += 1

        except Exception as exc:
            jobs_failed += 1
            err_str = f"{type(exc).__name__}: {exc}"
            logger.error("RawJob upsert failed for label %s: %s", label_pk, err_str)
            if len(upsert_errors) < 5:
                upsert_errors.append(err_str[:300])

        if idx == 1 or idx % 5 == 0 or idx == total_jobs:
            run.jobs_new = jobs_new
            run.jobs_updated = jobs_updated
            run.jobs_duplicate = jobs_duplicate
            run.jobs_failed = jobs_failed
            run.save(update_fields=["jobs_new", "jobs_updated", "jobs_duplicate", "jobs_failed"])
            try:
                pct = 35 + int((idx / max(total_jobs, 1)) * 60)
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "percent": min(95, max(35, pct)),
                        "message": f"Processing jobs… {idx}/{total_jobs}",
                        "jobs_found": total_jobs,
                        "jobs_new": jobs_new,
                        "jobs_updated": jobs_updated,
                        "jobs_duplicate": jobs_duplicate,
                        "jobs_failed": jobs_failed,
                    },
                )
            except Exception:
                pass

    # ── Update run record ─────────────────────────────────────────────────────
    run.status = (
        CompanyFetchRun.Status.SUCCESS
        if jobs_failed == 0
        else (CompanyFetchRun.Status.PARTIAL if (jobs_new + jobs_updated) > 0 else CompanyFetchRun.Status.FAILED)
    )
    run.jobs_found = len(raw_jobs)
    run.jobs_new = jobs_new
    run.jobs_updated = jobs_updated
    run.jobs_duplicate = jobs_duplicate
    run.jobs_failed = jobs_failed
    run.completed_at = timezone.now()
    if upsert_errors and not run.error_message:
        run.error_message = "Upsert errors: " + " | ".join(upsert_errors)
        run.error_type = CompanyFetchRun.ErrorType.PARSE_ERROR
    run.save(update_fields=[
        "status", "jobs_found", "jobs_total_available", "jobs_new", "jobs_updated",
        "jobs_duplicate", "jobs_failed", "completed_at", "error_message", "error_type",
    ])

    # ── Update batch counters + auto-complete ────────────────────────────────
    _batch_just_finished = False
    if batch:
        if run.status in (CompanyFetchRun.Status.SUCCESS, CompanyFetchRun.Status.PARTIAL):
            FetchBatch.objects.filter(pk=batch.pk).update(
                completed_companies=models.F("completed_companies") + 1,
                total_jobs_found=models.F("total_jobs_found") + len(raw_jobs),
                total_jobs_new=models.F("total_jobs_new") + jobs_new,
            )
        else:
            FetchBatch.objects.filter(pk=batch.pk).update(
                failed_companies=models.F("failed_companies") + 1,
            )

        # Auto-complete the batch when every child task has reported back.
        # Use a conditional UPDATE (WHERE status=RUNNING) so exactly ONE worker
        # wins the race — only that worker triggers the post-batch sync.
        refreshed = FetchBatch.objects.filter(pk=batch.pk).values(
            "total_companies", "completed_companies", "failed_companies"
        ).first()
        if refreshed:
            done = refreshed["completed_companies"] + refreshed["failed_companies"]
            total_co = refreshed["total_companies"]
            if total_co > 0 and done >= total_co:
                final_status = (
                    FetchBatch.Status.COMPLETED
                    if refreshed["failed_companies"] == 0
                    else FetchBatch.Status.PARTIAL
                )
                wrote = FetchBatch.objects.filter(
                    pk=batch.pk, status=FetchBatch.Status.RUNNING
                ).update(status=final_status, completed_at=timezone.now())
                if wrote == 1:
                    _batch_just_finished = True

    logger.info(
        "fetch_raw_jobs: label=%s new=%d updated=%d enriched_inline=pending failed=%d",
        label_pk, jobs_new, jobs_updated, jobs_failed,
    )

    # ── Inline pipeline: enrich only (safe — pure Python, zero HTTP) ────────────
    #
    # Enrich runs inline because it's pure Python regex/keyword extraction — ~1 ms/job,
    # no HTTP, no Playwright, no CPU spikes.
    #
    # JD backfill (Jarvis/Playwright/HTTP) does NOT run inline — it would spike CPU and
    # cause 502s on the app server.  It fires as a separate background task below,
    # scoped to THIS platform so it's focused and doesn't scan the whole DB.
    #
    # Gated by HarvestEngineConfig flags so either step can be disabled from the GUI.
    if new_raw_job_pks:
        try:
            from .models import HarvestEngineConfig as _EngCfg, RawJob
            from .enrichments import extract_enrichments

            _pipe_cfg = _EngCfg.get()

            # Re-load only the jobs we just created
            new_jobs = list(
                RawJob.objects.filter(pk__in=new_raw_job_pks).select_related("company")
            )

            # ── Inline enrich (pure Python, ~1 ms/job, no HTTP) ─────────────
            enriched = 0
            if _pipe_cfg.auto_enrich and new_jobs:
                ENRICH_FIELDS = [
                    "skills", "tech_stack", "job_category",
                    "normalized_title", "title_keywords",
                    "years_required", "years_required_max", "education_required",
                    "visa_sponsorship", "work_authorization", "clearance_required", "clearance_level",
                    "salary_equity", "signing_bonus", "relocation_assistance",
                    "travel_required", "travel_pct_min", "travel_pct_max",
                    "schedule_type", "shift_schedule", "shift_details", "hours_hint", "weekend_required",
                    "certifications", "licenses_required", "benefits_list",
                    "languages_required", "encouraged_to_apply",
                    "job_keywords", "department_normalized",
                    "word_count", "quality_score", "jd_quality_score",
                    "classification_confidence", "classification_provenance",
                    "field_confidence", "field_provenance",
                    "resume_ready_score", "description_clean", "description_raw_html",
                    "has_html_content", "cleaning_version",
                ]
                bulk_enrich: list[RawJob] = []
                for job in new_jobs:
                    if job.skills or job.job_category:
                        continue  # already enriched
                    enriched_data = extract_enrichments({
                        "title":        job.title or "",
                        "description":  job.description or "",
                        "requirements": job.requirements or "",
                    })
                    for f in ENRICH_FIELDS:
                        if f in enriched_data:
                            setattr(job, f, enriched_data[f])
                    bulk_enrich.append(job)
                    enriched += 1
                if bulk_enrich:
                    RawJob.objects.bulk_update(bulk_enrich, ENRICH_FIELDS)

            logger.info(
                "Inline enrich done: label=%s new_jobs=%d enriched=%d",
                label_pk, len(new_jobs), enriched,
            )

            # ── Background JD backfill — scoped to this platform, fires once ─
            # This queues a SINGLE background task for the platform just harvested.
            # The backfill task controls its own parallelism and rate limits so it
            # never spikes CPU.  Only queued if there are new jobs without descriptions.
            platform_s = (label.platform.slug if label and label.platform else "") or ""
            if _pipe_cfg.auto_backfill_jd:
                needs_jd_count = sum(1 for j in new_jobs if not (j.description or "").strip())
                if needs_jd_count > 0:
                    backfill_descriptions_task.apply_async(
                        kwargs={
                            "batch_size": min(needs_jd_count + 10, 100),  # focused batch
                            "parallel_workers": 1,  # ONE worker — never spike CPU
                            "platform_slug": platform_s or None,
                        },
                        countdown=15,  # 15 s after harvest — DB writes settle first
                        queue="harvest",  # dedicated queue, doesn't compete with app
                    )
                    logger.info(
                        "JD backfill queued for %d new %s jobs (bg task, 1 worker)",
                        needs_jd_count, platform_s or "all",
                    )

        except SoftTimeLimitExceeded:
            logger.warning("Inline pipeline aborted at soft time limit for label %s", label_pk)
        except Exception as exc:
            logger.warning("Inline pipeline failed for label %s: %s", label_pk, exc)

    # ── Post-batch: auto-sync to pool once the whole batch is done ───────────
    # Sync is the only step that still runs as a separate background task.
    # It touches the main Job model, involves dedup checks, and only needs to run
    # once after the whole batch — not per-company.  A short countdown (60 s)
    # after the batch closes is more than enough for all inline enrichment to settle.
    if _batch_just_finished:
        try:
            from .models import HarvestEngineConfig as _EngCfg3
            _sync_cfg = _EngCfg3.get()
            if _sync_cfg.auto_sync_to_pool:
                sync_harvested_to_pool_task.apply_async(
                    kwargs={"max_jobs": 5000},
                    countdown=60,  # 60 s after last task — inline enrich is already done
                )
                logger.info("Auto-sync queued 60 s after batch #%s completion", batch.pk)
        except Exception as exc:
            logger.warning("Auto-sync queue failed: %s", exc)

    _invalidate_rawjobs_dashboard_cache()

    return {
        "label_pk": label_pk,
        "run_id": run.pk,
        "jobs_found": len(raw_jobs),
        "jobs_new": jobs_new,
        "jobs_updated": jobs_updated,
        "jobs_duplicate": jobs_duplicate,
        "jobs_failed": jobs_failed,
    }


@shared_task(bind=True, name="harvest.fetch_raw_jobs_batch", max_retries=0)
def fetch_raw_jobs_batch_task(
    self,
    platform_slug: str = None,
    label_pks: list = None,
    batch_name: str = None,
    triggered_user_id: int = None,
    test_mode: bool = False,
    test_max_jobs: int = 10,
    companies_per_platform: int = 1,
    skip_platforms: list = None,
    min_hours_since_fetch: int = 6,
    fetch_all: bool = False,
):
    """
    Create a FetchBatch and dispatch fetch_raw_jobs_for_company_task for every matching label.

    test_mode=True — picks up to `companies_per_platform` companies per platform,
    passes max_jobs=test_max_jobs (no full pagination). Useful for smoke-testing.
    skip_platforms — list of platform slugs to exclude (e.g. ["greenhouse","lever"]).
    min_hours_since_fetch — skip labels that were successfully fetched within this many
    hours. Pass None to read from HarvestEngineConfig (default). Pass 0 to force re-fetch.
    """
    from django.contrib.auth import get_user_model
    from .models import CompanyPlatformLabel, CompanyFetchRun, FetchBatch, HarvestEngineConfig

    # ── Read live engine config — all tuning knobs come from DB ──────────────
    try:
        _ecfg = HarvestEngineConfig.get()
        # Caller-supplied min_hours_since_fetch overrides DB only when explicitly passed.
        # Default argument sentinel is 6; if it still equals 6, prefer the DB value.
        if min_hours_since_fetch == 6:
            min_hours_since_fetch = _ecfg.min_hours_since_fetch
        _api_stagger    = _ecfg.api_stagger_ms    / 1000.0   # convert ms → seconds
        _scraper_stagger = _ecfg.scraper_stagger_ms / 1000.0
    except Exception:
        _api_stagger    = 0.1
        _scraper_stagger = 1.5

    User = get_user_model()
    triggered_user = None
    if triggered_user_id:
        triggered_user = User.objects.filter(pk=triggered_user_id).first()

    # Build batch name
    if not batch_name:
        ts = timezone.now().strftime("%Y-%m-%d %H:%M")
        skipped = ", ".join(skip_platforms or [])
        if test_mode:
            skip_str = f" | skip: {skipped}" if skipped else ""
            batch_name = f"PLATFORM CHECK — {companies_per_platform} co/platform, {test_max_jobs} jobs{skip_str} — {ts}"
        elif platform_slug:
            batch_name = f"{platform_slug.title()} batch — {ts}"
        else:
            batch_name = f"Full batch — {ts}"

    batch = FetchBatch.objects.create(
        created_by=triggered_user,
        name=batch_name,
        status=FetchBatch.Status.RUNNING,
        platform_filter=platform_slug or "",
        task_id=self.request.id or "",
        started_at=timezone.now(),
    )

    # ── Build label queryset ──────────────────────────────────────────────────
    # Include portal_alive=True (confirmed up) AND portal_alive=None (never checked).
    # Exclude portal_alive=False (confirmed down — no point hammering dead portals).
    # Only include companies whose platform is enabled — respect is_enabled flag.
    qs = CompanyPlatformLabel.objects.filter(
        portal_alive__in=[True, None],
        platform__isnull=False,
        platform__is_enabled=True,
    ).exclude(tenant_id="").select_related("platform", "company").order_by("company__name")

    if platform_slug:
        qs = qs.filter(platform__slug=platform_slug)

    if label_pks:
        qs = qs.filter(pk__in=label_pks)

    if skip_platforms:
        qs = qs.exclude(platform__slug__in=skip_platforms)

    # ── Build skip-if-fresh set ───────────────────────────────────────────────
    # Labels with a successful/partial run completed within min_hours_since_fetch
    # are skipped — no point re-fetching the same jobs minutes/hours later.
    fresh_label_pks: set[int] = set()
    if min_hours_since_fetch > 0 and not test_mode:
        fresh_cutoff = timezone.now() - timedelta(hours=min_hours_since_fetch)
        fresh_label_pks = set(
            CompanyFetchRun.objects.filter(
                status__in=[CompanyFetchRun.Status.SUCCESS, CompanyFetchRun.Status.PARTIAL],
                completed_at__gte=fresh_cutoff,
            ).values_list("label_id", flat=True)
        )
        if fresh_label_pks:
            logger.info(
                "fetch_raw_jobs_batch: skipping %d labels fetched within last %dh",
                len(fresh_label_pks), min_hours_since_fetch,
            )

    if test_mode:
        # Pick up to `companies_per_platform` companies per platform slug
        per_plat = max(1, companies_per_platform)
        seen_platforms: dict[str, int] = {}  # slug -> count
        label_list = []
        for label in qs.iterator():
            slug = label.platform.slug if label.platform else ""
            if not slug:
                continue
            count = seen_platforms.get(slug, 0)
            if count < per_plat:
                seen_platforms[slug] = count + 1
                label_list.append(label.pk)
        logger.info(
            "fetch_raw_jobs_batch TEST MODE: %d platforms, %d companies selected (%d per platform)",
            len(seen_platforms), len(label_list), per_plat,
        )
    else:
        all_pks = list(qs.values_list("pk", flat=True))
        label_list = [pk for pk in all_pks if pk not in fresh_label_pks]
        skipped_fresh = len(all_pks) - len(label_list)
        if skipped_fresh:
            logger.info(
                "fetch_raw_jobs_batch: %d/%d labels skipped (fresh <%dh), %d queued",
                skipped_fresh, len(all_pks), min_hours_since_fetch, len(label_list),
            )

    total = len(label_list)

    batch.total_companies = total
    batch.save(update_fields=["total_companies"])

    update_task_progress(self, current=0, total=total, message=f"Dispatching {total} company fetches…")

    # Stagger by platform type: API platforms get a tighter stagger (0.1s),
    # HTML scrapers get a wider one (1.0s) to avoid hammering slow targets.
    # adp uses HTMLScrapeHarvester → needs the slower 1.5s scraper stagger
    SCRAPER_SLUGS = {"jobvite", "icims", "taleo", "ultipro", "applicantpro",
                     "applytojob", "theapplicantmanager", "zoho", "breezy", "teamtailor", "adp"}

    # Fetch label→platform slug mapping once to decide stagger
    label_platform_map: dict[int, str] = {}
    if label_list:
        for row in CompanyPlatformLabel.objects.filter(pk__in=label_list).values("pk", "platform__slug"):
            label_platform_map[row["pk"]] = row["platform__slug"] or ""

    api_offset = 0
    scraper_offset = 0
    for label_pk in label_list:
        slug = label_platform_map.get(label_pk, "")
        is_scraper = slug in SCRAPER_SLUGS
        if is_scraper:
            countdown = scraper_offset
            scraper_offset += _scraper_stagger   # from HarvestEngineConfig (default 1.5s)
        else:
            countdown = api_offset
            api_offset += _api_stagger           # from HarvestEngineConfig (default 0.1s)

        kwargs = {"max_jobs": test_max_jobs} if test_mode else {}
        if fetch_all and not test_mode:
            kwargs["fetch_all"] = True   # pass full-crawl flag to child tasks
        fetch_raw_jobs_for_company_task.apply_async(
            args=[label_pk, batch.pk, "BATCH"],
            kwargs=kwargs,
            countdown=countdown,
        )

    if label_list and label_list[0] % 50 == 0:
        pass  # progress update already at end
    update_task_progress(self, current=total, total=total,
                         message=f"All {total} fetches queued for batch #{batch.pk}")

    logger.info("fetch_raw_jobs_batch: queued %d companies (batch #%d, test=%s)", total, batch.pk, test_mode)
    return {"batch_id": batch.pk, "total_companies": total, "test_mode": test_mode}


@shared_task(bind=True, name="harvest.retry_failed_raw_jobs")
def retry_failed_raw_jobs_task(self):
    """Re-queue fetch_raw_jobs_for_company_task for all FAILED runs in the last 7 days."""
    from .models import CompanyFetchRun

    cutoff = timezone.now() - timedelta(days=7)
    failed_runs = CompanyFetchRun.objects.filter(
        status=CompanyFetchRun.Status.FAILED,
        started_at__gte=cutoff,
    ).select_related("label")

    queued = 0
    for run in failed_runs:
        fetch_raw_jobs_for_company_task.delay(
            run.label_id,
            run.batch_id,
            "SCHEDULED",
        )
        queued += 1

    logger.info("retry_failed_raw_jobs: re-queued %d tasks", queued)
    return {"queued": queued}


@shared_task(bind=True, name="harvest.validate_raw_job_urls")
def validate_raw_job_urls_task(
    self,
    platform_slug: str | None = None,
    batch_size: int = 200,
    concurrency: int = 20,
    max_jobs: int | None = None,
):
    """
    HEAD-check raw job URLs and mark is_active=False for ones that return 4xx/5xx.

    Runs after every FETCH ALL batch (or on a schedule) to surface broken links
    before a human ever sees them. Results are visible in the Jobs Browser
    (SYNC column stays PENDING; is_active=False jobs are hidden from candidates).

    Uses a thread pool for concurrency — HEAD requests are I/O bound so
    parallelism is safe and fast.

    platform_slug — limit to one platform (e.g. "workday")
    batch_size    — DB fetch chunk size (memory control)
    concurrency   — parallel HTTP threads
    max_jobs      — cap total checked (for quick spot-checks)
    """
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from .models import RawJob

    qs = RawJob.objects.filter(is_active=True).exclude(original_url="")
    if platform_slug:
        qs = qs.filter(platform_slug=platform_slug)
    if max_jobs:
        qs = qs[:max_jobs]

    total = qs.count()
    update_task_progress(self, current=0, total=total, message=f"Checking {total:,} URLs…")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; GoCareers-UrlValidator/1.0; +https://chennu.co)"
        )
    }

    checked = alive = dead = errors = 0

    def check_url(job_id: int, url: str) -> tuple[int, bool]:
        """Returns (job_id, is_alive)."""
        try:
            r = requests.head(url, timeout=10, allow_redirects=True, headers=headers)
            if r.status_code == 405:
                # HEAD blocked — try GET streaming (just headers)
                r = requests.get(url, timeout=12, stream=True, allow_redirects=True, headers=headers)
                r.close()
            return job_id, r.status_code < 400
        except Exception:
            return job_id, False  # treat network errors as dead

    offset = 0
    while True:
        chunk = list(qs.values("id", "original_url")[offset: offset + batch_size])
        if not chunk:
            break
        if max_jobs and offset >= max_jobs:
            break

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {
                pool.submit(check_url, row["id"], row["original_url"]): row["id"]
                for row in chunk
            }
            dead_ids = []
            for future in as_completed(futures):
                job_id, is_alive = future.result()
                checked += 1
                if is_alive:
                    alive += 1
                else:
                    dead += 1
                    dead_ids.append(job_id)

            # Mark dead jobs inactive in one batch update
            if dead_ids:
                RawJob.objects.filter(pk__in=dead_ids).update(is_active=False)

        offset += batch_size
        update_task_progress(
            self, current=checked, total=total,
            message=f"Checked {checked:,}/{total:,} — {alive:,} alive, {dead:,} dead",
        )

    logger.info(
        "validate_raw_job_urls: checked=%d alive=%d dead=%d errors=%d",
        checked, alive, dead, errors,
    )
    return {"checked": checked, "alive": alive, "dead": dead}


@shared_task(name="harvest.cleanup_harvested_jobs")
def cleanup_harvested_jobs_task():
    """Phase 5: clean expired RawJob rows (HarvestedJob/HarvestRun removed)."""
    from .models import RawJob

    now = timezone.now()
    expired, _ = RawJob.objects.filter(
        expires_at__lt=now,
        sync_status__in=["PENDING", "SKIPPED"],
        is_active=True,
    ).update(is_active=False), 0  # soft-delete, not hard delete

    old_cutoff = now - timedelta(days=30)
    purged, _ = RawJob.objects.filter(
        is_active=False,
        fetched_at__lt=old_cutoff,
    ).delete()

    logger.info("Cleanup: %d RawJobs deactivated, %d purged.", expired[0] if isinstance(expired, tuple) else 0, purged)
    return {"deactivated": expired[0] if isinstance(expired, tuple) else expired, "purged": purged}



@shared_task(bind=True, name="harvest.sync_harvested_to_pool")
def sync_harvested_to_pool_task(self, max_jobs: int = 100):
    """Promote pending RawJobs to the Job pool (Phase 5 — reads RawJob, not HarvestedJob)."""
    from .models import RawJob
    from jobs.models import Job
    from jobs.dedup import find_existing_job_by_url
    from jobs.quality import compute_quality_score
    from django.contrib.auth import get_user_model
    from django.utils import timezone as _tz

    User = get_user_model()
    system_user = User.objects.filter(is_superuser=True).first()
    if not system_user:
        logger.error("No superuser found for sync task.")
        return {"synced": 0}

    # Quality gate: only sync RawJobs that have a real description (>50 chars).
    # This filters out stale/empty rows from the 100K backlog and keeps the pool clean.
    pending = list(
        RawJob.objects.filter(sync_status="PENDING", is_active=True, company__isnull=False)
        .exclude(original_url="")
        .exclude(description="")
        .filter(description__length__gt=50)
        .order_by("-fetched_at")
        .select_related("company", "job_platform")[:max_jobs]
    )

    synced = skipped = failed = 0
    total_n = len(pending)
    if total_n:
        update_task_progress(self, current=0, total=total_n, message="Sync RawJobs to pool…")

    for idx, rj in enumerate(pending, start=1):
        existing = (
            (Job.objects.filter(url_hash=rj.url_hash, is_archived=False).first() if rj.url_hash else None)
            or find_existing_job_by_url(rj.original_url)
            or Job.objects.filter(original_link=rj.original_url).first()
        )
        if existing:
            RawJob.objects.filter(pk=rj.pk).update(sync_status="SKIPPED")
            skipped += 1
            continue

        try:
            platform_slug = rj.platform_slug or (rj.job_platform.slug if rj.job_platform else "")
            with transaction.atomic():
                job = Job.objects.create(
                    title=rj.title,
                    company=rj.company_name or (rj.company.name if rj.company else ""),
                    company_obj=rj.company,
                    location=rj.location_raw or "",
                    description=rj.description or rj.title,
                    original_link=rj.original_url,
                    salary_range=rj.salary_raw or "",
                    job_type=rj.employment_type if rj.employment_type != "UNKNOWN" else "FULL_TIME",
                    status="POOL",
                    stage=Job.Stage.VETTED,
                    stage_changed_at=_tz.now(),
                    url_hash=rj.url_hash or "",
                    job_source=f"HARVESTED_{platform_slug.upper()}" if platform_slug else "HARVESTED",
                    posted_by=system_user,
                )
                job.quality_score = compute_quality_score(job)
                Job.objects.filter(pk=job.pk).update(quality_score=job.quality_score)
                RawJob.objects.filter(pk=rj.pk).update(sync_status="SYNCED")
                synced += 1
        except Exception as e:
            RawJob.objects.filter(pk=rj.pk).update(sync_status="FAILED")
            logger.error("Sync failed for RawJob %s: %s", rj.pk, e)
            failed += 1

        if total_n:
            update_task_progress(self, current=idx, total=total_n, message=f"Sync {idx}/{total_n}")

    logger.info("Sync: %d synced, %d skipped, %d failed.", synced, skipped, failed)
    return {"synced": synced, "skipped": skipped, "failed": failed}


# ─── Job Jarvis — single-URL ingestion ───────────────────────────────────────

@shared_task(bind=True, name="harvest.jarvis_ingest")
def jarvis_ingest_task(self, url: str, user_id: int | None = None):
    """
    Fetch *url* with JobJarvis, extract all job fields, find-or-create the
    Company, and persist a RawJob (platform_slug="jarvis" or detected slug).

    Returns a dict with the extracted data plus ``raw_job_id`` when saved
    successfully, or ``error`` on failure.
    """
    from .jarvis import JobJarvis
    from .models import RawJob, JobBoardPlatform
    from .normalizer import compute_url_hash

    update_task_progress(self, current=0, total=3, message="Fetching job page…")

    jarvis = JobJarvis()
    try:
        data = jarvis.ingest(url)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "url": url}

    if data.get("error"):
        return {"ok": False, "error": data["error"], "url": url, "data": data}

    update_task_progress(self, current=2, total=3, message="Saving to database…")

    # ── Resolve Company (smart matching) ─────────────────────────────────────
    company_name = (data.get("company_name") or "").strip()
    if not company_name:
        company_name = _extract_company_from_url(url)

    company = _jarvis_resolve_company(company_name, url)

    # ── Resolve platform ──────────────────────────────────────────────────────
    # Always tag Jarvis imports as platform_slug="jarvis" so they form their
    # own namespace and the recent-imports list is easy to filter.
    # The real detected ATS (greenhouse, lever, etc.) is stored in raw_payload.
    platform_slug = "jarvis"
    detected_ats = data.get("platform_slug") or ""
    job_platform = None
    if detected_ats:
        job_platform = JobBoardPlatform.objects.filter(slug=detected_ats).first()
        if not job_platform and detected_ats == "dayforce":
            job_platform, _ = JobBoardPlatform.objects.get_or_create(
                slug="dayforce",
                defaults={
                    "name": "Dayforce",
                    "url_patterns": ["jobs.dayforcehcm.com", "dayforcehcm.com"],
                    "api_type": JobBoardPlatform.ApiType.UNKNOWN,
                    "notes": "Auto-created by Jarvis from Dayforce import detection.",
                },
            )

    # ── Build RawJob ──────────────────────────────────────────────────────────
    import hashlib
    from datetime import timedelta
    original_url = data.get("original_url") or url
    url_hash = compute_url_hash(original_url)

    # ── Resolve company label context for fetch-all workflows ───────────────
    platform_label, board_ctx = _jarvis_ensure_company_platform_label(
        company=company,
        detected_ats=detected_ats,
        source_url=original_url,
        job_platform=job_platform,
    )

    # Parse posted_date
    posted_date = _jarvis_parse_date(data.get("posted_date_raw", ""))
    closing_date = _jarvis_parse_date(data.get("closing_date_raw", ""))

    from .enrichments import clean_job_content, clean_job_text

    # Normalize HTML-heavy scraped content into cleaner plain text.
    desc_meta = clean_job_content(data.get("description") or "", max_len=50000)
    description = desc_meta["clean_text"]
    requirements = clean_job_text(data.get("requirements") or "", max_len=20000)
    benefits = clean_job_text(data.get("benefits") or "", max_len=10000)

    # Enrich raw_payload with Jarvis metadata
    raw_payload = data.get("raw_payload") or {}
    raw_payload["jarvis_detected_ats"] = detected_ats
    raw_payload["jarvis_strategy"] = data.get("strategy", "")
    raw_payload["jarvis_source_url"] = url
    raw_payload["jarvis_tenant_id"] = board_ctx.get("tenant_id") or ""
    raw_payload["jarvis_company_jobs_url"] = board_ctx.get("company_jobs_url") or ""
    raw_payload["jarvis_platform_label_id"] = platform_label.pk if platform_label else None
    raw_payload["jarvis_fetch_all_supported"] = bool(board_ctx.get("fetch_all_supported"))

    # ── Run enrichment extraction ─────────────────────────────────────────
    from .enrichments import extract_enrichments
    enriched = extract_enrichments({
        "title": data.get("title") or "",
        "description": description,
        "requirements": requirements,
        "benefits": benefits,
        "department": data.get("department") or "",
        "location_raw": data.get("location_raw") or "",
        "employment_type": data.get("employment_type") or "",
        "experience_level": data.get("experience_level") or "",
        "salary_raw": data.get("salary_raw") or "",
        "company_name": company_name or "",
        "posted_date": posted_date,
    })

    external_id = (data.get("external_id") or "").strip()[:512]
    raw_job_defaults = {
        "company": company,
        "platform_label": platform_label,
        "job_platform": job_platform,
        "platform_slug": platform_slug,
        "external_id": external_id,
        "original_url": original_url[:1024],
        "apply_url": (data.get("apply_url") or original_url)[:1024],
        "title": (data.get("title") or "Untitled")[:512],
        "company_name": company_name[:256] if company_name else "",
        "department": (data.get("department") or "")[:256],
        "team": (data.get("team") or "")[:256],
        "location_raw": (data.get("location_raw") or "")[:512],
        "city": (data.get("city") or "")[:128],
        "state": (data.get("state") or "")[:128],
        "country": (data.get("country") or "")[:128],
        "is_remote": bool(data.get("is_remote")),
        "location_type": data.get("location_type") or "UNKNOWN",
        "employment_type": data.get("employment_type") or "UNKNOWN",
        "experience_level": data.get("experience_level") or "UNKNOWN",
        "salary_min": data.get("salary_min"),
        "salary_max": data.get("salary_max"),
        "salary_currency": (data.get("salary_currency") or "USD")[:8],
        "salary_period": (data.get("salary_period") or "")[:16],
        "salary_raw": (data.get("salary_raw") or "")[:256],
        "description": description,
        "description_clean": (enriched.get("description_clean") or description)[:50000],
        "description_raw_html": (desc_meta.get("raw_html") or "")[:120000],
        "has_html_content": bool(desc_meta.get("has_html_content")),
        "cleaning_version": (desc_meta.get("cleaning_version") or "v2")[:20],
        "requirements": requirements,
        "benefits": benefits,
        "posted_date": posted_date,
        "closing_date": closing_date,
        "raw_payload": raw_payload,
        "sync_status": "PENDING",
        "is_active": True,
        "expires_at": timezone.now() + timedelta(days=30),
        **_company_snapshot_fields(company),
        # ── enrichment fields ─────────────────────────────────────────
        **enriched,
    }

    raw_job = None
    created = False

    # Secondary dedupe guard by external_id within the resolved company+platform.
    if external_id:
        ext_match_qs = RawJob.objects.filter(
            company=company,
            external_id=external_id,
        )
        if job_platform:
            ext_match_qs = ext_match_qs.filter(
                Q(job_platform=job_platform)
                | Q(platform_slug=job_platform.slug)
                | Q(job_platform__isnull=True)
            )
        ext_match = ext_match_qs.order_by("pk").first()
        if ext_match:
            hash_owned_elsewhere = (
                RawJob.objects.filter(url_hash=url_hash)
                .exclude(pk=ext_match.pk)
                .values_list("pk", flat=True)
                .first()
            )
            if not hash_owned_elsewhere:
                for field, val in raw_job_defaults.items():
                    setattr(ext_match, field, val)
                ext_match.url_hash = url_hash
                ext_match.save()
                raw_job = ext_match

    # Query-variant reconciliation: same job path with old tracking query hash.
    if raw_job is None:
        base_url = original_url.split("?", 1)[0].strip()
        if base_url:
            variant_qs = RawJob.objects.filter(
                company=company,
                original_url__startswith=base_url,
            )
            if job_platform:
                variant_qs = variant_qs.filter(
                    Q(job_platform=job_platform)
                    | Q(platform_slug=job_platform.slug)
                    | Q(job_platform__isnull=True)
                )
            variant_row = variant_qs.order_by("pk").first()
            if variant_row and variant_row.url_hash != url_hash:
                hash_owned_elsewhere = (
                    RawJob.objects.filter(url_hash=url_hash)
                    .exclude(pk=variant_row.pk)
                    .values_list("pk", flat=True)
                    .first()
                )
                if not hash_owned_elsewhere:
                    for field, val in raw_job_defaults.items():
                        setattr(variant_row, field, val)
                    variant_row.url_hash = url_hash
                    variant_row.save()
                    raw_job = variant_row

    # Legacy hash reconciliation so old rows are updated instead of duplicated.
    if raw_job is None:
        legacy_hash = hashlib.sha256(original_url.strip().encode("utf-8")).hexdigest()
        if legacy_hash and legacy_hash != url_hash:
            legacy_row = RawJob.objects.filter(url_hash=legacy_hash).order_by("pk").first()
            if legacy_row:
                hash_owned_elsewhere = (
                    RawJob.objects.filter(url_hash=url_hash)
                    .exclude(pk=legacy_row.pk)
                    .values_list("pk", flat=True)
                    .first()
                )
                if not hash_owned_elsewhere:
                    for field, val in raw_job_defaults.items():
                        setattr(legacy_row, field, val)
                    legacy_row.url_hash = url_hash
                    legacy_row.save()
                    raw_job = legacy_row

    if raw_job is None:
        raw_job, created = RawJob.objects.update_or_create(
            url_hash=url_hash,
            defaults=raw_job_defaults,
        )
    _invalidate_rawjobs_dashboard_cache()

    update_task_progress(self, current=3, total=3, message="Done ✓")

    logger.info(
        "Jarvis ingested: %s | %s | raw_job_id=%d (%s)",
        data.get("title"), company_name, raw_job.pk,
        "created" if created else "updated",
    )

    return {
        "ok": True,
        "raw_job_id": raw_job.pk,
        "created": created,
        "title": data.get("title", ""),
        "company_name": company.name,             # actual matched company name
        "company_id": company.pk,
        "platform_slug": platform_slug,
        "strategy": data.get("strategy", ""),
        "detected_ats": detected_ats,
        "tenant_id": board_ctx.get("tenant_id") or "",
        "company_jobs_url": board_ctx.get("company_jobs_url") or "",
        "platform_label_id": platform_label.pk if platform_label else None,
        "fetch_all_supported": bool(board_ctx.get("fetch_all_supported")),
        "data": {k: v for k, v in data.items() if k != "raw_payload"},
    }


def _jarvis_company_jobs_url(platform_slug: str, tenant_id: str) -> str:
    """Build a user-facing company jobs URL from platform + tenant."""
    if not platform_slug or not tenant_id:
        return ""
    try:
        # Dayforce board root is cleaner than /jobs for user navigation.
        if platform_slug == "dayforce":
            if "|" in tenant_id:
                tenant, board = tenant_id.split("|", 1)
                tenant = (tenant or "").strip()
                board = (board or "").strip() or "CANDIDATEPORTAL"
                if tenant:
                    return f"https://jobs.dayforcehcm.com/en-US/{tenant}/{board}"
            t = tenant_id.strip()
            if t:
                return f"https://jobs.dayforcehcm.com/en-US/{t}/CANDIDATEPORTAL"

        from .career_url import build_career_url
        return build_career_url(platform_slug, tenant_id)
    except Exception:
        return ""


def _jarvis_ensure_company_platform_label(*, company, detected_ats: str, source_url: str, job_platform=None):
    """
    Ensure CompanyPlatformLabel exists for Jarvis-ingested company.

    Returns ``(label_or_none, board_context_dict)`` where board_context includes:
      - tenant_id
      - company_jobs_url
      - fetch_all_supported
      - platform_slug
    """
    from .detectors import extract_tenant
    from .harvesters import get_harvester
    from .models import CompanyPlatformLabel, JobBoardPlatform

    platform_slug = (detected_ats or "").strip().lower()
    tenant_id = extract_tenant(platform_slug, source_url) if platform_slug and source_url else ""
    company_jobs_url = _jarvis_company_jobs_url(platform_slug, tenant_id)
    board_ctx = {
        "platform_slug": platform_slug,
        "tenant_id": tenant_id,
        "company_jobs_url": company_jobs_url,
        "fetch_all_supported": False,
    }
    if not company:
        return None, board_ctx

    now_ts = timezone.now()

    platform = job_platform if getattr(job_platform, "slug", "") == platform_slug else None
    if not platform and platform_slug:
        platform = JobBoardPlatform.objects.filter(slug=platform_slug).first()
    if not platform and platform_slug == "dayforce":
        platform, _ = JobBoardPlatform.objects.get_or_create(
            slug="dayforce",
            defaults={
                "name": "Dayforce",
                "url_patterns": ["jobs.dayforcehcm.com", "dayforcehcm.com"],
                "api_type": JobBoardPlatform.ApiType.UNKNOWN,
                "notes": "Auto-created by Jarvis from Dayforce import detection.",
            },
        )

    # Prefer an existing label that matches the detected platform/tenant instead
    # of taking an arbitrary first label for the company.
    label = None
    company_labels = CompanyPlatformLabel.objects.filter(company=company)
    if platform:
        label = (
            company_labels
            .filter(platform=platform)
            .order_by("-is_verified", "pk")
            .first()
        )
    if not label and tenant_id:
        label = (
            company_labels
            .filter(tenant_id=tenant_id)
            .order_by("-is_verified", "pk")
            .first()
        )
    if not label:
        label = company_labels.order_by("-is_verified", "pk").first()
    if not label and not platform_slug:
        return None, board_ctx
    if not label:
        label = CompanyPlatformLabel.objects.create(
            company=company,
            platform=platform,
            tenant_id=tenant_id,
            confidence=CompanyPlatformLabel.Confidence.HIGH,
            detection_method=CompanyPlatformLabel.DetectionMethod.URL_PATTERN,
            detected_at=now_ts if platform else None,
            last_checked_at=now_ts,
        )

    changed: list[str] = []
    is_manual_locked = bool(
        label.is_verified
        or label.detection_method == CompanyPlatformLabel.DetectionMethod.MANUAL
    )

    # Jarvis URL should correct stale auto-detected platform labels unless manually locked.
    if platform:
        if not label.platform_id:
            label.platform = platform
            changed.append("platform")
        elif label.platform_id != platform.pk and not is_manual_locked:
            label.platform = platform
            changed.append("platform")

    # Prefer extracted tenant when we have one. If platform changed, refresh tenant too.
    if tenant_id:
        platform_changed = "platform" in changed
        if not (label.tenant_id or "").strip() or platform_changed or (label.tenant_id != tenant_id and not is_manual_locked):
            label.tenant_id = tenant_id
            changed.append("tenant_id")

    # Keep detection metadata healthy for auto-detected labels.
    if platform and label.detection_method == CompanyPlatformLabel.DetectionMethod.UNDETECTED:
        label.detection_method = CompanyPlatformLabel.DetectionMethod.URL_PATTERN
        changed.append("detection_method")
    if platform and not label.detected_at:
        label.detected_at = now_ts
        changed.append("detected_at")
    label.last_checked_at = now_ts
    changed.append("last_checked_at")

    if changed:
        # Preserve field order while removing duplicates.
        deduped_fields = list(dict.fromkeys(changed))
        label.save(update_fields=deduped_fields)

    resolved_platform_slug = (
        (label.platform.slug if label.platform else "")
        or platform_slug
        or ""
    )
    resolved_tenant = (label.tenant_id or "").strip() or tenant_id
    resolved_company_jobs_url = (
        _jarvis_company_jobs_url(resolved_platform_slug, resolved_tenant)
        or label.career_page_url
        or company_jobs_url
        or ""
    )

    if resolved_company_jobs_url and not (company.career_site_url or "").strip():
        company.career_site_url = resolved_company_jobs_url
        company.save(update_fields=["career_site_url", "updated_at"])

    board_ctx["platform_slug"] = resolved_platform_slug
    board_ctx["tenant_id"] = resolved_tenant
    board_ctx["company_jobs_url"] = resolved_company_jobs_url
    board_ctx["fetch_all_supported"] = bool(
        resolved_platform_slug
        and resolved_tenant
        and get_harvester(resolved_platform_slug) is not None
    )
    return label, board_ctx


def _extract_company_from_url(url: str) -> str:
    """Best-effort: pull a human-readable company name from the URL hostname."""
    import re as _re
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        # Dayforce URLs include tenant in path: /en-US/{tenant}/{board}/jobs/{id}
        # Use tenant as the company fallback instead of the ATS hostname.
        if "dayforcehcm.com" in host:
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) >= 2 and _re.match(r"^[a-z]{2}-[a-z]{2}$", parts[0], _re.I):
                tenant = parts[1]
                if tenant:
                    return tenant.replace("-", " ").replace("_", " ").title().strip()

        # Strip www. / jobs. / careers. prefixes
        for prefix in ("www.", "jobs.", "careers.", "boards."):
            if host.startswith(prefix):
                host = host[len(prefix):]
        # Remove known ATS domains: greenhouse.io, lever.co, etc.
        for suffix in (
            ".greenhouse.io", ".lever.co", ".ashbyhq.com",
            ".myworkdayjobs.com", ".workable.com", ".bamboohr.com",
            ".dayforcehcm.com", ".icims.com", ".smartrecruiters.com",
            ".taleo.net", ".jobvite.com", ".zohorecruit.com",
        ):
            suffix_root = suffix.lstrip(".")
            if host == suffix_root:
                host = ""
                break
            if host.endswith(suffix):
                host = host[: -len(suffix)]
        if host in {"dayforcehcm.com", "greenhouse.io", "lever.co", "ashbyhq.com"}:
            return "Unknown"
        # Convert hyphens/dots to spaces, title-case
        company = host.replace("-", " ").replace(".", " ").title()
        return company.strip() or "Unknown"
    except Exception:
        return "Unknown"


def _root_url(url: str) -> str:
    from urllib.parse import urlparse
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        ats_hosts = (
            "dayforcehcm.com",
            "greenhouse.io",
            "lever.co",
            "ashbyhq.com",
            "myworkdayjobs.com",
            "workable.com",
            "bamboohr.com",
            "smartrecruiters.com",
            "icims.com",
            "taleo.net",
            "jobvite.com",
            "zohorecruit.com",
        )
        if any(host == d or host.endswith("." + d) for d in ats_hosts):
            return ""
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return ""


def _jarvis_parse_date(raw: str):
    """Parse an ISO-8601 or YYYY-MM-DD string into a date object (or None)."""
    if not raw:
        return None
    import re as _re
    from datetime import date
    # Extract YYYY-MM-DD from strings like "2026-04-15T00:00:00Z"
    m = _re.search(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def _jarvis_company_name_key(raw: str) -> str:
    """Normalize name for duplicate checks (ignore punctuation/legal suffix noise)."""
    import re as _re

    if not raw:
        return ""
    text = raw.strip().lower().replace("&", " and ")
    text = _re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [t for t in text.split() if t]
    if not tokens:
        return ""
    stop = {
        "the", "inc", "incorporated", "llc", "ltd", "ltda", "corp", "corporation",
        "co", "company", "group", "holdings", "plc", "gmbh",
        "sa", "bv", "srl", "pte", "and", "of", "for", "a", "an", "do", "de", "da",
    }
    reduced = [t for t in tokens if t not in stop] or tokens
    return " ".join(reduced)


def _jarvis_clean_company_name(raw: str) -> str:
    import re as _re

    text = (raw or "").strip()
    if not text:
        return ""
    return _re.sub(r"\s+", " ", text).strip(" -_,.")


def _jarvis_resolve_company(company_name: str, job_url: str):
    """
    Smart company lookup for Jarvis imports.

    Priority:
      1. Domain match   — extract root domain from URL, look for company with
                          matching .domain or .website (most reliable)
      2. Exact name     — Company.name == company_name
      3. Fuzzy contains — one name is a substring of the other
                          e.g. "Bayview" ↔ "Bayview Asset Management"
      4. Create new     — only when all matching strategies fail
    """
    from django.db.models import Q
    from companies.models import Company
    from urllib.parse import urlparse

    company_name = _jarvis_clean_company_name(company_name)

    # ── 1. Domain match ──────────────────────────────────────────────────────
    root_domain = ""
    try:
        host = urlparse(job_url).netloc.lower()
        # Strip well-known ATS/career subdomains
        for sub in ("careers.", "jobs.", "boards.", "apply.", "recruiting.",
                    "career.", "job.", "hire.", "talent.", "work."):
            if host.startswith(sub):
                host = host[len(sub):]
                break
        # Remove known ATS root domains entirely (they are not the company domain)
        ATS_DOMAINS = (
            ".greenhouse.io", ".lever.co", ".ashbyhq.com",
            ".myworkdayjobs.com", ".workable.com", ".bamboohr.com",
            ".icims.com", ".taleo.net", ".jobvite.com", ".smartrecruiters.com",
            ".dayforcehcm.com",
        )
        for ats in ATS_DOMAINS:
            ats_root = ats.lstrip(".")
            if host == ats_root or host.endswith(ats):
                host = ""
                break
        if host:
            parts = host.split(".")
            root_domain = ".".join(parts[-2:]) if len(parts) >= 2 else host
    except Exception:
        pass

    if root_domain:
        match = Company.objects.filter(
            Q(domain__iexact=root_domain) |
            Q(domain__iendswith="." + root_domain) |
            Q(website__icontains=root_domain)
        ).first()
        if match:
            logger.info("Jarvis company match by domain: %s → %s", root_domain, match.name)
            return match

    # ── 2. Exact match (case-insensitive) + normalized key match ───────────
    if company_name:
        exact = (
            Company.objects.filter(Q(name__iexact=company_name) | Q(alias__iexact=company_name))
            .order_by(Length("name"), "name")
            .first()
        )
        if exact:
            logger.info("Jarvis exact company match: '%s' → '%s'", company_name, exact.name)
            return exact

        key = _jarvis_company_name_key(company_name)
        compact = key.replace(" ", "")
        if key:
            token_q = Q()
            for tok in key.split()[:3]:
                token_q |= Q(name__icontains=tok) | Q(alias__icontains=tok)
            candidate_qs = Company.objects.filter(token_q) if token_q else Company.objects.all()
            best = None
            for cand in candidate_qs.only("id", "name", "alias").order_by("name")[:300]:
                for cand_name in (cand.name, cand.alias):
                    cand_key = _jarvis_company_name_key(cand_name or "")
                    if not cand_key:
                        continue
                    cand_compact = cand_key.replace(" ", "")
                    if cand_key == key or (compact and cand_compact == compact):
                        if best is None or len(cand.name) < len(best.name):
                            best = cand
                        break
            if best:
                logger.info("Jarvis normalized company match: '%s' → '%s'", company_name, best.name)
                return best

    # ── 3. Word-by-word fuzzy scan ───────────────────────────────────────────
    # NOTE: intentionally skipping a plain exact-name match here.
    # If a previous Jarvis run created a stub company (e.g. "Bayview Asset
    # Management"), an exact match would return that stub instead of the
    # real "Bayview" company. The word scan is smarter: it checks each
    # significant word independently and prefers the shorter / canonical name.
    # Handles variants like:
    #   "BRA 3M do Brasil Ltda." → finds "3M"   (first_word "BRA" wouldn't work)
    #   "Bayview Asset Management" → finds "Bayview"
    #   "3M Company" → finds "3M"
    _STOP = {"the", "inc", "llc", "ltd", "ltda", "corp", "co", "company",
             "group", "holdings", "do", "de", "da", "di", "du", "van", "and",
             "of", "for", "a", "an", "&"}

    if company_name:
        name_lower = company_name.lower()
        words = [w.strip(".,") for w in company_name.split()
                 if len(w.strip(".,")) >= 2 and w.lower().strip(".,") not in _STOP]

        best = None
        seen_ids: set[int] = set()

        for word in words:
            # First try: exact company name == this single word (e.g., "3M")
            try:
                exact_word = Company.objects.get(name__iexact=word)
                if exact_word.pk not in seen_ids:
                    logger.info(
                        "Jarvis word-exact match: '%s' (word '%s') → '%s'",
                        company_name, word, exact_word.name,
                    )
                    return exact_word
            except Company.DoesNotExist:
                pass
            except Company.MultipleObjectsReturned:
                pass

            # Second try: containment scan for this word
            for cand in Company.objects.filter(name__icontains=word).order_by("name")[:10]:
                if cand.pk in seen_ids:
                    continue
                seen_ids.add(cand.pk)
                cn = cand.name.lower()
                # Only accept if one name is fully contained in the other
                if cn in name_lower or name_lower in cn:
                    if best is None or len(cand.name) < len(best.name):
                        best = cand

        if best:
            logger.info(
                "Jarvis fuzzy match: '%s' → '%s'",
                company_name, best.name,
            )
            return best

    # ── 4. Create new ────────────────────────────────────────────────────────
    clean_name = company_name or "Unknown (Jarvis Import)"
    company, created = Company.objects.get_or_create(
        name=clean_name,
        defaults={"website": _root_url(job_url)},
    )
    if created:
        logger.info("Jarvis created new company: %s", company.name)
    return company


# ─── Description Backfill ────────────────────────────────────────────────────

def _fast_workday_description(job_url: str) -> str:
    """Fetch a Workday JD directly via the CXS JSON API — no Jarvis/scraping needed.

    Returns the description HTML string or '' on any failure.
    Workday's CXS endpoint returns structured JSON at:
      https://{subdomain}.myworkdayjobs.com/wday/cxs/{tenant}/{jobboard}{ext_path}
    """
    import re as _re2
    import requests as _req

    m = _re2.match(
        r"https?://([\w-]+(?:\.wd\d+)?)\.myworkdayjobs\.com/([^/?#]+)(/(?:details|job)/[^?#]+)",
        job_url,
        _re2.I,
    )
    if not m:
        return ""
    full_subdomain = m.group(1)
    jobboard = m.group(2)
    ext_path = m.group(3).split("?")[0]
    tenant = _re2.sub(r"\.wd\d+$", "", full_subdomain, flags=_re2.I)
    cxs_url = f"https://{full_subdomain}.myworkdayjobs.com/wday/cxs/{tenant}/{jobboard}{ext_path}"
    try:
        resp = _req.get(cxs_url, headers={"Accept": "application/json"}, timeout=10)
        if not resp.ok:
            return ""
        data = resp.json()
        if not isinstance(data, dict):
            return ""
        info = data.get("jobPostingInfo") or data
        for key in ("jobDescription", "jobPostingDescription", "externalJobDescription", "shortDescription"):
            val = info.get(key) or data.get(key) or ""
            if isinstance(val, dict):
                val = val.get("content", "") or ""
            val = str(val).strip()
            if val:
                return val
    except Exception:
        pass
    return ""


def _backfill_process_one_job(job, jarvis):
    """
    Fetch JD for a single RawJob row that was already claim-locked.
    Clears jd_backfill_locked_at on every exit path.
    Returns one of: ``updated``, ``skipped``, ``failed`` and a log dict.
    """
    from celery.exceptions import SoftTimeLimitExceeded

    from .enrichments import extract_enrichments
    from .models import RawJob

    fetch_url = (job.original_url or "").strip()
    platform = (job.platform_slug or "").lower()

    if platform == "smartrecruiters":
        from .smartrecruiters_support import backfill_fetch_url_for_raw_job
        fetch_url = backfill_fetch_url_for_raw_job(job) or fetch_url

    log_base = {
        "pk": job.pk,
        "title": (job.title or "")[:60],
        "company": (job.company_name or "")[:40],
        "platform": job.platform_slug or "",
        "url": (fetch_url or "")[:120],
    }

    # Fast path: Workday CXS JSON API — no Jarvis/browser scraping needed.
    # Falls back to Jarvis only if the CXS call returns nothing.
    if platform == "workday":
        cxs_desc = _fast_workday_description(fetch_url)
        if cxs_desc:
            data = {"description": cxs_desc, "strategy": "workday_cxs"}
        else:
            try:
                data = jarvis.ingest(fetch_url)
            except SoftTimeLimitExceeded:
                raise
            except Exception as exc:
                logger.warning("Backfill (Workday fallback) failed for job %s: %s", job.pk, exc)
                RawJob.objects.filter(pk=job.pk).update(
                    description=" ", has_description=False, jd_backfill_locked_at=None
                )
                return "failed", {**log_base, "status": "failed", "reason": str(exc)[:80]}
    else:
        try:
            data = jarvis.ingest(fetch_url)
        except SoftTimeLimitExceeded:
            raise
        except Exception as exc:
            logger.warning("Backfill failed for job %s: %s", job.pk, exc)
            RawJob.objects.filter(pk=job.pk).update(
                description=" ", has_description=False, jd_backfill_locked_at=None
            )
            log = {**log_base, "status": "failed", "reason": str(exc)[:80]}
            return "failed", log

    desc_str = _backfill_str(data.get("description")).strip()

    if not desc_str:
        upd: dict = {"description": " ", "has_description": False, "jd_backfill_locked_at": None}
        pl = {}
        if data.get("raw_payload"):
            # Prefer newest API/Jarvis payload over stale DB rows (fixes SmartRecruiters active flag).
            pl = {**(job.raw_payload or {}), **dict(data["raw_payload"])}
            upd["raw_payload"] = pl
        if isinstance(pl, dict) and pl.get("active") is False:
            upd["is_active"] = False
        RawJob.objects.filter(pk=job.pk).update(**upd)
        log = {
            **log_base,
            "status": "skipped",
            "reason": (_backfill_str(data.get("error")).strip() or "No description")[:80],
            "strategy": _backfill_str(data.get("strategy"))[:30],
        }
        return "skipped", log

    update_fields: dict = {"description": desc_str[:50000], "has_description": True, "jd_backfill_locked_at": None}

    for f, mx in (("requirements", 20000), ("benefits", 10000)):
        v = _backfill_str(data.get(f)).strip()
        if v:
            update_fields[f] = v[:mx]

    for f in ("salary_min", "salary_max"):
        v = data.get(f)
        if v is not None:
            update_fields[f] = v

    for f in ("salary_currency", "salary_period", "salary_raw"):
        v = _backfill_str(data.get(f)).strip()
        if v:
            update_fields[f] = v[:256]

    for f in ("employment_type", "experience_level"):
        v = _backfill_str(data.get(f)).strip()
        if v and v != "UNKNOWN":
            update_fields[f] = v

    for f in ("department", "city", "state", "country", "location_raw"):
        v = _backfill_str(data.get(f)).strip()
        if v:
            update_fields[f] = v[:256]

    for f in ("is_remote", "location_type"):
        v = data.get(f)
        if v is not None and v not in ("", "UNKNOWN"):
            update_fields[f] = v

    if data.get("raw_payload"):
        # Prefer fresh Jarvis/API keys over older harvest list payloads (e.g. active, sections).
        merged_pl = {**(job.raw_payload or {}), **dict(data["raw_payload"])}
        update_fields["raw_payload"] = merged_pl
        if isinstance(merged_pl, dict) and merged_pl.get("active") is False:
            update_fields["is_active"] = False

    update_fields.update(extract_enrichments({
        "title": job.title,
        "description": update_fields.get("description") or job.description,
        "requirements": update_fields.get("requirements") or job.requirements,
        "benefits": update_fields.get("benefits") or job.benefits,
        "department": update_fields.get("department") or job.department,
        "location_raw": update_fields.get("location_raw") or job.location_raw,
        "employment_type": update_fields.get("employment_type") or job.employment_type,
        "experience_level": update_fields.get("experience_level") or job.experience_level,
        "salary_raw": update_fields.get("salary_raw") or job.salary_raw,
        "company_name": job.company_name,
        "posted_date": str(job.posted_date) if job.posted_date else "",
    }))

    RawJob.objects.filter(pk=job.pk).update(**update_fields)
    log = {
        **log_base,
        "status": "updated",
        "desc_len": len(desc_str),
        "strategy": _backfill_str(data.get("strategy"))[:30],
    }
    logger.info("Backfill updated job %s (%s)", job.pk, job.title[:60])
    return "updated", log


def _backfill_descriptions_chunk_impl(
    claim_size: int,
    platform_slug: str | None,
    *,
    progress_hook=None,
    shard_index: int = 0,
    shard_count: int = 1,
) -> dict:
    """
    Claim up to *claim_size* rows (SKIP LOCKED on Postgres) and fetch JDs sequentially.
    Used by the Celery chunk task (standalone), the orchestrator (inline or thread pool),
    and must be safe to call from worker threads (fresh DB connections per thread).

    If *progress_hook* is set, it is invoked with
    ``(event, job=..., entry=..., lu=..., ls=..., lf=...)`` where event is
    ``"job_start"`` before HTTP fetch and ``"job_done"`` after each job so the
    orchestrator can update Celery PROGRESS every row (live UI).
    """
    import time as _time

    from celery.exceptions import SoftTimeLimitExceeded

    from .jarvis import JobJarvis
    from .models import RawJob

    updated = skipped = failed = 0
    logs: list[dict] = []

    try:
        jobs = _claim_backfill_job_batch(
            claim_size,
            platform_slug,
            shard_index=shard_index,
            shard_count=shard_count,
        )
    except Exception as exc:
        logger.exception("backfill chunk claim failed: %s", exc)
        return {"claimed": 0, "updated": 0, "skipped": 0, "failed": 0, "log": [], "error": str(exc)}

    if not jobs:
        return {"claimed": 0, "updated": 0, "skipped": 0, "failed": 0, "log": []}

    jarvis = JobJarvis()
    for idx, job in enumerate(jobs):
        if progress_hook:
            progress_hook("job_start", job=job, entry=None, lu=updated, ls=skipped, lf=failed)
        try:
            outcome, entry = _backfill_process_one_job(job, jarvis)
        except SoftTimeLimitExceeded:
            logger.warning("backfill chunk soft time limit at job %s", job.pk)
            RawJob.objects.filter(pk__in=[j.pk for j in jobs[idx:]]).update(
                jd_backfill_locked_at=None,
            )
            return {
                "claimed": len(jobs),
                "updated": updated,
                "skipped": skipped,
                "failed": failed,
                "log": logs if not progress_hook else [],
                "soft_time_limit": True,
            }
        logs.append(entry)
        if outcome == "updated":
            updated += 1
        elif outcome == "skipped":
            skipped += 1
        else:
            failed += 1
        if progress_hook:
            progress_hook("job_done", job=job, entry=entry, lu=updated, ls=skipped, lf=failed)
        _time.sleep(_backfill_inter_job_delay_sec())

    return {
        "claimed": len(jobs),
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "log": logs if not progress_hook else [],
    }


@shared_task(
    name="harvest.backfill_descriptions_chunk",
    soft_time_limit=3600,
    time_limit=3900,
    max_retries=0,
)
def backfill_descriptions_chunk_task(
    claim_size: int,
    platform_slug: str | None,
    shard_index: int = 0,
    shard_count: int = 1,
):
    """Celery entry point for :func:`_backfill_descriptions_chunk_impl`."""
    return _backfill_descriptions_chunk_impl(
        claim_size,
        platform_slug,
        shard_index=shard_index,
        shard_count=shard_count,
    )


@shared_task(bind=True, name="harvest.backfill_descriptions", soft_time_limit=86400, time_limit=90000)
def backfill_descriptions_task(
    self,
    batch_size: int = 200,
    parallel_workers: int = 4,
    platform_slug: str | None = None,
    offset: int = 0,
    _chain_depth: int = 0,
    _skip_streak: int = 0,
):
    """
    Fetch JDs for RawJobs with no description using parallel chunk workers.

    *batch_size* — rows claimed per chunk (default 200).
    *parallel_workers* — concurrent chunk runners (capped at 8), implemented with
      a thread pool inside this task (not nested Celery tasks).

    Without ``SKIP LOCKED``, chunks use PK modulo sharding so rows are not doubled.

    Uses ``jd_backfill_locked_at`` + either ``SKIP LOCKED`` or PK sharding so workers
    do not process the same row twice.
    """
    import time as _time

    from celery.exceptions import SoftTimeLimitExceeded

    from .models import RawJob

    if offset:
        logger.warning(
            "backfill_descriptions_task: offset=%s is ignored.",
            offset,
        )

    claim_size = max(10, min(int(batch_size), 500))
    workers_requested = max(1, min(int(parallel_workers), BACKFILL_MAX_PARALLEL))
    parallelism = workers_requested
    parallelism_notes: list[str] = []

    use_pk_sharding = not _supports_select_for_update_skip_locked()
    if use_pk_sharding and parallelism > 1:
        parallelism_notes.append(
            "Parallel chunks use PK modulo sharding (MOD id) — safe on SQLite; "
            "PostgreSQL + SKIP LOCKED is still best for even load."
        )

    parallelism_note = " ".join(parallelism_notes)

    total = _backfill_eligible_queryset(platform_slug).count()
    if total == 0:
        return {"message": "All jobs already have descriptions.", "updated": 0}

    updated = skipped = failed = 0
    processed = 0
    recent_log: list[dict] = []
    _LOG_MAX = 25
    _start_time = _time.monotonic()
    round_num = 0

    def _push_log(entries: list[dict]) -> None:
        nonlocal recent_log
        for e in entries:
            recent_log.append(e)
        recent_log = recent_log[-_LOG_MAX:]

    def _make_detail(
        cur_job=None,
        *,
        disp_u=None,
        disp_s=None,
        disp_f=None,
    ) -> dict:
        u = disp_u if disp_u is not None else updated
        s = disp_s if disp_s is not None else skipped
        f = disp_f if disp_f is not None else failed
        elapsed = _time.monotonic() - _start_time
        done = u + s + f
        speed = done / elapsed if elapsed > 0 else 0
        remaining = max(0, total - done)
        eta_secs = int(remaining / speed) if speed > 0 else 0
        eta_min = eta_secs // 60
        eta_hrs = eta_min // 60
        eta_str = (
            f"~{eta_hrs}h {eta_min % 60}m"
            if eta_hrs > 0
            else (f"~{eta_min}m" if eta_min > 0 else f"~{eta_secs}s")
        )
        d = {
            "updated": u,
            "skipped": s,
            "failed": f,
            "remaining_global": remaining,
            "batch_num": round_num,
            "parallel_workers": parallelism,
            "workers_requested": workers_requested,
            "claim_size": claim_size,
            "parallelism_note": parallelism_note,
            "speed": round(speed, 1),
            "elapsed_secs": int(elapsed),
            "eta": eta_str,
            "log": list(recent_log),
        }
        if cur_job is not None:
            d["current_job"] = {
                "pk": cur_job.pk,
                "title": (cur_job.title or "")[:60],
                "company": (cur_job.company_name or "")[:40],
                "platform": cur_job.platform_slug or "",
                "url": (cur_job.original_url or "")[:200],
            }
        return d

    start_msg = (
        f"Starting — {total} jobs — {parallelism} worker(s) × {claim_size} rows/chunk"
    )
    if workers_requested != parallelism:
        start_msg += f" (you asked for {workers_requested})"
    if parallelism_note:
        start_msg += f" — {parallelism_note}"

    update_task_progress(
        self,
        current=0,
        total=total,
        message=start_msg,
        detail=_make_detail(),
    )

    try:
        while True:
            round_num += 1
            base_u, base_s, base_f = updated, skipped, failed

            if parallelism == 1:

                def _inline_progress(event, job=None, entry=None, lu=0, ls=0, lf=0):
                    nonlocal recent_log
                    if event == "job_done" and entry is not None:
                        recent_log.append(entry)
                        recent_log = recent_log[-_LOG_MAX:]
                    du, ds, df = base_u + lu, base_s + ls, base_f + lf
                    done = du + ds + df
                    cur = job
                    msg = (
                        f"Fetching: {(job.title or 'Untitled')[:50]} ({job.platform_slug or '?'})…"
                        if event == "job_start" and job is not None
                        else (
                            f"Progress — {du} updated, {ds} skipped, {df} failed"
                            if event == "job_done"
                            else "…"
                        )
                    )
                    update_task_progress(
                        self,
                        current=min(done, total),
                        total=total,
                        message=msg,
                        detail=_make_detail(
                            cur,
                            disp_u=du,
                            disp_s=ds,
                            disp_f=df,
                        ),
                    )

                chunk_results = [
                    _backfill_descriptions_chunk_impl(
                        claim_size,
                        platform_slug,
                        progress_hook=_inline_progress,
                        shard_index=0,
                        shard_count=1,
                    )
                ]
            else:
                # Parallel chunks run in a thread pool inside this task. Do not use
                # Celery group()+Result.get() here — Celery forbids blocking on subtask
                # results from within a task (RuntimeError), and join_native can still
                # call nested .get() with unsafe defaults.
                from concurrent.futures import ThreadPoolExecutor

                from django.db import close_old_connections

                def _one_shard(shard_index: int, shard_count: int) -> dict:
                    close_old_connections()
                    try:
                        return _backfill_descriptions_chunk_impl(
                            claim_size,
                            platform_slug,
                            shard_index=shard_index,
                            shard_count=shard_count,
                        )
                    finally:
                        close_old_connections()

                if _supports_select_for_update_skip_locked():
                    shard_specs = [(0, 1)] * parallelism
                else:
                    shard_specs = [(i, parallelism) for i in range(parallelism)]

                with ThreadPoolExecutor(max_workers=parallelism) as pool:
                    futures = [pool.submit(_one_shard, si, sc) for si, sc in shard_specs]
                    chunk_results = [fut.result() for fut in futures]

            round_claimed = 0
            for cr in chunk_results:
                if not isinstance(cr, dict):
                    continue
                round_claimed += int(cr.get("claimed") or 0)
                updated += int(cr.get("updated") or 0)
                skipped += int(cr.get("skipped") or 0)
                failed += int(cr.get("failed") or 0)
                _push_log(cr.get("log") or [])

            processed = updated + skipped + failed

            last_job = None
            if recent_log:
                last_pk = recent_log[-1].get("pk")
                if last_pk:
                    last_job = RawJob.objects.filter(pk=last_pk).first()

            update_task_progress(
                self,
                current=processed,
                total=total,
                message=(
                    f"Round {round_num} — {parallelism} workers — "
                    f"{updated} updated, {skipped} skipped, {failed} failed "
                    f"(claimed {round_claimed} this round)"
                ),
                detail=_make_detail(last_job),
            )

            if round_claimed == 0:
                break

    except SoftTimeLimitExceeded:
        logger.warning(
            "Backfill orchestrator soft time limit — processed %s jobs",
            processed,
        )
        return {
            "updated": updated,
            "skipped": skipped,
            "failed": failed,
            "remaining": _backfill_eligible_queryset(platform_slug).count(),
            "parallel_workers": parallelism,
        }

    remaining_n = _backfill_eligible_queryset(platform_slug).count()
    result = {
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "total_processed": processed,
        "remaining": remaining_n,
        "parallel_workers": parallelism,
        "claim_size": claim_size,
        "chained_next": False,
    }
    logger.info("Backfill descriptions FINISHED: %s", result)
    return result


# ─── Enrich Existing Jobs (no HTTP) ──────────────────────────────────────────

@shared_task(bind=True, name="harvest.enrich_existing_jobs")
def enrich_existing_jobs_task(
    self,
    batch_size: int = 2000,
    platform_slug: str | None = None,
    only_unenriched: bool = True,
    offset: int = 0,
):
    """
    Run extract_enrichments() on jobs already in the DB — no HTTP calls.

    Perfect for:
      - Jobs that already have descriptions (Greenhouse, Lever ~11k)
      - After a schema update adds new enrichment fields
      - Re-enriching all jobs after improving the extractor

    `only_unenriched=True`  → skips jobs that already have skills/category set
    `only_unenriched=False` → re-runs on every job (full re-enrich)

    Processes `batch_size` jobs per run at ~1000 jobs/sec (pure Python, no I/O).
    Safe to run multiple times.
    """
    from .enrichments import extract_enrichments
    from .models import RawJob

    qs = RawJob.objects.all()
    if platform_slug:
        qs = qs.filter(platform_slug=platform_slug)
    if only_unenriched:
        # Only jobs that have no skills AND no category yet
        qs = qs.filter(skills=[], job_category="")

    total = qs.count()
    if total == 0:
        return {"message": "Nothing to enrich.", "updated": 0}

    update_task_progress(self, current=0, total=total,
                         message=f"Found {total:,} jobs to enrich…")

    updated = skipped = 0
    jobs = list(qs.order_by("id")[offset: offset + batch_size])

    # Bulk-update in chunks of 500 for efficiency
    CHUNK = 500
    bulk_updates: list[RawJob] = []

    ENRICH_FIELDS = [
        "skills", "tech_stack", "job_category",
        "normalized_title", "title_keywords",
        "years_required", "years_required_max", "education_required",
        "visa_sponsorship", "work_authorization", "clearance_required", "clearance_level",
        "salary_equity", "signing_bonus", "relocation_assistance",
        "travel_required", "travel_pct_min", "travel_pct_max",
        "schedule_type", "shift_schedule", "shift_details", "hours_hint", "weekend_required",
        "certifications", "licenses_required", "benefits_list",
        "languages_required", "encouraged_to_apply",
        "job_keywords", "department_normalized",
        "word_count", "quality_score", "jd_quality_score",
        "classification_confidence", "classification_provenance",
        "field_confidence", "field_provenance",
        "resume_ready_score", "description_clean", "description_raw_html",
        "has_html_content", "cleaning_version",
    ]

    for idx, job in enumerate(jobs, start=1):
        enriched = extract_enrichments({
            "title":           job.title,
            "description":     job.description,
            "requirements":    job.requirements,
            "benefits":        job.benefits,
            "department":      job.department,
            "location_raw":    job.location_raw,
            "employment_type": job.employment_type,
            "experience_level":job.experience_level,
            "salary_raw":      job.salary_raw,
            "company_name":    job.company_name,
            "posted_date":     str(job.posted_date) if job.posted_date else "",
        })

        has_change = False
        for field in ENRICH_FIELDS:
            val = enriched.get(field)
            current = getattr(job, field, None)
            # Skip if no new value
            if val in (None, [], "", 0):
                continue
            if val != current:
                setattr(job, field, val)
                has_change = True

        if has_change:
            bulk_updates.append(job)
            updated += 1
        else:
            skipped += 1

        # Flush chunk
        if len(bulk_updates) >= CHUNK:
            RawJob.objects.bulk_update(bulk_updates, ENRICH_FIELDS)
            bulk_updates.clear()

        if idx % 100 == 0:
            update_task_progress(
                self,
                current=idx,
                total=len(jobs),
                message=f"Enriched {updated:,} / {idx:,} processed…",
            )

    # Flush remainder
    if bulk_updates:
        RawJob.objects.bulk_update(bulk_updates, ENRICH_FIELDS)

    result = {
        "updated":         updated,
        "skipped":         skipped,
        "total_processed": len(jobs),
        "total_eligible":  total,
        "remaining":       max(0, total - (offset + len(jobs))),
    }
    logger.info("Enrich existing jobs complete: %s", result)
    return result


@shared_task(bind=True, name="harvest.backfill_resume_contract")
def backfill_resume_contract_task(
    self,
    batch_size: int = 1500,
    offset: int = 0,
):
    """
    Backfill new resume-classification contract fields for historical rows.
    Safe to run repeatedly and in chunks.
    """
    from .enrichments import clean_job_content, extract_enrichments, normalize_job_title
    from .models import RawJob

    qs = RawJob.objects.select_related("company").order_by("pk")
    total = qs.count()
    if total == 0:
        return {"message": "No RawJobs found.", "updated": 0}

    jobs = list(qs[offset: offset + batch_size])
    if not jobs:
        return {"message": "No jobs in requested chunk.", "updated": 0, "remaining": 0}

    update_task_progress(
        self,
        current=0,
        total=len(jobs),
        message=f"Backfilling resume contract ({len(jobs)} jobs)…",
    )

    updated = 0
    CHUNK = 300
    bulk_updates: list[RawJob] = []
    update_fields = [
        "description_clean",
        "description_raw_html",
        "has_html_content",
        "cleaning_version",
        "normalized_title",
        "title_keywords",
        "schedule_type",
        "shift_details",
        "hours_hint",
        "weekend_required",
        "clearance_level",
        "travel_pct_min",
        "travel_pct_max",
        "licenses_required",
        "jd_quality_score",
        "classification_confidence",
        "classification_provenance",
        "field_confidence",
        "field_provenance",
        "resume_ready_score",
        "company_industry",
        "company_stage",
        "company_funding",
        "company_size",
        "company_employee_count_band",
        "company_founding_year",
    ]

    for idx, job in enumerate(jobs, start=1):
        desc_meta = clean_job_content(job.description or "", max_len=50000)
        enriched = extract_enrichments(
            {
                "title": job.title or "",
                "description": job.description or "",
                "requirements": job.requirements or "",
                "benefits": job.benefits or "",
                "department": job.department or "",
                "location_raw": job.location_raw or "",
                "employment_type": job.employment_type or "",
                "experience_level": job.experience_level or "",
                "salary_raw": job.salary_raw or "",
                "company_name": job.company_name or "",
                "country": job.country or "",
                "state": job.state or "",
                "posted_date": str(job.posted_date) if job.posted_date else "",
            }
        )
        company = job.company
        job.description_clean = (enriched.get("description_clean") or desc_meta["clean_text"] or "")[:50000]
        job.description_raw_html = (enriched.get("description_raw_html") or desc_meta["raw_html"] or "")[:120000]
        job.has_html_content = bool(enriched.get("has_html_content", desc_meta["has_html_content"]))
        job.cleaning_version = (enriched.get("cleaning_version") or "v2")[:20]
        job.normalized_title = (enriched.get("normalized_title") or normalize_job_title(job.title or ""))[:255]
        job.title_keywords = enriched.get("title_keywords") or job.title_keywords or []
        job.schedule_type = (enriched.get("schedule_type") or job.schedule_type or "")[:32]
        job.shift_details = (enriched.get("shift_details") or job.shift_details or "")[:255]
        job.hours_hint = (enriched.get("hours_hint") or job.hours_hint or "")[:64]
        job.weekend_required = enriched.get("weekend_required", job.weekend_required)
        job.clearance_level = (enriched.get("clearance_level") or job.clearance_level or "")[:64]
        job.travel_pct_min = enriched.get("travel_pct_min", job.travel_pct_min)
        job.travel_pct_max = enriched.get("travel_pct_max", job.travel_pct_max)
        job.licenses_required = enriched.get("licenses_required") or job.licenses_required or []
        job.jd_quality_score = enriched.get("jd_quality_score", job.jd_quality_score)
        job.classification_confidence = enriched.get("classification_confidence", job.classification_confidence)
        job.classification_provenance = enriched.get("classification_provenance") or job.classification_provenance or {}
        job.field_confidence = enriched.get("field_confidence") or job.field_confidence or {}
        job.field_provenance = enriched.get("field_provenance") or job.field_provenance or {}
        job.resume_ready_score = enriched.get("resume_ready_score", job.resume_ready_score)
        job.company_industry = ((job.company_industry or "") or (company.industry if company else "") or "")[:255]
        job.company_stage = ((job.company_stage or "") or (company.funding_stage if company else "") or "")[:64]
        job.company_funding = ((job.company_funding or "") or (company.funding_amount if company else "") or "")[:128]
        job.company_size = ((job.company_size or "") or (company.size_band if company else "") or (company.headcount_range if company else "") or "")[:64]
        job.company_employee_count_band = ((job.company_employee_count_band or "") or (company.employee_count_band if company else "") or (company.headcount_range if company else "") or "")[:64]
        job.company_founding_year = job.company_founding_year or (company.founding_year if company else None)
        bulk_updates.append(job)
        updated += 1

        if len(bulk_updates) >= CHUNK:
            RawJob.objects.bulk_update(bulk_updates, update_fields)
            bulk_updates.clear()

        if idx % 100 == 0:
            update_task_progress(
                self,
                current=idx,
                total=len(jobs),
                message=f"Backfilled {idx}/{len(jobs)} jobs…",
            )

    if bulk_updates:
        RawJob.objects.bulk_update(bulk_updates, update_fields)

    _invalidate_rawjobs_dashboard_cache()
    processed = len(jobs)
    remaining = max(0, total - (offset + processed))
    result = {
        "updated": updated,
        "total_processed": processed,
        "total_eligible": total,
        "remaining": remaining,
        "next_offset": offset + processed,
    }
    logger.info("Backfill resume contract complete: %s", result)
    return result
