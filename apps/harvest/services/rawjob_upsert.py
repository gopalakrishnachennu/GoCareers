from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass

from django.db import IntegrityError, connection, transaction
from django.db.models import Q

from harvest.models import RawJob
from harvest.runtime_config import legacy_hash_bridge_enabled

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RawJobUpsertResult:
    raw_job: RawJob | None
    created: bool
    action: str
    reason: str = ""
    duplicate_pk: int | None = None


def _platform_identity_q(platform_label=None, job_platform=None, platform_slug: str = "") -> Q:
    q = Q()
    if platform_label is not None:
        q |= Q(platform_label=platform_label)
    if job_platform is not None:
        q |= Q(job_platform=job_platform)
    if platform_slug:
        q |= Q(platform_slug=platform_slug[:64])
    q |= Q(job_platform__isnull=True)
    return q


def _row_matches_platform(row: RawJob, *, platform_label=None, job_platform=None, platform_slug: str = "") -> bool:
    if platform_label is not None and row.platform_label_id == platform_label.pk:
        return True
    if job_platform is not None and row.job_platform_id == job_platform.pk:
        return True
    if platform_slug and (row.platform_slug or "") == platform_slug[:64]:
        return True
    return row.job_platform_id is None


def _advisory_identity_lock(lock_key: str) -> None:
    """Serialize concurrent upserts for the same logical job on PostgreSQL."""
    if connection.vendor != "postgresql" or not lock_key:
        return
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", [lock_key[:512]])
    except Exception:
        logger.exception("RawJob advisory lock failed for key %s", lock_key[:120])


def _save_existing(row: RawJob, defaults: dict, url_hash: str) -> RawJob:
    for field, value in defaults.items():
        setattr(row, field, value)
    row.url_hash = url_hash
    row.save()
    return row


def _candidate_query(
    *,
    company,
    url_hash: str,
    original_url: str,
    external_id: str,
    content_hash: str,
    legacy_hash: str,
    platform_label=None,
    job_platform=None,
    platform_slug: str = "",
) -> Q:
    query = Q(url_hash=url_hash)
    if legacy_hash:
        query |= Q(url_hash=legacy_hash)

    platform_q = _platform_identity_q(platform_label, job_platform, platform_slug)
    if external_id:
        query |= Q(company=company, external_id=external_id) & platform_q

    base_url = original_url.split("?", 1)[0].strip()
    if base_url:
        query |= Q(company=company, original_url__startswith=base_url) & platform_q

    if content_hash:
        query |= Q(company=company, content_hash=content_hash, is_active=True)
    return query


def upsert_raw_job_with_dedupe(
    *,
    company,
    defaults: dict,
    url_hash: str,
    original_url: str,
    external_id: str = "",
    platform_label=None,
    job_platform=None,
    platform_slug: str = "",
) -> RawJobUpsertResult:
    """
    Atomically decide RawJob identity before writing.

    This replaces the old waterfall checks by locking all candidate identities,
    choosing one winner, and only then updating/creating/skipping.
    """
    clean_defaults = dict(defaults)
    clean_defaults.setdefault("company", company)
    clean_defaults.setdefault("external_id", external_id[:512])
    clean_defaults.setdefault("original_url", original_url[:1024])
    if platform_label is not None:
        clean_defaults.setdefault("platform_label", platform_label)
    if job_platform is not None:
        clean_defaults.setdefault("job_platform", job_platform)
    if platform_slug:
        clean_defaults.setdefault("platform_slug", platform_slug[:64])

    external_id = (external_id or "").strip()[:512]
    original_url = (original_url or "").strip()
    content_hash = (clean_defaults.get("content_hash") or "").strip()
    base_url = original_url.split("?", 1)[0].strip()
    legacy_hash = ""
    if original_url and legacy_hash_bridge_enabled():
        legacy_hash = hashlib.sha256(original_url.encode("utf-8")).hexdigest()
        if legacy_hash == url_hash:
            legacy_hash = ""

    lock_identity = content_hash or external_id or base_url or url_hash
    lock_key = f"rawjob-upsert:{company.pk}:{lock_identity}"

    for attempt in range(2):
        try:
            with transaction.atomic():
                _advisory_identity_lock(lock_key)
                candidate_q = _candidate_query(
                    company=company,
                    url_hash=url_hash,
                    original_url=original_url,
                    external_id=external_id,
                    content_hash=content_hash,
                    legacy_hash=legacy_hash,
                    platform_label=platform_label,
                    job_platform=job_platform,
                    platform_slug=platform_slug,
                )
                candidates = list(RawJob.objects.select_for_update().filter(candidate_q).order_by("pk"))

                exact = next((row for row in candidates if row.url_hash == url_hash), None)
                external = next(
                    (
                        row for row in candidates
                        if external_id
                        and row.company_id == company.pk
                        and row.external_id == external_id
                        and _row_matches_platform(
                            row,
                            platform_label=platform_label,
                            job_platform=job_platform,
                            platform_slug=platform_slug,
                        )
                    ),
                    None,
                )
                variant = next(
                    (
                        row for row in candidates
                        if base_url
                        and row.company_id == company.pk
                        and (row.original_url or "").startswith(base_url)
                        and row.url_hash != url_hash
                        and _row_matches_platform(
                            row,
                            platform_label=platform_label,
                            job_platform=job_platform,
                            platform_slug=platform_slug,
                        )
                    ),
                    None,
                )
                legacy = next(
                    (row for row in candidates if legacy_hash and row.url_hash == legacy_hash),
                    None,
                )
                content_dup = next(
                    (
                        row for row in candidates
                        if content_hash
                        and row.company_id == company.pk
                        and row.content_hash == content_hash
                        and row.is_active
                        and row.url_hash != url_hash
                    ),
                    None,
                )

                target = exact or external or variant or legacy
                if content_dup is not None and (target is None or target.pk != content_dup.pk):
                    return RawJobUpsertResult(
                        raw_job=None,
                        created=False,
                        action="duplicate",
                        reason="content_hash_duplicate",
                        duplicate_pk=content_dup.pk,
                    )
                if target is not None:
                    if target.sync_status == RawJob.SyncStatus.SYNCED:
                        return RawJobUpsertResult(
                            raw_job=None,
                            created=False,
                            action="duplicate",
                            reason="already_synced",
                            duplicate_pk=target.pk,
                        )
                    return RawJobUpsertResult(
                        raw_job=_save_existing(target, clean_defaults, url_hash),
                        created=False,
                        action="updated",
                        reason=(
                            "url_hash"
                            if target is exact
                            else "external_id"
                            if target is external
                            else "query_variant"
                            if target is variant
                            else "legacy_hash"
                        ),
                    )

                raw_job = RawJob.objects.create(url_hash=url_hash, **clean_defaults)
                return RawJobUpsertResult(raw_job=raw_job, created=True, action="created")
        except IntegrityError:
            if attempt == 0:
                continue
            raise

    return RawJobUpsertResult(raw_job=None, created=False, action="duplicate", reason="integrity_race")
