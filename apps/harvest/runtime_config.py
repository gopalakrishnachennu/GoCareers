from __future__ import annotations

import logging

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone

logger = logging.getLogger(__name__)

DEFAULT_READY_STAGE_MIN_CONFIDENCE = 0.55
DEFAULT_JD_BACKFILL_LOCK_STALE_MINUTES = 15


class HarvestConfigReadError(RuntimeError):
    """Raised when a harvest worker cannot read required runtime config."""


def _record_config_read_failure(context: str, exc: Exception) -> None:
    """Best-effort ops audit for config failures; never raises."""
    cache_key = f"harvest:config-read-failed:{context}"
    try:
        if not cache.add(cache_key, True, timeout=300):
            return
    except Exception:
        pass

    try:
        from .models import HarvestOpsRun

        HarvestOpsRun.objects.create(
            operation=HarvestOpsRun.Operation.CONFIG_FAILURE,
            status=HarvestOpsRun.Status.FAILED,
            finished_at=timezone.now(),
            audit_payload={
                "completion": {
                    "context": context,
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:500],
                }
            },
        )
    except Exception:
        logger.exception("Failed to persist HarvestEngineConfig failure audit for %s", context)


def get_harvest_engine_config(context: str, *, fail_fast: bool = False):
    """
    Read HarvestEngineConfig with explicit failure behavior.

    Workers call this with fail_fast=True so config outages do not silently run with
    stale hardcoded values. Read-only/dashboard paths can use the default and apply a
    visible fallback.
    """
    try:
        from .models import HarvestEngineConfig

        return HarvestEngineConfig.get()
    except Exception as exc:
        logger.exception("HarvestEngineConfig read failed in %s", context)
        _record_config_read_failure(context, exc)
        if fail_fast:
            raise HarvestConfigReadError(
                f"HarvestEngineConfig read failed in {context}: {type(exc).__name__}"
            ) from exc
        return None


def require_harvest_engine_config(context: str):
    return get_harvest_engine_config(context, fail_fast=True)


def get_ready_stage_min_confidence() -> float:
    cache_key = "harvest:ready-stage-min-confidence:v1"
    cached = cache.get(cache_key)
    if cached is not None:
        return float(cached)

    cfg = get_harvest_engine_config("ready_stage_min_confidence")
    if cfg is None:
        value = float(getattr(settings, "HARVEST_READY_STAGE_MIN_CONFIDENCE", DEFAULT_READY_STAGE_MIN_CONFIDENCE))
    else:
        value = float(getattr(cfg, "ready_stage_min_confidence", DEFAULT_READY_STAGE_MIN_CONFIDENCE))
    value = max(0.0, min(1.0, value))
    cache.set(cache_key, value, timeout=30)
    return value


def get_jd_backfill_lock_stale_minutes() -> int:
    cache_key = "harvest:jd-backfill-lock-stale-minutes:v1"
    cached = cache.get(cache_key)
    if cached is not None:
        return int(cached)

    cfg = get_harvest_engine_config("jd_backfill_lock_stale_minutes")
    if cfg is None:
        value = int(getattr(settings, "HARVEST_JD_BACKFILL_LOCK_STALE_MINUTES", DEFAULT_JD_BACKFILL_LOCK_STALE_MINUTES))
    else:
        value = int(getattr(cfg, "jd_backfill_lock_stale_minutes", DEFAULT_JD_BACKFILL_LOCK_STALE_MINUTES) or DEFAULT_JD_BACKFILL_LOCK_STALE_MINUTES)
    value = max(1, min(240, value))
    cache.set(cache_key, value, timeout=30)
    return value


def legacy_hash_bridge_enabled() -> bool:
    cache_key = "harvest:legacy-hash-bridge-enabled:v1"
    cached = cache.get(cache_key)
    if cached is not None:
        return bool(cached)

    cfg = get_harvest_engine_config("legacy_hash_bridge_enabled")
    if cfg is None:
        enabled = bool(getattr(settings, "HARVEST_LEGACY_HASH_BRIDGE_ENABLED", True))
    else:
        enabled = bool(getattr(cfg, "legacy_hash_bridge_enabled", True))
    cache.set(cache_key, enabled, timeout=60)
    return enabled
