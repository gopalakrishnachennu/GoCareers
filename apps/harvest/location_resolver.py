from __future__ import annotations

import json
import logging
import os
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass

from django.db.models import Count, Sum
from django.utils import timezone

from jobs.classifier import country as country_classifier

from .models import HarvestEngineConfig, LocationCache, RawJob


logger = logging.getLogger(__name__)


DEFAULT_TARGET_COUNTRIES = ["US", "IN", "CA", "GB", "AU"]

COUNTRY_NAME_TO_CODE = {
    "united states": "US",
    "usa": "US",
    "u.s.": "US",
    "us": "US",
    "america": "US",
    "india": "IN",
    "bharat": "IN",
    "united kingdom": "GB",
    "uk": "GB",
    "u.k.": "GB",
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "northern ireland": "GB",
    "great britain": "GB",
    "australia": "AU",
    "canada": "CA",
    "germany": "DE",
    "france": "FR",
    "netherlands": "NL",
    "ireland": "IE",
    "singapore": "SG",
    "new zealand": "NZ",
    "brazil": "BR",
    "mexico": "MX",
    "poland": "PL",
    "sweden": "SE",
    "switzerland": "CH",
    "united arab emirates": "AE",
    "uae": "AE",
}

COUNTRY_CODE_TO_NAME = {
    "US": "United States",
    "IN": "India",
    "GB": "United Kingdom",
    "AU": "Australia",
    "CA": "Canada",
    "DE": "Germany",
    "FR": "France",
    "NL": "Netherlands",
    "IE": "Ireland",
    "SG": "Singapore",
    "NZ": "New Zealand",
    "BR": "Brazil",
    "MX": "Mexico",
    "PL": "Poland",
    "SE": "Sweden",
    "CH": "Switzerland",
    "AE": "United Arab Emirates",
}

_PLACEHOLDER_LOCATION_VALUES = {
    "remote",
    "hybrid",
    "hybrid remote",
    "remote hybrid",
    "onsite",
    "on site",
    "on-site",
    "multiple locations",
    "various locations",
    "various",
    "global",
    "worldwide",
    "anywhere",
    "not specified",
    "unspecified",
    "n a",
    "na",
    "n/a",
    "emea",
    "apac",
    "europe",
}
_LOCATION_COUNT_RE = re.compile(r"^\d+\s+locations?$", re.I)

TARGET_DOMAIN_SLUGS = {
    # IT
    "software-developer",
    "backend-developer",
    "frontend-developer",
    "full-stack-developer",
    "mobile-developer",
    "ml-ai-engineer",
    "data-engineer",
    "data-analyst",
    "data-scientist",
    "devops-engineer",
    "cloud-engineer",
    "sre-platform-engineer",
    "security-engineer",
    "cybersecurity-engineer",
    "qa-test-engineer",
    "servicenow-developer",
    "servicenow-admin",
    "salesforce-developer",
    "sap-consultant",
    "oracle-consultant",
    "workday-consultant",
    "it-support-helpdesk",
    "network-systems-engineer",
    "systems-administrator",
    "database-administrator",
    "business-analyst-it",
    "systems-analyst",
    "it-project-manager",
    "scrum-master-agile-coach",
    "product-manager",
    "general-it",
    # Non-IT engineering
    "civil-engineer",
    "mechanical-engineer",
    "electrical-engineer",
    "structural-engineer",
    "manufacturing-engineer",
    "embedded-systems-engineer",
    "general-engineering",
}


@dataclass(frozen=True)
class LocationResolution:
    raw_text: str
    normalized_text: str
    country_code: str = ""
    country_name: str = ""
    region_code: str = ""
    region_name: str = ""
    city: str = ""
    confidence: float = 0.0
    source: str = "unknown"
    status: str = LocationCache.Status.UNKNOWN
    provider: str = ""
    provider_place_id: str = ""


def normalize_location_text(*parts: str) -> str:
    text = " ".join(str(part or "").strip() for part in parts if str(part or "").strip())
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"\b(remote|hybrid|onsite|on-site)\b", " ", text, flags=re.I)
    text = re.sub(r"[\|\u2022;]+", ",", text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"\s+", " ", text).strip(" ,").lower()
    return text[:512]


def _location_token(value: str) -> str:
    token = re.sub(r"<[^>]+>", " ", str(value or ""))
    token = token.replace("&nbsp;", " ")
    token = re.sub(r"[^a-z0-9]+", " ", token.lower())
    return re.sub(r"\s+", " ", token).strip()


def is_placeholder_location_value(value: str) -> bool:
    """True for location placeholders that are not geocodable places."""
    token = _location_token(value)
    if not token:
        return False
    if token in _PLACEHOLDER_LOCATION_VALUES:
        return True
    if _LOCATION_COUNT_RE.match(token):
        return True
    if "locations" in token and any(word in token for word in ("remote", "hybrid", "multiple", "various")):
        return True
    return False


def _is_ambiguous_location_only(*parts: str) -> bool:
    values = [str(part or "").strip() for part in parts if str(part or "").strip()]
    return bool(values) and all(is_placeholder_location_value(value) for value in values)


def _code_for_country(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    upper = value.upper()
    if len(upper) == 2 and upper.isalpha():
        return upper
    known = COUNTRY_NAME_TO_CODE.get(value.lower(), "")
    if known:
        return known
    # Skip obviously-not-country strings before calling coco — saves both noise
    # and CPU. coco prints "X not found in regex" to stderr for every miss.
    if (
        len(value) > 40
        or any(ch.isdigit() for ch in value)
        or any(ch in value for ch in "()[]{}/\\@#$%&*+=<>")
    ):
        return ""
    try:
        # Silence coco's stderr logging globally on first call.
        import country_converter as coco  # type: ignore
        import logging as _logging
        _coco_logger = _logging.getLogger("country_converter")
        if _coco_logger.level < _logging.CRITICAL:
            _coco_logger.setLevel(_logging.CRITICAL)
        result = coco.convert(names=[value], to="ISO2", not_found=None)
        if isinstance(result, list):
            result = result[0] if result else None
        if result and result != "not found":
            code = str(result).upper()
            if len(code) == 2:
                return code
    except Exception:
        pass
    return ""


def _split_location_parts(text: str) -> list[str]:
    return [part.strip() for part in re.split(r"[,|/]+", text or "") if part.strip()]


def _city_country_code(city: str) -> str:
    country = getattr(country_classifier, "_CITY_COUNTRY", {}).get((city or "").lower().strip(), "")
    return _code_for_country(country)


def _resolve_from_explicit_country(country: str, raw_text: str, normalized: str) -> LocationResolution | None:
    code = _code_for_country(country)
    if not code:
        return None
    return LocationResolution(
        raw_text=raw_text,
        normalized_text=normalized,
        country_code=code,
        country_name=COUNTRY_CODE_TO_NAME.get(code, country),
        confidence=0.98,
        source="ats_country",
        status=LocationCache.Status.RESOLVED,
    )


def _resolve_from_state_city(raw_text: str, normalized: str) -> LocationResolution | None:
    parts = _split_location_parts(raw_text)
    if not parts:
        return None

    city = parts[0]
    city_code = _city_country_code(city)
    region_code = ""
    for part in reversed(parts[1:]):
        token = re.sub(r"[^A-Za-z]", "", part).upper()
        if len(token) == 2:
            region_code = token
            break

    us_states = getattr(country_classifier, "_US_STATES", set())
    ca_provinces = getattr(country_classifier, "_CA_PROVINCES", set())

    if city_code and region_code == "CA" and city_code == "CA":
        return LocationResolution(
            raw_text=raw_text,
            normalized_text=normalized,
            country_code="CA",
            country_name="Canada",
            region_code="",
            city=city,
            confidence=0.93,
            source="city_dict",
            status=LocationCache.Status.RESOLVED,
        )

    if region_code in ca_provinces:
        return LocationResolution(
            raw_text=raw_text,
            normalized_text=normalized,
            country_code="CA",
            country_name="Canada",
            region_code=region_code,
            city=city,
            confidence=0.94,
            source="state_region",
            status=LocationCache.Status.RESOLVED,
        )

    if region_code in us_states:
        return LocationResolution(
            raw_text=raw_text,
            normalized_text=normalized,
            country_code="US",
            country_name="United States",
            region_code=region_code,
            city=city,
            confidence=0.94,
            source="state_region",
            status=LocationCache.Status.RESOLVED,
        )

    if city_code:
        return LocationResolution(
            raw_text=raw_text,
            normalized_text=normalized,
            country_code=city_code,
            country_name=COUNTRY_CODE_TO_NAME.get(city_code, ""),
            city=city,
            confidence=0.9,
            source="city_dict",
            status=LocationCache.Status.RESOLVED,
        )

    return None


def _resolve_from_classifier(raw_text: str, normalized: str, title: str = "", description: str = "") -> LocationResolution | None:
    country_name, region_name = country_classifier.detect_country(raw_text, title=title, description=description)
    code = _code_for_country(country_name)
    if not code:
        return None
    return LocationResolution(
        raw_text=raw_text,
        normalized_text=normalized,
        country_code=code,
        country_name=COUNTRY_CODE_TO_NAME.get(code, country_name),
        region_name=region_name if region_name not in {"Remote"} else "",
        confidence=0.86 if region_name else 0.88,
        source="rules",
        status=LocationCache.Status.RESOLVED,
    )


def _month_start():
    now = timezone.now()
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def provider_requests_this_month(provider: str) -> int:
    if not provider or provider == "none":
        return 0
    qs = LocationCache.objects.filter(
        provider=provider,
        looked_up_at__gte=_month_start(),
    )
    attempts = qs.aggregate(total=Sum("request_count"))["total"] or 0
    legacy_successes = qs.filter(request_count=0).exclude(provider_place_id="").count()
    return int(attempts) + int(legacy_successes)


def _provider_quota_available(cfg: HarvestEngineConfig) -> bool:
    provider = (cfg.geocoding_provider or "none").strip().lower()
    if not cfg.geocoding_provider_enabled or provider == "none":
        return False
    if provider_requests_this_month(provider) >= int(cfg.geocoding_monthly_limit or 0):
        return False
    return True


def _resolve_provider_token(provider: str, cfg: HarvestEngineConfig) -> str:
    """Token resolution priority: DB token (set via GUI) → env var.

    DB storage allows rotating the token from the portal without SSH access.
    Env var remains the more secure default — used when DB field is blank.
    """
    db_token = (cfg.geocoding_provider_token or "").strip()
    if db_token:
        return db_token
    if provider == "mapbox":
        return os.getenv("MAPBOX_ACCESS_TOKEN", "").strip()
    if provider == "google":
        return os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
    return ""


def _record_provider_attempt(provider: str, raw_text: str, normalized: str) -> None:
    if not normalized:
        return
    cache, _ = LocationCache.objects.get_or_create(
        normalized_text=normalized[:512],
        defaults={
            "raw_text": raw_text[:512],
            "source": "provider",
            "provider": provider,
            "status": LocationCache.Status.UNKNOWN,
        },
    )
    cache.raw_text = raw_text[:512]
    cache.source = "provider"
    cache.provider = provider
    cache.request_count = int(cache.request_count or 0) + 1
    if not cache.status:
        cache.status = LocationCache.Status.UNKNOWN
    cache.looked_up_at = timezone.now()
    cache.save(update_fields=[
        "raw_text",
        "source",
        "provider",
        "request_count",
        "status",
        "looked_up_at",
    ])


def _mapbox_geocode(raw_text: str, normalized: str, cfg: HarvestEngineConfig) -> LocationResolution | None:
    if not _provider_quota_available(cfg):
        return None
    token = _resolve_provider_token("mapbox", cfg)
    if not token:
        return None
    _record_provider_attempt("mapbox", raw_text, normalized)

    params = urllib.parse.urlencode({
        "q": raw_text,
        "access_token": token,
        "limit": "1",
        "types": "address,place,locality,region,country",
    })
    url = f"https://api.mapbox.com/search/geocode/v6/forward?{params}"
    try:
        with urllib.request.urlopen(url, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        logger.warning("Mapbox geocode failed for %s: %s", normalized, exc)
        return LocationResolution(
            raw_text=raw_text,
            normalized_text=normalized,
            confidence=0.0,
            source="provider",
            provider="mapbox",
            status=LocationCache.Status.FAILED,
        )

    features = payload.get("features") or []
    if not features:
        return LocationResolution(
            raw_text=raw_text,
            normalized_text=normalized,
            confidence=0.0,
            source="provider",
            provider="mapbox",
            status=LocationCache.Status.UNKNOWN,
        )

    feature = features[0]
    props = feature.get("properties") or {}
    context = props.get("context") or {}
    country = context.get("country") or {}
    region = context.get("region") or {}
    place = context.get("place") or {}

    country_code = _code_for_country(country.get("country_code", ""))
    if not country_code:
        country_code = _code_for_country(country.get("name", ""))
    if not country_code:
        return LocationResolution(
            raw_text=raw_text,
            normalized_text=normalized,
            confidence=0.0,
            source="provider",
            provider="mapbox",
            provider_place_id=str(props.get("mapbox_id") or feature.get("id") or ""),
            status=LocationCache.Status.UNKNOWN,
        )

    return LocationResolution(
        raw_text=raw_text,
        normalized_text=normalized,
        country_code=country_code,
        country_name=COUNTRY_CODE_TO_NAME.get(country_code, country.get("name", "")),
        region_code=(region.get("region_code") or "").upper(),
        region_name=region.get("name", ""),
        city=place.get("name", ""),
        confidence=0.97,
        source="provider",
        provider="mapbox",
        provider_place_id=str(props.get("mapbox_id") or feature.get("id") or ""),
        status=LocationCache.Status.RESOLVED,
    )


def _cache_resolution(resolution: LocationResolution) -> LocationResolution:
    if not resolution.normalized_text:
        return resolution
    LocationCache.objects.update_or_create(
        normalized_text=resolution.normalized_text,
        defaults={
            "raw_text": resolution.raw_text[:512],
            "country_code": resolution.country_code,
            "country_name": resolution.country_name,
            "region_code": resolution.region_code,
            "region_name": resolution.region_name,
            "city": resolution.city,
            "confidence": resolution.confidence,
            "source": resolution.source,
            "provider": resolution.provider,
            "provider_place_id": resolution.provider_place_id,
            "status": resolution.status,
        },
    )
    return resolution


def resolve_location(
    *,
    location_raw: str = "",
    city: str = "",
    state: str = "",
    country: str = "",
    title: str = "",
    description: str = "",
    cfg: HarvestEngineConfig | None = None,
    use_provider: bool = False,
) -> LocationResolution:
    cfg = cfg or HarvestEngineConfig.get()
    raw_text = ", ".join(part for part in [location_raw, city, state, country] if (part or "").strip())
    raw_text = raw_text or location_raw or city or state or country
    normalized = normalize_location_text(raw_text)
    if not normalized:
        if _is_ambiguous_location_only(location_raw, city, state, country):
            return LocationResolution(
                raw_text=raw_text,
                normalized_text=_location_token(raw_text)[:512],
                confidence=0.0,
                source="ambiguous_multi_location",
                status=LocationCache.Status.UNKNOWN,
            )
        return LocationResolution(raw_text="", normalized_text="", source="empty")
    placeholder_only = _is_ambiguous_location_only(location_raw, city, state, country)
    country_for_rules = "" if is_placeholder_location_value(country) else country
    raw_text_for_rules = "" if placeholder_only else raw_text

    if cfg.geocoding_cache_enabled:
        cached = LocationCache.objects.filter(normalized_text=normalized).first()
        # Skip cache hit if previous resolution was UNKNOWN AND provider is now
        # available — gives the upgraded resolver a chance to retry via Mapbox.
        # Otherwise UNKNOWN cache entries from earlier no-provider runs would
        # permanently shadow the provider call.
        cache_is_unknown = cached and cached.status == LocationCache.Status.UNKNOWN
        provider_now_available = (
            use_provider
            and cfg.geocoding_provider_enabled
            and cfg.geocoding_provider in {"mapbox", "google"}
            and bool(_resolve_provider_token(cfg.geocoding_provider, cfg))
        )
        if cached and not (cache_is_unknown and provider_now_available):
            return LocationResolution(
                raw_text=cached.raw_text or raw_text,
                normalized_text=cached.normalized_text,
                country_code=cached.country_code,
                country_name=cached.country_name,
                region_code=cached.region_code,
                region_name=cached.region_name,
                city=cached.city,
                confidence=cached.confidence,
                source=cached.source,
                status=cached.status,
                provider=cached.provider,
                provider_place_id=cached.provider_place_id,
            )

    resolution = (
        _resolve_from_explicit_country(country_for_rules, raw_text, normalized)
        or _resolve_from_state_city(raw_text_for_rules, normalized)
        or _resolve_from_classifier(raw_text_for_rules, normalized, title=title, description=description)
    )

    if not resolution and use_provider and cfg.geocoding_provider == "mapbox" and not placeholder_only:
        resolution = _mapbox_geocode(raw_text, normalized, cfg)

    if not resolution:
        resolution = LocationResolution(
            raw_text=raw_text,
            normalized_text=normalized,
            confidence=0.0,
            source="ambiguous_multi_location" if placeholder_only else "unknown",
            status=LocationCache.Status.UNKNOWN,
        )

    if cfg.geocoding_cache_enabled:
        _cache_resolution(resolution)
    return resolution


def has_target_domain_signal(raw_job: RawJob) -> bool:
    if raw_job.job_domain and raw_job.job_domain in TARGET_DOMAIN_SLUGS:
        return True
    for slug in raw_job.job_domain_candidates or []:
        if slug in TARGET_DOMAIN_SLUGS:
            return True

    try:
        from .enrichments import detect_job_domains
        slugs = detect_job_domains(
            raw_job.title or "",
            raw_job.description or "",
            raw_job.job_category or "",
            raw_job.department_normalized or "",
            max_matches=3,
        )
    except Exception:
        slugs = []
    return any(slug in TARGET_DOMAIN_SLUGS for slug in slugs)


def evaluate_rawjob_scope(
    raw_job: RawJob,
    *,
    cfg: HarvestEngineConfig | None = None,
    use_provider: bool = False,
    save: bool = False,
) -> dict:
    cfg = cfg or HarvestEngineConfig.get()
    resolution = resolve_location(
        location_raw=raw_job.location_raw or "",
        city=raw_job.city or "",
        state=raw_job.state or "",
        country=raw_job.country or "",
        title=raw_job.title or "",
        description=raw_job.description or raw_job.description_clean or "",
        cfg=cfg,
        use_provider=use_provider,
    )

    target_countries = set(cfg.get_target_countries() or DEFAULT_TARGET_COUNTRIES)
    updates = {
        "country_code": resolution.country_code,
        "country_confidence": resolution.confidence,
        "country_source": resolution.source,
        "last_scope_evaluated_at": timezone.now(),
    }

    country_is_placeholder = is_placeholder_location_value(raw_job.country or "")
    state_is_placeholder = is_placeholder_location_value(raw_job.state or "")
    city_is_placeholder = is_placeholder_location_value(raw_job.city or "")

    if country_is_placeholder:
        updates["country"] = ""
    if state_is_placeholder:
        updates["state"] = ""
    if city_is_placeholder:
        updates["city"] = ""

    if resolution.country_name and (not raw_job.country or country_is_placeholder):
        updates["country"] = resolution.country_name
    if resolution.region_code and (not raw_job.state or state_is_placeholder):
        updates["state"] = resolution.region_code
    if resolution.city and (not raw_job.city or city_is_placeholder):
        updates["city"] = resolution.city

    if resolution.country_code in target_countries:
        updates.update({
            "scope_status": RawJob.ScopeStatus.PRIORITY_TARGET,
            "scope_reason": f"target_country:{resolution.country_code}",
            "is_priority": True,
        })
    elif not resolution.country_code:
        if cfg.process_unknown_country_with_target_domain and has_target_domain_signal(raw_job):
            updates.update({
                "scope_status": RawJob.ScopeStatus.REVIEW_UNKNOWN_COUNTRY,
                "scope_reason": (
                    "ambiguous_multi_location_target_domain"
                    if resolution.source == "ambiguous_multi_location"
                    else "unknown_country_target_domain"
                ),
                "is_priority": True,
            })
        else:
            updates.update({
                "scope_status": RawJob.ScopeStatus.COLD_NO_LOCATION if not resolution.normalized_text else RawJob.ScopeStatus.REVIEW_UNKNOWN_COUNTRY,
                "scope_reason": (
                    "ambiguous_multi_location"
                    if resolution.source == "ambiguous_multi_location"
                    else "country_unknown"
                ),
                "is_priority": False,
            })
    else:
        updates.update({
            "scope_status": RawJob.ScopeStatus.COLD_NON_TARGET_COUNTRY,
            "scope_reason": f"non_target_country:{resolution.country_code}",
            "is_priority": False,
        })

    if save:
        for field, value in updates.items():
            setattr(raw_job, field, value)
        raw_job.save(update_fields=list(updates.keys()) + ["updated_at"])
    return updates


def scope_counts():
    return {
        row["scope_status"]: row["count"]
        for row in RawJob.objects.values("scope_status").annotate(count=Count("id"))
    }
