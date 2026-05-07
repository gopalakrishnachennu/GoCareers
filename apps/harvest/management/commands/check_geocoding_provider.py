"""
check_geocoding_provider
========================
Read-only diagnostic that reports the current state of the geocoding
provider config and makes ONE live test call (does not count toward
the monthly cap meaningfully).

Output is safe to print to logs — never displays the token value.

Usage:
    python manage.py check_geocoding_provider
    python manage.py check_geocoding_provider --query "Tokyo, Japan"
    python manage.py check_geocoding_provider --skip-call    # no Mapbox call
"""
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Diagnose geocoding provider config + run one test call"

    def add_arguments(self, parser):
        parser.add_argument("--query", default="Bangalore, India",
                            help="Test string to geocode (default: 'Bangalore, India')")
        parser.add_argument("--skip-call", action="store_true",
                            help="Print config only, do not hit Mapbox")

    def handle(self, *args, **options):
        from harvest.location_resolver import _resolve_provider_token, provider_requests_this_month
        from harvest.models import HarvestEngineConfig

        cfg = HarvestEngineConfig.get()
        provider = (cfg.geocoding_provider or "none").strip().lower()

        self.stdout.write("=" * 60)
        self.stdout.write("GEOCODING PROVIDER DIAGNOSTIC")
        self.stdout.write("=" * 60)
        self.stdout.write(f"  cfg.geocoding_provider          : {provider}")
        self.stdout.write(f"  cfg.geocoding_provider_enabled  : {cfg.geocoding_provider_enabled}")
        self.stdout.write(f"  cfg.geocoding_cache_enabled     : {cfg.geocoding_cache_enabled}")
        self.stdout.write(f"  cfg.geocoding_monthly_limit     : {cfg.geocoding_monthly_limit:,}")
        self.stdout.write(f"  used this month                 : {provider_requests_this_month(provider):,}")

        # Token sources — never print the value, only presence
        db_token = (cfg.geocoding_provider_token or "").strip()
        env_var = "MAPBOX_ACCESS_TOKEN" if provider == "mapbox" else (
            "GOOGLE_MAPS_API_KEY" if provider == "google" else ""
        )
        env_token = os.getenv(env_var, "").strip() if env_var else ""

        self.stdout.write("")
        self.stdout.write(f"  DB token present                : {bool(db_token)}")
        if db_token:
            masked = f"{db_token[:6]}…{db_token[-4:]}" if len(db_token) >= 12 else "•" * len(db_token)
            self.stdout.write(f"  DB token (masked)               : {masked}  (len={len(db_token)})")
        self.stdout.write(f"  ENV var ({env_var or 'n/a'}): {'present' if env_token else 'missing'}")
        if env_token:
            masked_env = f"{env_token[:6]}…{env_token[-4:]}" if len(env_token) >= 12 else "•" * len(env_token)
            self.stdout.write(f"  ENV token (masked)              : {masked_env}  (len={len(env_token)})")

        resolved_token = _resolve_provider_token(provider, cfg)
        self.stdout.write("")
        self.stdout.write(f"  Resolver will use               : "
                          f"{'DB' if db_token else ('ENV' if env_token else 'NONE — provider call disabled')}")
        self.stdout.write(f"  Resolver token len              : {len(resolved_token)}")

        # Pre-call gate checks
        self.stdout.write("")
        self.stdout.write("PRE-CALL GATE CHECKS")
        self.stdout.write("-" * 60)
        gate_a = bool(cfg.geocoding_provider_enabled)
        gate_b = provider in ("mapbox", "google")
        gate_c = bool(resolved_token)
        gate_d = provider_requests_this_month(provider) < int(cfg.geocoding_monthly_limit or 0)
        self.stdout.write(f"  [{'✓' if gate_a else '✗'}] provider_enabled = True")
        self.stdout.write(f"  [{'✓' if gate_b else '✗'}] provider in (mapbox, google)")
        self.stdout.write(f"  [{'✓' if gate_c else '✗'}] token resolved (DB or env)")
        self.stdout.write(f"  [{'✓' if gate_d else '✗'}] monthly quota not exhausted")

        all_pass = gate_a and gate_b and gate_c and gate_d
        if not all_pass:
            self.stdout.write("")
            self.stdout.write(self.style.ERROR(
                "❌  Provider call would be SKIPPED. Fix the failing gate above."
            ))
            return

        if options["skip_call"]:
            self.stdout.write("")
            self.stdout.write(self.style.SUCCESS("✓ All gates pass. (--skip-call set, not making live call)"))
            return

        # Make ONE live call
        query = options["query"]
        self.stdout.write("")
        self.stdout.write("LIVE CALL")
        self.stdout.write("-" * 60)
        self.stdout.write(f"  Query   : {query}")

        if provider != "mapbox":
            self.stdout.write(self.style.WARNING(
                f"  Live test only implemented for mapbox; provider={provider}. Skipping."
            ))
            return

        params = urllib.parse.urlencode({
            "q": query, "access_token": resolved_token, "limit": "1",
            "types": "address,place,locality,region,country",
        })
        url = f"https://api.mapbox.com/search/geocode/v6/forward?{params}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                status = resp.status
                payload = json.loads(resp.read().decode("utf-8"))
            self.stdout.write(f"  HTTP    : {status}")
            features = payload.get("features") or []
            self.stdout.write(f"  Features: {len(features)}")
            if features:
                f0 = features[0]
                props = f0.get("properties") or {}
                ctx = props.get("context") or {}
                country = (ctx.get("country") or {}).get("name", "")
                cc = (ctx.get("country") or {}).get("country_code", "")
                region = (ctx.get("region") or {}).get("name", "")
                place = (ctx.get("place") or {}).get("name", "")
                self.stdout.write(f"  Country : {country} ({cc})")
                self.stdout.write(f"  Region  : {region}")
                self.stdout.write(f"  City    : {place}")
                self.stdout.write("")
                self.stdout.write(self.style.SUCCESS("✓✓✓  MAPBOX IS WORKING  ✓✓✓"))
            else:
                self.stdout.write(self.style.WARNING("  No features returned — query may be too vague."))
        except urllib.error.HTTPError as exc:
            self.stdout.write(self.style.ERROR(f"  HTTP error: {exc.code} {exc.reason}"))
            try:
                body = exc.read().decode("utf-8")[:500]
                self.stdout.write(f"  Body    : {body}")
            except Exception:
                pass
            if exc.code in (401, 403):
                self.stdout.write(self.style.ERROR("  → Token is INVALID or EXPIRED. Replace it via GUI."))
        except Exception as exc:
            self.stdout.write(self.style.ERROR(f"  Network error: {type(exc).__name__}: {exc}"))
