"""
configure_geocoding_provider
============================
One-shot command to set up geocoding from CI without piping Python into
`manage.py shell` (which the SSH action's command wrapper breaks).

Reads token from MAPBOX_TOKEN env, writes to HarvestEngineConfig.

Usage:
    MAPBOX_TOKEN=pk.… python manage.py configure_geocoding_provider \
        --provider mapbox --enable --cache
"""
from __future__ import annotations

import os
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Configure HarvestEngineConfig geocoding fields without exposing the token"

    def add_arguments(self, parser):
        parser.add_argument("--provider", choices=["none", "mapbox", "google"],
                            default="mapbox")
        parser.add_argument("--enable", action="store_true",
                            help="Set geocoding_provider_enabled=True")
        parser.add_argument("--disable", action="store_true",
                            help="Set geocoding_provider_enabled=False")
        parser.add_argument("--cache", action="store_true",
                            help="Set geocoding_cache_enabled=True")
        parser.add_argument("--clear-token", action="store_true",
                            help="Wipe the DB token (resolver falls back to env var)")
        parser.add_argument("--token-env", default="MAPBOX_TOKEN",
                            help="Env var name to read token from (default MAPBOX_TOKEN)")

    def handle(self, *args, **options):
        from harvest.models import HarvestEngineConfig

        cfg = HarvestEngineConfig.get()
        cfg.geocoding_provider = options["provider"]

        if options["enable"]:
            cfg.geocoding_provider_enabled = True
        if options["disable"]:
            cfg.geocoding_provider_enabled = False
        if options["cache"]:
            cfg.geocoding_cache_enabled = True

        if options["clear_token"]:
            cfg.geocoding_provider_token = ""
        else:
            tok = os.environ.get(options["token_env"], "").strip()
            if tok:
                cfg.geocoding_provider_token = tok

        cfg.save()

        self.stdout.write("OK config saved")
        self.stdout.write(f"  provider               = {cfg.geocoding_provider}")
        self.stdout.write(f"  provider_enabled       = {cfg.geocoding_provider_enabled}")
        self.stdout.write(f"  cache_enabled          = {cfg.geocoding_cache_enabled}")
        self.stdout.write(f"  monthly_limit          = {cfg.geocoding_monthly_limit:,}")
        # Never print the token value
        tok_len = len(cfg.geocoding_provider_token or "")
        if tok_len:
            tok = cfg.geocoding_provider_token
            mask = f"{tok[:6]}…{tok[-4:]}" if tok_len >= 12 else "•" * tok_len
            self.stdout.write(f"  token_db               = present (len={tok_len}, {mask})")
        else:
            self.stdout.write("  token_db               = (empty — will fall back to env var)")
