from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from harvest.models import HarvestFilterSnapshot


class Command(BaseCommand):
    help = "Export a selective harvest filter snapshot as JSON."

    def add_arguments(self, parser):
        parser.add_argument("--snapshot-id", required=True, help="Snapshot UUID or database id.")
        parser.add_argument("--dry-run", action="store_true", help="Accepted for ops consistency; this command is read-only.")

    def handle(self, *args, **options):
        ident = str(options["snapshot_id"]).strip()
        qs = HarvestFilterSnapshot.objects.all()
        snapshot = qs.filter(snapshot_id=ident).first() if "-" in ident else qs.filter(pk=ident).first()
        if not snapshot:
            raise CommandError(f"Snapshot not found: {ident}")
        payload = {
            "snapshot_id": str(snapshot.snapshot_id),
            "taken_at": snapshot.taken_at.isoformat(),
            "batch_id": snapshot.batch_id,
            "phrase_hash": snapshot.phrase_hash,
            "category_data": snapshot.category_data,
        }
        self.stdout.write(json.dumps(payload, indent=2, sort_keys=True))
