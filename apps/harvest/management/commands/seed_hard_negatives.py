"""seed_hard_negatives — load pure healthcare/non-tech title blockers.

Hard negatives fire at TITLE level and produce NO_MATCH immediately —
the job is stored but its JD is never fetched and it never reaches the pool.

They only fire when NO include-phrase from any HarvestRoleCategory matched.
So "Healthcare IT Analyst" is safe: "healthcare it" include phrase wins first.
"Registered Nurse" has no include match → hard negative "registered nurse" fires → NO_MATCH.

Usage:
    python manage.py seed_hard_negatives          # preview
    python manage.py seed_hard_negatives --apply  # write to HarvestEngineConfig
"""
from __future__ import annotations

from django.core.management.base import BaseCommand

# ── Hard negative phrases ─────────────────────────────────────────────────────
# Rules:
#   1. Multi-word preferred — single words like "nurse" are risky because
#      "nurse practitioner" → fine to block but "nursing informatics" → IT role
#      (though "nursing informatics" is not one of our IT marketing roles either).
#   2. These fire ONLY when no include-phrase matched → safe to be specific.
#   3. After title normalization, seniority words (senior/staff/lead) are stripped,
#      so "Senior Registered Nurse" normalizes to "registered nurse" → still blocked.
# ─────────────────────────────────────────────────────────────────────────────
HARD_NEGATIVES = [
    # ── Nursing ──────────────────────────────────────────────────────────────
    "registered nurse",
    "licensed practical nurse",
    "licensed vocational nurse",
    "nurse practitioner",
    "clinical nurse",
    "travel nurse",
    "charge nurse",
    "staff nurse",
    "rn bsn",
    "rn msn",
    "nursing assistant",
    "certified nursing assistant",
    "cna",
    "nursing coordinator",
    "nursing supervisor",
    "nurse manager",
    "director of nursing",
    "chief nursing officer",

    # ── Physician / Medical Doctor ────────────────────────────────────────────
    "physician",
    "medical doctor",
    "hospitalist",
    "intensivist",
    "radiologist",
    "anesthesiologist",
    "psychiatrist",
    "dermatologist",
    "cardiologist",
    "neurologist",
    "oncologist",
    "surgeon",
    "urologist",
    "gastroenterologist",
    "ophthalmologist",
    "orthopedic surgeon",
    "resident physician",
    "fellow physician",
    "locum physician",

    # ── Pharmacy ─────────────────────────────────────────────────────────────
    "pharmacist",
    "pharmacy technician",
    "pharmacy intern",
    "clinical pharmacist",
    "retail pharmacist",
    "hospital pharmacist",

    # ── Allied Health / Therapy ───────────────────────────────────────────────
    "physical therapist",
    "occupational therapist",
    "speech therapist",
    "speech language pathologist",
    "respiratory therapist",
    "radiation therapist",
    "medical laboratory technician",
    "medical laboratory scientist",
    "clinical laboratory scientist",
    "lab technologist",
    "phlebotomist",
    "surgical technician",
    "sterile processing technician",
    "central sterile technician",
    "patient care technician",
    "patient care assistant",
    "home health aide",
    "home care aide",
    "caregiver",
    "direct support professional",
    "dietary aide",
    "dietitian",
    "nutritionist",

    # ── Dental ───────────────────────────────────────────────────────────────
    "dentist",
    "dental hygienist",
    "dental assistant",
    "orthodontist",
    "oral surgeon",

    # ── Behavioral / Mental Health ────────────────────────────────────────────
    "licensed clinical social worker",
    "licensed professional counselor",
    "mental health counselor",
    "marriage family therapist",
    "behavior analyst",
    "applied behavior analysis",
    "board certified behavior analyst",
    "psychiatric technician",
    "psychiatric aide",

    # ── Radiology / Imaging ───────────────────────────────────────────────────
    "radiologic technologist",
    "radiology technician",
    "mri technologist",
    "ct technologist",
    "ultrasound technologist",
    "sonographer",
    "nuclear medicine technologist",

    # ── Non-tech / Hospitality / Operations ───────────────────────────────────
    "cashier",
    "store associate",
    "retail associate",
    "sales associate",
    "customer service representative",
    "call center agent",
    "receptionist",
    "front desk agent",
    "housekeeper",
    "janitor",
    "custodian",
    "warehouse associate",
    "forklift operator",
    "delivery driver",
    "truck driver",
    "cdl driver",
    "package handler",
    "food service worker",
    "cook",
    "dishwasher",
    "line cook",
    "prep cook",
    "server",
    "bartender",
    "barista",
    "security guard",
    "security officer",
    "loss prevention",

    # ── Finance / Admin (non-IT) ──────────────────────────────────────────────
    # (only add if your consultants do NOT handle these roles)
    # "accountant", "bookkeeper", "financial analyst", "loan officer",
    # — left commented because some IT staffing firms DO place finance IT (SAP FI etc.)
]


class Command(BaseCommand):
    help = (
        "Upsert hard_negative_phrases in HarvestEngineConfig.  "
        "Only adds missing phrases, never removes existing ones.  "
        "Preview by default; use --apply to write."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            default=False,
            help="Write to HarvestEngineConfig.  Without this flag, preview only.",
        )
        parser.add_argument(
            "--list",
            action="store_true",
            default=False,
            help="Just print all hard negatives that would be active after seeding.",
        )

    def handle(self, *args, **options):
        from harvest.models import HarvestEngineConfig

        cfg = HarvestEngineConfig.get()
        existing = set(cfg.hard_negative_phrases or [])
        to_add = [p for p in HARD_NEGATIVES if p not in existing]
        already_have = [p for p in HARD_NEGATIVES if p in existing]

        if options["list"]:
            all_active = sorted(existing | set(HARD_NEGATIVES))
            self.stdout.write(f"\n{len(all_active)} hard negatives after seeding:\n")
            for p in all_active:
                marker = "+" if p in to_add else " "
                self.stdout.write(f"  {marker} {p}")
            return

        label = "APPLY" if options["apply"] else "DRY-RUN"
        self.stdout.write(self.style.WARNING(f"\n[{label}] seed_hard_negatives\n"))
        self.stdout.write(f"  Currently in DB : {len(existing)}")
        self.stdout.write(f"  Already present : {len(already_have)}")
        self.stdout.write(f"  Would add       : {len(to_add)}\n")

        if to_add:
            self.stdout.write(self.style.SUCCESS("Phrases to add:"))
            for p in to_add:
                self.stdout.write(f"  + {p!r}")
        else:
            self.stdout.write(self.style.SUCCESS("All hard negatives already present — nothing to do."))
            return

        if options["apply"]:
            cfg.hard_negative_phrases = sorted(existing | set(HARD_NEGATIVES))
            cfg.save(update_fields=["hard_negative_phrases"])
            self.stdout.write(self.style.SUCCESS(
                f"\n✓ Saved.  HarvestEngineConfig now has {len(cfg.hard_negative_phrases)} hard negatives."
            ))
        else:
            self.stdout.write(self.style.NOTICE("\nRun with --apply to write to DB."))
