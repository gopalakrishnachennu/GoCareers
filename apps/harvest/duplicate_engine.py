"""
Duplicate detection engine for RawJob records.

Rules (applied in priority order within each candidate group):
  EXACT           — same normalized title + company + description hash
  URL_DUPLICATE   — same url_hash (edge-cases that bypass unique constraint)
  REQUISITION     — same company + external_id (non-empty)
  STRONG_MATCH    — same company + title + description Jaccard ≥ 0.95, diff URL
  LOCATION_VARIANT— same company + title + JD ≥ 0.90 + different location
  NEAR_DUPLICATE  — same company + title Jaccard ≥ 0.80 + JD ≥ 0.90
  REPOST          — same company + title ≥ 0.85 + JD ≥ 0.85 + date gap ≥ 30d
  AGENCY_DUP      — diff company + title ≥ 0.90 + JD ≥ 0.92

Each rule emits (primary_id, duplicate_id, label, similarity, method) tuples.
Primary is chosen as the one with the higher quality_score (tie → lower pk).
"""
from __future__ import annotations

import hashlib
import logging
import re
from datetime import timedelta
from itertools import combinations
from typing import Iterator

from django.db import transaction
from django.db.models import Count, Q
from django.utils import timezone

from .models import DuplicateLabel, DuplicateResolution, RawJob, RawJobDuplicatePair

logger = logging.getLogger(__name__)

# ── text helpers ──────────────────────────────────────────────────────────────

_STOP = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "has", "have", "had", "will", "would", "could", "should", "may", "might",
    "we", "our", "your", "this", "that", "as", "it", "its", "not", "you",
    "all", "new", "job", "work", "role", "position", "opportunity",
})


def _normalize_company(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _jd_hash(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").lower().strip())
    return hashlib.sha256(cleaned.encode()).hexdigest()[:32]


def _tokenize(text: str) -> frozenset[str]:
    return frozenset(
        w for w in re.findall(r"[a-z0-9]+", (text or "").lower())
        if len(w) > 2 and w not in _STOP
    )


def _jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    return inter / (len(a) + len(b) - inter)


def _pick_primary(j1: dict, j2: dict) -> tuple[dict, dict]:
    """Return (primary, secondary) — prefer higher quality_score, then lower pk."""
    q1 = j1.get("quality_score") or 0.0
    q2 = j2.get("quality_score") or 0.0
    if q1 >= q2:
        return j1, j2
    return j2, j1


# ── core detection logic ──────────────────────────────────────────────────────

def _detect_for_company_group(
    jobs: list[dict],
) -> Iterator[tuple[int, int, str, float, str]]:
    """
    Yield (primary_id, dup_id, label, similarity, method) for all duplicate
    pairs within a single company-name group.
    """
    # Pre-compute tokens + hashes once per job
    for j in jobs:
        j["_title_tok"] = _tokenize(j["normalized_title"] or j["title"])
        j["_jd_tok"]    = _tokenize(j["description_clean"] or j["description"])
        j["_jd_hash"]   = _jd_hash(j["description_clean"] or j["description"])
        j["_loc"]       = (j["location_raw"] or "").strip().lower()

    seen: set[tuple[int, int]] = set()

    for j1, j2 in combinations(jobs, 2):
        pair_key = (min(j1["id"], j2["id"]), max(j1["id"], j2["id"]))
        if pair_key in seen:
            continue

        title_sim = _jaccard(j1["_title_tok"], j2["_title_tok"])
        jd_sim    = _jaccard(j1["_jd_tok"],    j2["_jd_tok"])

        label = method = None
        sim = 0.0

        # 1. Exact — same title tokens + same JD hash
        if (
            j1["_title_tok"] == j2["_title_tok"]
            and j1["_jd_hash"] == j2["_jd_hash"]
            and j1["_jd_hash"] != _jd_hash("")
        ):
            label, method, sim = DuplicateLabel.EXACT, "title_eq+jd_hash_eq", 1.0

        # 2. Strong Match — same company, title & JD both very high, diff URL
        elif title_sim >= 0.95 and jd_sim >= 0.95 and j1["url_hash"] != j2["url_hash"]:
            label, method, sim = (
                DuplicateLabel.STRONG_MATCH, "title_jaccard+jd_jaccard≥0.95", (title_sim + jd_sim) / 2
            )

        # 3. Location Variant — same title+JD but different location
        elif (
            title_sim >= 0.90
            and jd_sim >= 0.90
            and j1["_loc"] != j2["_loc"]
            and j1["_loc"] and j2["_loc"]
        ):
            label, method, sim = (
                DuplicateLabel.LOCATION_VARIANT, "title≥0.90+jd≥0.90+loc_diff",
                (title_sim + jd_sim) / 2,
            )

        # 4. Near Duplicate — same company, high title+JD similarity
        elif title_sim >= 0.80 and jd_sim >= 0.90:
            label, method, sim = (
                DuplicateLabel.NEAR_DUPLICATE, "title≥0.80+jd≥0.90",
                (title_sim + jd_sim) / 2,
            )

        # 5. Repost — same title+JD with ≥30-day gap
        elif title_sim >= 0.85 and jd_sim >= 0.85:
            d1 = j1.get("fetched_at")
            d2 = j2.get("fetched_at")
            if d1 and d2 and abs((d1 - d2).days) >= 30:
                label, method, sim = (
                    DuplicateLabel.REPOST, "title≥0.85+jd≥0.85+date_gap≥30d",
                    (title_sim + jd_sim) / 2,
                )

        if label:
            seen.add(pair_key)
            p, d = _pick_primary(j1, j2)
            yield p["id"], d["id"], label, round(sim, 4), method


def _detect_agency_pairs(
    all_jobs: list[dict],
) -> Iterator[tuple[int, int, str, float, str]]:
    """
    Detect Agency Duplicates: same JD + same title across DIFFERENT companies.
    Uses JD hash bucketing for O(n) pre-grouping, then title similarity check.
    """
    from collections import defaultdict

    by_jd_hash: dict[str, list[dict]] = defaultdict(list)
    for j in all_jobs:
        h = j.get("_jd_hash") or _jd_hash(j["description_clean"] or j["description"])
        if h != _jd_hash(""):
            by_jd_hash[h].append(j)

    seen: set[tuple[int, int]] = set()
    for bucket in by_jd_hash.values():
        if len(bucket) < 2:
            continue
        for j1, j2 in combinations(bucket, 2):
            # Different companies
            if _normalize_company(j1["company_name"]) == _normalize_company(j2["company_name"]):
                continue
            pair_key = (min(j1["id"], j2["id"]), max(j1["id"], j2["id"]))
            if pair_key in seen:
                continue
            title_sim = _jaccard(
                j1.get("_title_tok") or _tokenize(j1["normalized_title"] or j1["title"]),
                j2.get("_title_tok") or _tokenize(j2["normalized_title"] or j2["title"]),
            )
            if title_sim >= 0.90:
                seen.add(pair_key)
                p, d = _pick_primary(j1, j2)
                yield (
                    p["id"], d["id"],
                    DuplicateLabel.AGENCY_DUP,
                    round((1.0 + title_sim) / 2, 4),
                    "jd_hash_eq+title≥0.90+diff_company",
                )


# ── public API ────────────────────────────────────────────────────────────────

_PRIORITY = {
    DuplicateLabel.EXACT:            1,
    DuplicateLabel.URL_DUPLICATE:    2,
    DuplicateLabel.REQUISITION:      3,
    DuplicateLabel.STRONG_MATCH:     4,
    DuplicateLabel.LOCATION_VARIANT: 5,
    DuplicateLabel.NEAR_DUPLICATE:   6,
    DuplicateLabel.REPOST:           7,
    DuplicateLabel.AGENCY_DUP:       8,
}


def run_detection(
    limit: int = 5000,
    company_slug: str = "",
    skip_existing: bool = True,
    company_chunk_size: int = 50,
    sleep_between_chunks: float = 0.15,
) -> dict:
    """
    CPU-friendly duplicate detection over RawJobs.

    Processes companies in small chunks with a brief sleep between each chunk
    so the Celery worker never monopolises the CPU and the web process stays
    responsive.  Always call via the Celery task — never from a web request.

    Args:
        limit:                Max jobs to consider (ordered by quality_score DESC).
        company_slug:         Narrow to one company (for targeted re-scans).
        skip_existing:        Incremental mode — skip pairs already in DB.
        company_chunk_size:   Companies processed per micro-batch before sleeping.
        sleep_between_chunks: Seconds to sleep between micro-batches (yields CPU).
    """
    import time
    from collections import defaultdict

    qs = RawJob.objects.filter(is_active=True, has_description=True)
    if company_slug:
        qs = qs.filter(company_name__icontains=company_slug)

    fields = [
        "id", "title", "normalized_title", "company_name", "url_hash",
        "external_id", "location_raw", "quality_score",
        "fetched_at", "description_clean", "description",
    ]
    # Load only ids + lightweight fields first — description loaded per-company below
    jobs_qs = list(qs.values(*fields).order_by("-quality_score")[:limit])

    if not jobs_qs:
        return {"pairs_found": 0, "pairs_saved": 0, "pairs_skipped": 0, "companies_scanned": 0}

    # Pre-compute tokens + hashes (vectorised pass, no heavy pairwise yet)
    for j in jobs_qs:
        j["_title_tok"] = _tokenize(j["normalized_title"] or j["title"])
        j["_jd_tok"]    = _tokenize(j["description_clean"] or j["description"])
        j["_jd_hash"]   = _jd_hash(j["description_clean"] or j["description"])
        j["_loc"]       = (j["location_raw"] or "").strip().lower()

    # ── Phase 1: Requisition duplicates (pure SQL, very fast) ────────────────
    req_pairs: list[tuple] = []
    req_groups = (
        RawJob.objects.filter(is_active=True)
        .exclude(external_id="")
        .values("company_name", "external_id")
        .annotate(cnt=Count("id"))
        .filter(cnt__gt=1)
    )
    for group in req_groups:
        ids = list(
            RawJob.objects.filter(
                company_name=group["company_name"],
                external_id=group["external_id"],
                is_active=True,
            ).values_list("id", "quality_score").order_by("-quality_score")
        )
        if len(ids) >= 2:
            primary_id = ids[0][0]
            for dup_id, _ in ids[1:]:
                req_pairs.append((primary_id, dup_id, DuplicateLabel.REQUISITION, 1.0, "company+external_id"))

    # ── Phase 2: URL duplicates (rare due to unique constraint, pure SQL) ─────
    url_pairs: list[tuple] = []
    url_groups = (
        RawJob.objects.filter(is_active=True)
        .values("url_hash")
        .annotate(cnt=Count("id"))
        .filter(cnt__gt=1)
    )
    for group in url_groups:
        ids = list(
            RawJob.objects.filter(url_hash=group["url_hash"], is_active=True)
            .values_list("id", "quality_score").order_by("-quality_score")
        )
        if len(ids) >= 2:
            primary_id = ids[0][0]
            for dup_id, _ in ids[1:]:
                url_pairs.append((primary_id, dup_id, DuplicateLabel.URL_DUPLICATE, 1.0, "url_hash_eq"))

    # ── Phase 3: Company-group similarity — chunked to throttle CPU ──────────
    by_company: dict[str, list[dict]] = defaultdict(list)
    for j in jobs_qs:
        by_company[_normalize_company(j["company_name"])].append(j)

    company_pairs: list[tuple] = []
    multi_job_companies = [
        (slug, grp) for slug, grp in by_company.items() if len(grp) >= 2
    ]
    for chunk_start in range(0, len(multi_job_companies), company_chunk_size):
        chunk = multi_job_companies[chunk_start: chunk_start + company_chunk_size]
        for _, group_jobs in chunk:
            for row in _detect_for_company_group(group_jobs):
                company_pairs.append(row)
        # Yield CPU between chunks — keeps nginx + gunicorn alive
        time.sleep(sleep_between_chunks)

    # ── Phase 4: Agency duplicates — chunked by JD hash bucket ───────────────
    from collections import defaultdict as _dd
    by_jd_hash: dict[str, list[dict]] = _dd(list)
    empty_hash = _jd_hash("")
    for j in jobs_qs:
        h = j["_jd_hash"]
        if h != empty_hash:
            by_jd_hash[h].append(j)

    agency_pairs: list[tuple] = []
    buckets = [b for b in by_jd_hash.values() if len(b) >= 2]
    for chunk_start in range(0, len(buckets), company_chunk_size):
        chunk_buckets = buckets[chunk_start: chunk_start + company_chunk_size]
        seen_agency: set[tuple[int, int]] = set()
        for bucket in chunk_buckets:
            for j1, j2 in combinations(bucket, 2):
                if _normalize_company(j1["company_name"]) == _normalize_company(j2["company_name"]):
                    continue
                pair_key = (min(j1["id"], j2["id"]), max(j1["id"], j2["id"]))
                if pair_key in seen_agency:
                    continue
                title_sim = _jaccard(j1["_title_tok"], j2["_title_tok"])
                if title_sim >= 0.90:
                    seen_agency.add(pair_key)
                    p, d = _pick_primary(j1, j2)
                    agency_pairs.append((
                        p["id"], d["id"],
                        DuplicateLabel.AGENCY_DUP,
                        round((1.0 + title_sim) / 2, 4),
                        "jd_hash_eq+title≥0.90+diff_company",
                    ))
        time.sleep(sleep_between_chunks)

    # ── Merge all candidates, keep highest-priority label per pair ────────────
    all_pairs: dict[tuple[int, int], tuple] = {}
    for row in req_pairs + url_pairs + company_pairs + agency_pairs:
        p_id, d_id, label, sim, method = row
        key = (min(p_id, d_id), max(p_id, d_id))
        existing = all_pairs.get(key)
        if not existing or _PRIORITY.get(label, 99) < _PRIORITY.get(existing[2], 99):
            all_pairs[key] = row

    # ── Persist in small batches ──────────────────────────────────────────────
    pairs_found  = len(all_pairs)
    pairs_saved  = 0
    pairs_skipped = 0

    if skip_existing:
        existing_keys = set(
            RawJobDuplicatePair.objects.values_list("primary_id", "duplicate_id")
        )
    else:
        existing_keys = set()

    to_create: list[RawJobDuplicatePair] = []
    for (k1, k2), (p_id, d_id, label, sim, method) in all_pairs.items():
        if (p_id, d_id) in existing_keys or (d_id, p_id) in existing_keys:
            pairs_skipped += 1
            continue
        to_create.append(
            RawJobDuplicatePair(
                primary_id=p_id,
                duplicate_id=d_id,
                label=label,
                similarity=sim,
                method=method,
                resolution=DuplicateResolution.PENDING,
            )
        )

    # Write in chunks of 500 with a sleep to avoid long DB locks
    WRITE_CHUNK = 500
    for i in range(0, len(to_create), WRITE_CHUNK):
        chunk = to_create[i: i + WRITE_CHUNK]
        with transaction.atomic():
            RawJobDuplicatePair.objects.bulk_create(chunk, ignore_conflicts=True)
        pairs_saved += len(chunk)
        time.sleep(0.05)

    logger.info(
        "Duplicate detection complete: %d found, %d saved, %d skipped, %d companies",
        pairs_found, pairs_saved, pairs_skipped, len(by_company),
    )
    return {
        "pairs_found":       pairs_found,
        "pairs_saved":       pairs_saved,
        "pairs_skipped":     pairs_skipped,
        "companies_scanned": len(by_company),
    }


def merge_pair(pair: "RawJobDuplicatePair", resolved_by=None) -> dict:
    """
    Merge duplicate into primary:
    - Copy any blank fields from duplicate to primary (backfill only, don't overwrite)
    - Set duplicate.is_active = False
    - Mark pair as MERGED
    Returns dict with action summary.
    """
    primary   = pair.primary
    duplicate = pair.duplicate

    BACKFILL_FIELDS = [
        "description", "description_clean", "requirements", "benefits",
        "salary_raw", "salary_min", "salary_max",
        "posted_date", "closing_date", "department",
        "skills", "tech_stack", "years_required", "education_required",
    ]
    filled: list[str] = []
    for field in BACKFILL_FIELDS:
        pval = getattr(primary, field, None)
        dval = getattr(duplicate, field, None)
        empty = pval in (None, "", [], {})
        if empty and dval not in (None, "", [], {}):
            setattr(primary, field, dval)
            filled.append(field)

    with transaction.atomic():
        if filled:
            primary.save(update_fields=filled)
        duplicate.is_active = False
        duplicate.save(update_fields=["is_active"])
        pair.resolution  = DuplicateResolution.MERGED
        pair.resolved_at = timezone.now()
        pair.resolved_by = resolved_by
        pair.save(update_fields=["resolution", "resolved_at", "resolved_by"])

    return {"merged": True, "backfilled_fields": filled}


def dismiss_pair(pair: "RawJobDuplicatePair", resolved_by=None, notes: str = "") -> None:
    """Mark pair as Keep Both (dismissed)."""
    with transaction.atomic():
        pair.resolution  = DuplicateResolution.DISMISSED
        pair.resolved_at = timezone.now()
        pair.resolved_by = resolved_by
        pair.notes       = notes
        pair.save(update_fields=["resolution", "resolved_at", "resolved_by", "notes"])


def confirm_pair(pair: "RawJobDuplicatePair", resolved_by=None) -> None:
    """Mark pair as confirmed duplicate (no merge, just flagged)."""
    with transaction.atomic():
        pair.resolution  = DuplicateResolution.CONFIRMED
        pair.resolved_at = timezone.now()
        pair.resolved_by = resolved_by
        pair.save(update_fields=["resolution", "resolved_at", "resolved_by"])
