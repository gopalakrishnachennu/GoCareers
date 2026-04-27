"""Phase 4: cross-company dedup via Job.url_hash.

Prevents duplicate Job rows when the same posting is discovered through
multiple ATS detections (e.g. SmartRecruiters + a company's career page both
pointing at the same URL).
"""
from __future__ import annotations

from typing import Optional

from harvest.normalizer import compute_url_hash

from .models import Job


def url_hash_for(url: str) -> str:
    return compute_url_hash(url)


def find_existing_job_by_url(url: str) -> Optional[Job]:
    """Return an existing active Job with matching url_hash, or None."""
    h = url_hash_for(url)
    if not h:
        return None
    return Job.objects.filter(url_hash=h, is_archived=False).order_by('created_at').first()
