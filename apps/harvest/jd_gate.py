from __future__ import annotations

import re
from dataclasses import dataclass, asdict

from django.conf import settings


_WORD_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9'/+.#-]*")

REASON_MISSING_JD = "MISSING_JD"
REASON_TOO_SHORT = "JD_TOO_SHORT"
REASON_TITLE_ONLY = "TITLE_ONLY_JD"
REASON_INACTIVE = "INACTIVE_POSTING"
REASON_LOW_CLASSIFICATION = "LOW_CLASSIFICATION_CONFIDENCE"
REASON_OK = "RESUME_JD_OK"


def _clean_spaces(value: str) -> str:
    return re.sub(r"\\s+", " ", (value or "").strip())


def _word_count(value: str) -> int:
    if not value:
        return 0
    return len(_WORD_RE.findall(value))


def _normalized_text(value: str) -> str:
    txt = _clean_spaces(value).lower()
    return re.sub(r"[^a-z0-9 ]+", "", txt)


@dataclass
class ResumeJDGate:
    usable: bool
    reason_code: str
    reason_text: str
    word_count: int
    min_words: int
    min_chars: int
    min_classification_confidence: float

    def asdict(self) -> dict:
        return asdict(self)


def evaluate_raw_job_resume_gate(raw_job) -> ResumeJDGate:
    min_words = max(1, int(getattr(settings, "RESUME_JD_MIN_WORDS", 80)))
    min_chars = max(1, int(getattr(settings, "RESUME_JD_MIN_CHARS", 400)))
    min_class_conf = float(getattr(settings, "RESUME_JD_MIN_CLASSIFICATION_CONFIDENCE", 0.35))

    deferred = set(getattr(raw_job, "get_deferred_fields", lambda: set())() or set())
    desc_clean = "" if "description_clean" in deferred else (getattr(raw_job, "description_clean", "") or "")
    desc_raw = "" if "description" in deferred else (getattr(raw_job, "description", "") or "")
    desc = _clean_spaces(desc_clean or desc_raw)
    title = _clean_spaces(getattr(raw_job, "title", ""))
    is_active = bool(getattr(raw_job, "is_active", True))
    class_conf = getattr(raw_job, "classification_confidence", None)
    class_conf = float(class_conf) if class_conf is not None else 0.0

    # Prefer stored enrichment word_count to avoid recomputing on list pages.
    stored_wc = getattr(raw_job, "word_count", 0) or 0
    wc = int(stored_wc) if stored_wc else _word_count(desc)
    txt_len = len(desc)

    if not is_active:
        return ResumeJDGate(
            usable=False,
            reason_code=REASON_INACTIVE,
            reason_text="Posting is inactive/expired",
            word_count=wc,
            min_words=min_words,
            min_chars=min_chars,
            min_classification_confidence=min_class_conf,
        )

    if not desc and not stored_wc:
        return ResumeJDGate(
            usable=False,
            reason_code=REASON_MISSING_JD,
            reason_text="No job description text available",
            word_count=0,
            min_words=min_words,
            min_chars=min_chars,
            min_classification_confidence=min_class_conf,
        )

    n_desc = _normalized_text(desc)
    n_title = _normalized_text(title)
    if desc and n_title and (n_desc == n_title or n_desc in {f"{n_title} apply now", f"apply now {n_title}"}):
        return ResumeJDGate(
            usable=False,
            reason_code=REASON_TITLE_ONLY,
            reason_text="Description is only title text",
            word_count=wc,
            min_words=min_words,
            min_chars=min_chars,
            min_classification_confidence=min_class_conf,
        )

    if wc < min_words or (desc and txt_len < min_chars):
        return ResumeJDGate(
            usable=False,
            reason_code=REASON_TOO_SHORT,
            reason_text=f"JD too short ({wc} words, needs >= {min_words})",
            word_count=wc,
            min_words=min_words,
            min_chars=min_chars,
            min_classification_confidence=min_class_conf,
        )

    if class_conf < min_class_conf:
        return ResumeJDGate(
            usable=False,
            reason_code=REASON_LOW_CLASSIFICATION,
            reason_text=f"Low classification confidence ({class_conf:.2f} < {min_class_conf:.2f})",
            word_count=wc,
            min_words=min_words,
            min_chars=min_chars,
            min_classification_confidence=min_class_conf,
        )

    return ResumeJDGate(
        usable=True,
        reason_code=REASON_OK,
        reason_text="JD is resume-usable",
        word_count=wc,
        min_words=min_words,
        min_chars=min_chars,
        min_classification_confidence=min_class_conf,
    )
