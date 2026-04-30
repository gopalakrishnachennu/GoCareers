# Unified Ops + Harvest Master Plan

## 1) Product Motto (Single Source of Truth)
Build one reliable job intelligence system where:
- Harvest ingests at scale.
- Only verified, active, candidate-ready jobs enter Vet Queue.
- Ops Center is the single control plane for run state, backlog, quality, and approvals.

---

## 2) What To Keep, Merge, Remove

### Keep (high value)
- ATS/platform adapters in `apps/harvest/harvesters/*`
- Enrichment pipeline in `apps/harvest/enrichments.py`
- Gating model in `apps/jobs/gating.py`
- URL health checks in `apps/harvest/url_health.py`
- Task orchestration in `apps/harvest/tasks.py`, `apps/core/tasks.py`
- Vet queue workflow in `apps/jobs/views.py`

### Merge (reduce confusion)
- Merge operational surfaces into Ops Center tabs:
  - Raw Jobs (`apps/harvest/views.py`)
  - Jobs Pipeline (`apps/jobs/views.py`)
  - Run Monitor (`apps/harvest/views.py`)
  - Schedules (`apps/core/views.py`)
- Keep existing endpoints temporarily; route users to unified tabs.

### Deprecate/Remove (low value / duplicate)
- Duplicate KPI cards and funnel copies with conflicting numbers.
- Duplicate “run status” pages that are read-only mirrors.
- Any hardcoded limit behavior in “Sync Qualified to Vet Queue” UI actions.

---

## 3) Target Lifecycle (authoritative)
Every row must have one stage and one reason code.

`HARVESTED -> NORMALIZED -> ENRICHED -> CLASSIFIED -> QUALIFIED -> VET_REVIEW -> LIVE`

Side stages:
- `BLOCKED`
- `DUPLICATE`
- `INACTIVE`
- `EXPIRED`
- `ERROR`

Rules:
- Stage transitions are append-audit events, not silent overwrites.
- Stage + reason must be queryable and visible in UI.

---

## 4) Current Gaps (validated against code)
1. Confidence semantics still confusing in practice (category vs extraction).
2. “Sync Qualified to Vet Queue” behavior perceived as capped/partial; UX and task progress need explicit all-qualified semantics.
3. Active-link detection has edge-case misses (soft-404/ATS-specific closed pages).
4. Too many surfaces show similar counters with different filters.
5. Country visibility/filtering inconsistent across views and historical rows.
6. Human correction feedback is not fully looped into retraining/versioned governance.

---

## 5) North-Star Operating Metrics
- Auto-lane precision: `>= 99%`
- Qualified-to-vet sync completion: `100% of currently qualified rows` (async/chunked, resumable)
- Active link validation precision on sampled audit: `>= 98.5%`
- Resume-ready coverage (of active jobs): `>= 70%` target over time
- Duplicate leakage to Vet/Live: `< 0.5%`
- Ops page reconciliation drift across counters: `0`

---

## 6) Execution Plan (Jira-style backlog)

## Epic A: Canonical Stage + Reason System
Objective: one truth for pipeline stage and gating reason.

### OPS-101 (8 SP)
- Title: Canonical stage model and reason enum hardening
- Owner: Backend
- Files:
  - `apps/harvest/models.py`
  - `apps/jobs/models.py`
  - `apps/harvest/migrations/*`
  - `apps/jobs/migrations/*`
- Deliverables:
  - Ensure authoritative stage enum coverage.
  - Add/normalize reason code catalog.
  - Add `stage_updated_at` consistency and index.
- Acceptance:
  - Every raw/job row has stage + reason (or explicit `NONE`).
  - No ambiguous status combinations in DB.
- Dependencies: none

### OPS-102 (5 SP)
- Title: Stage transition audit log
- Owner: Backend
- Files:
  - `apps/core/models.py` (or dedicated audit model in harvest)
  - `apps/harvest/tasks.py`
  - `apps/jobs/views.py`
- Deliverables:
  - Append-only transition events with actor (`system`, `task_id`, or user).
- Acceptance:
  - Any stage move traceable from UI/API.
- Dependencies: OPS-101

---

## Epic B: Qualification Engine v2 (strict but explainable)
Objective: move only worthy jobs to Vet Queue.

### OPS-201 (8 SP)
- Title: Split and calibrate confidence dimensions
- Owner: Backend + Data
- Files:
  - `apps/harvest/enrichments.py`
  - `apps/jobs/gating.py`
  - `apps/harvest/models.py`
- Deliverables:
  - Distinct fields:
    - `extraction_confidence`
    - `category_confidence`
    - `overall_confidence`
  - Update gate formula to use explicit fields.
- Acceptance:
  - Confidence provenance visible in payload and UI.
- Dependencies: OPS-101

### OPS-202 (8 SP)
- Title: Hardgate contract for QUALIFIED stage
- Owner: Backend
- Files:
  - `apps/jobs/gating.py`
  - `apps/harvest/tasks.py`
  - `apps/harvest/views.py`
- Deliverables:
  - QUALIFIED requires:
    - active status pass
    - non-duplicate
    - JD quality pass
    - minimal normalization pass
    - company resolve pass
- Acceptance:
  - Non-qualifying rows never enter Vet Queue.
  - Rejection reason code always populated.
- Dependencies: OPS-201

### OPS-203 (5 SP)
- Title: Resume JD gate governance
- Owner: Backend
- Files:
  - `apps/harvest/jd_gate.py`
  - `apps/harvest/models.py`
  - `apps/harvest/views.py`
- Deliverables:
  - Configurable thresholds via Engine UI.
  - Visible pass/fail reason in list/detail.
- Acceptance:
  - “Blocked for resume” always explains exact failing check.
- Dependencies: OPS-202

---

## Epic C: Active/Inactive Detection Engine v2
Objective: high precision active job validation.

### OPS-301 (8 SP)
- Title: Multi-signal URL validator (ATS-aware)
- Owner: Backend
- Files:
  - `apps/harvest/url_health.py`
  - `apps/harvest/detectors/html_parse.py`
  - `apps/harvest/detectors/http_head.py`
  - `apps/harvest/tests_url_health.py`
- Deliverables:
  - Signals:
    - HTTP/redirect/final URL
    - soft-404 markers
    - ATS closed-page signatures
    - board-level consistency checks
  - Output fields: `activity_status`, `activity_confidence`, `activity_reason`, `activity_checked_at`
- Acceptance:
  - Known false positive/false negative fixtures pass.
- Dependencies: OPS-202

### OPS-302 (3 SP)
- Title: Revalidation scheduler policy
- Owner: Backend
- Files:
  - `apps/core/tasks.py`
  - `apps/harvest/tasks.py`
  - `apps/core/views.py`
- Deliverables:
  - ACTIVE every 7d, UNKNOWN every 24h, INACTIVE every 14d (or configurable).
- Acceptance:
  - Recheck jobs created with clear cadence in Ops Center.
- Dependencies: OPS-301

---

## Epic D: Sync Qualified to Vet Queue (all-qualified, resumable)
Objective: one click means all currently qualified jobs, no hidden cap semantics.

### OPS-401 (8 SP)
- Title: Full-scope qualified sync task contract
- Owner: Backend
- Files:
  - `apps/harvest/tasks.py`
  - `apps/harvest/views.py`
  - `apps/harvest/urls.py`
- Deliverables:
  - Endpoint starts async task over all current qualified rows.
  - Internal chunking allowed, but global scope fixed.
  - Idempotent cursor/resume by task_id.
- Acceptance:
  - Summary reports: total considered / moved / skipped by reason / failed.
- Dependencies: OPS-202

### OPS-402 (5 SP)
- Title: UI progress + reconciliation for qualified sync
- Owner: Frontend + Backend
- Files:
  - `templates/harvest/*`
  - `apps/harvest/views.py`
  - `apps/core/task_progress.py`
- Deliverables:
  - Real-time task progress bar and counters.
  - “Why not moved?” breakdown panel.
- Acceptance:
  - Operator can see exact outcome without reading logs.
- Dependencies: OPS-401

---

## Epic E: Ops Center Unification (single shell)
Objective: one place for all operational actions and visibility.

### OPS-501 (8 SP)
- Title: Unified Ops Center tab architecture
- Owner: Frontend
- Tabs:
  - Overview
  - In Progress
  - Scheduled
  - Completed
  - Workers
  - Pipelines
  - Raw Queue
  - Qualified Queue
  - Vet Queue
  - Live
- Files:
  - `apps/core/views.py`
  - `templates/core/ops_center*.html`
  - `apps/core/urls.py`
- Acceptance:
  - Existing pages linked/redirected to corresponding tabs.
- Dependencies: OPS-401

### OPS-502 (5 SP)
- Title: Counter reconciliation service
- Owner: Backend
- Files:
  - `apps/core/dashboard_metrics.py`
  - `apps/harvest/views.py`
  - `apps/jobs/views.py`
- Deliverables:
  - Shared metrics provider; no duplicate formulas.
- Acceptance:
  - Raw/Jobs/Ops counters reconcile exactly for same filters/time.
- Dependencies: OPS-501

---

## Epic F: Raw Jobs UX modernization
Objective: fast, understandable, filterable workbench for ops.

### OPS-601 (8 SP)
- Title: Clickable funnel cards with guaranteed filter mapping
- Owner: Frontend + Backend
- Files:
  - `apps/harvest/views.py`
  - `templates/harvest/raw_jobs*.html`
- Deliverables:
  - Click Fetched/Parsed/Enriched/Classified/Ready/Synced -> exact stage filter pre-applied.
- Acceptance:
  - No “filtered chip says classified but table empty due hidden filters” confusion.
- Dependencies: OPS-502

### OPS-602 (5 SP)
- Title: Active column, country column, and active filter clarity
- Owner: Frontend + Backend
- Files:
  - `apps/harvest/models.py`
  - `apps/harvest/views.py`
  - `templates/harvest/raw_jobs*.html`
- Deliverables:
  - Visible `Active` column and tri-state filter (`Active`, `Inactive`, `Unknown`).
  - `Country` persisted + shown + filterable.
- Acceptance:
  - Operator can isolate inactive/unknown quickly and audit by country.
- Dependencies: OPS-301

### OPS-603 (5 SP)
- Title: Stuck queue and quality debt tabs
- Owner: Frontend + Backend
- Deliverables:
  - Queue aging buckets
  - Missing JD, low confidence, missing salary/location, inactive risk panels
- Dependencies: OPS-601

---

## Epic G: Data quality + country backfill
Objective: ensure historical rows meet new visibility standards.

### OPS-701 (8 SP)
- Title: Country inference backfill (historical RawJob rows)
- Owner: Backend
- Files:
  - `apps/harvest/tasks.py`
  - `apps/harvest/management/commands/*`
- Deliverables:
  - Batch backfill with checkpointing.
- Acceptance:
  - >99% rows have `country` or explicit `UNKNOWN`.
- Dependencies: OPS-602

### OPS-702 (8 SP)
- Title: Re-enrichment backfill with versioning
- Owner: Backend + Data
- Files:
  - `apps/harvest/models.py`
  - `apps/harvest/tasks.py`
- Deliverables:
  - `enrichment_version` tracking.
  - Re-enrich stale rows only.
- Dependencies: OPS-201

---

## Epic H: ML + LLM fallback (controlled adoption)
Objective: improve department/category quality without destabilizing pipeline.

### OPS-801 (8 SP)
- Title: Baseline department classifier service
- Owner: Data + Backend
- Files:
  - `apps/harvest/enrichments.py`
  - new `apps/harvest/classification/*`
- Deliverables:
  - Deterministic + model hybrid output.
- Dependencies: OPS-201

### OPS-802 (5 SP)
- Title: LLM fallback for low-confidence edge cases
- Owner: Data + Backend
- Deliverables:
  - Trigger only on conflict/low confidence.
  - Strict JSON schema + cache by hash+model_version.
- Dependencies: OPS-801

### OPS-803 (5 SP)
- Title: Human feedback loop storage and retrain hook
- Owner: Backend + Data
- Deliverables:
  - Store corrected labels, retrain queue, weekly model eval report.
- Dependencies: OPS-801

---

## Epic I: Vet queue reset + controlled re-entry (one-time baseline reset)
Objective: empty legacy noisy vet queue and repopulate by new qualification policy.

### OPS-901 (3 SP)
- Title: Move all current vet pool jobs back to Raw with reset reason
- Owner: Backend
- Existing command:
  - `apps/harvest/management/commands/reset_vet_queue_to_raw.py`
- Acceptance:
  - Vet queue empty.
  - Raw rows tagged with reset provenance.

### OPS-902 (5 SP)
- Title: Recompute gates and repopulate Vet from QUALIFIED only
- Owner: Backend
- Acceptance:
  - Vet queue contains only policy-compliant rows.
- Dependencies: OPS-901 + OPS-202 + OPS-401

---

## 7) UX Principles to enforce across all pages
1. One primary action per section.
2. Every metric card is clickable and explains the exact query behind it.
3. Every blocked state has reason + suggested remediation.
4. Avoid repeated KPI panels with different scopes.
5. Keep table columns stable; allow optional column toggles.

---

## 8) Production rollout plan

### Stage 0: Safety
- Backup DB snapshot.
- Freeze destructive admin actions behind feature flag.

### Stage 1: Schema + metric unification
- Deploy migration set for stage/reason/confidence/country fields.
- Deploy shared metrics provider.

### Stage 2: Gate + sync behavior
- Deploy hardgate v2 and qualified sync task v2.
- Enable progress UI.

### Stage 3: Ops center unification
- Enable unified tabs.
- Redirect legacy pages with banner + fallback links.

### Stage 4: Backfills
- Country backfill.
- Enrichment version backfill.

### Stage 5: Classifier upgrades
- Enable ML baseline.
- Enable LLM fallback behind feature flag.

---

## 9) Risks and mitigations
- Risk: wrong hardgate thresholds reduce throughput.
  - Mitigation: start conservative and monitor acceptance by platform.
- Risk: active detector false in ATS edge pages.
  - Mitigation: platform-specific marker tests + sample audits.
- Risk: large sync jobs create queue pressure.
  - Mitigation: chunked, resumable tasks with worker concurrency caps.
- Risk: counter mismatch erodes trust.
  - Mitigation: single metrics provider and reconciliation test suite.

---

## 10) Acceptance checklist (Definition of Done)
- [ ] One control plane for operations (Ops Center tabs).
- [ ] “Sync Qualified to Vet Queue” processes all currently qualified rows with resumable progress.
- [ ] Vet queue contains only qualified jobs.
- [ ] Active/inactive status visible and filterable everywhere relevant.
- [ ] Country persisted and filterable for historical + new rows.
- [ ] Funnel cards map to real filters and show corresponding rows.
- [ ] All blocked jobs show deterministic reason code.
- [ ] Daily KPI report includes conversion and blocker reasons per platform.

---

## 11) Suggested sprint sequence (4 sprints)
- Sprint 1: OPS-101/102, OPS-201/202
- Sprint 2: OPS-301/302, OPS-401/402
- Sprint 3: OPS-501/502, OPS-601/602/603
- Sprint 4: OPS-701/702, OPS-801/802/803, OPS-901/902

---

## 12) Immediate next 72 hours
1. Ship stage/reason/counter unification patch (OPS-101, OPS-502).
2. Patch qualified sync semantics and task progress (OPS-401, OPS-402).
3. Ship active-column + filter clarity + country column visibility (OPS-602).
4. Run vet reset and controlled re-entry (OPS-901, OPS-902).

