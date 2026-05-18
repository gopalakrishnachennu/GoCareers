#!/usr/bin/env python3
"""
GoCareers MCP Server
====================
Exposes the GoCareers harvest pipeline as tools to Claude Desktop,
Claude Code, Cursor, Cline, and any other MCP-compatible AI client.

Transport: stdio (default) — launched as a subprocess by the MCP client.

Usage
-----
  # Direct
  python mcp_server.py

  # Via uv (recommended — no virtualenv activation needed)
  uv run mcp_server.py

  # In Docker (local-harvester stack)
  docker compose -f docker-compose.local-harvester.yml run --rm harvester python mcp_server.py

See docs/MCP.md for full setup and client configuration guide.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

# ── Django bootstrap ──────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "apps"))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402
django.setup()

# ── MCP imports (after django.setup so settings are live) ─────────────────────
from mcp.server import Server  # noqa: E402
from mcp.server.stdio import stdio_server  # noqa: E402
import mcp.types as types  # noqa: E402

from django.conf import settings  # noqa: E402

# ── Helpers ───────────────────────────────────────────────────────────────────

MAX_ROWS: int = getattr(settings, "MCP_MAX_ROWS", 100)
ALLOWED_ACTIONS: set[str] = set(
    a.strip().lower()
    for a in getattr(settings, "MCP_ALLOWED_ACTIONS", "read,write").split(",")
    if a.strip()
)
WRITE_ENABLED = "write" in ALLOWED_ACTIONS


def _ok(data: Any) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=json.dumps(data, indent=2, default=str))]


def _err(msg: str) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=json.dumps({"error": msg}, indent=2))]


# ── Server definition ─────────────────────────────────────────────────────────

server = Server("gocareers")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    tools = [
        # ── READ tools ──────────────────────────────────────────────────────
        types.Tool(
            name="get_pipeline_stats",
            description=(
                "Returns a live snapshot of the GoCareers harvest pipeline: "
                "RawJob counts by sync/scope status, Job pool counts, "
                "and the last 5 ops run results."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="get_recent_ops_runs",
            description="Returns recent HarvestOpsRun records (sync/harvest operations) with status and result summary.",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Number of runs to return (default 10, max 50)",
                        "default": 10,
                    },
                    "operation": {
                        "type": "string",
                        "description": "Filter by operation name (e.g. 'sync_pool', 'fetch_batch'). Omit for all.",
                    },
                },
            },
        ),
        types.Tool(
            name="get_rawjobs",
            description="Query RawJobs from the harvest pipeline by sync or scope status.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sync_status": {
                        "type": "string",
                        "description": "Filter by sync status: PENDING, SYNCED, FAILED, SKIPPED, DUPLICATE",
                    },
                    "scope_status": {
                        "type": "string",
                        "description": "Filter by scope status: UNSCOPED, PRIORITY_TARGET, REVIEW_UNKNOWN_COUNTRY, COLD_NON_TARGET_COUNTRY, COLD_NO_LOCATION",
                    },
                    "company_name": {
                        "type": "string",
                        "description": "Filter by company name (partial match).",
                    },
                    "limit": {
                        "type": "integer",
                        "description": f"Max rows to return (default 20, max {MAX_ROWS})",
                        "default": 20,
                    },
                },
            },
        ),
        types.Tool(
            name="get_company",
            description="Get details for a company including its harvest platforms and recent RawJob stats.",
            inputSchema={
                "type": "object",
                "properties": {
                    "company_id": {"type": "integer", "description": "Company primary key"},
                    "name": {"type": "string", "description": "Company name (partial match, used if company_id not given)"},
                },
            },
        ),
        types.Tool(
            name="search_jobs",
            description="Search the vetted Job pool by title, company, or location.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search term (title, company, or location)"},
                    "status": {"type": "string", "description": "Filter by job status (POOL, ACTIVE, ARCHIVED). Default: POOL"},
                    "country": {"type": "string", "description": "Filter by country code or name"},
                    "limit": {"type": "integer", "description": "Max results (default 20)", "default": 20},
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="get_unknown_country_jobs",
            description="Returns RawJobs pending unknown-country review (scope_status=REVIEW_UNKNOWN_COUNTRY).",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max rows (default 20)", "default": 20},
                },
            },
        ),
        types.Tool(
            name="get_ops_run_detail",
            description="Get full audit payload for a specific HarvestOpsRun by ID.",
            inputSchema={
                "type": "object",
                "properties": {
                    "ops_run_id": {"type": "integer", "description": "HarvestOpsRun primary key"},
                },
                "required": ["ops_run_id"],
            },
        ),
        types.Tool(
            name="explain_rawjob",
            description=(
                "Returns a detailed breakdown of a single RawJob: "
                "title, company, location, scope/sync status, gate result, "
                "and why it passed or failed vetting."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "rawjob_id": {"type": "integer", "description": "RawJob primary key"},
                },
                "required": ["rawjob_id"],
            },
        ),
    ]

    if WRITE_ENABLED:
        tools += [
            # ── WRITE tools ─────────────────────────────────────────────────
            types.Tool(
                name="trigger_sync",
                description=(
                    "Fires sync_harvested_to_pool_task synchronously and returns the result. "
                    "Syncs qualified RawJobs into the Job vetting pool."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "qualified_only": {
                            "type": "boolean",
                            "description": "Only sync PRIORITY_TARGET scope jobs (default true)",
                            "default": True,
                        },
                        "max_jobs": {
                            "type": "integer",
                            "description": "Cap on jobs to process. 0 = no limit (default 0)",
                            "default": 0,
                        },
                    },
                },
            ),
            types.Tool(
                name="approve_unknown_country",
                description="Set the country on a REVIEW_UNKNOWN_COUNTRY RawJob and re-scope it to PRIORITY_TARGET.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "rawjob_id": {"type": "integer", "description": "RawJob primary key"},
                        "country": {"type": "string", "description": "Two-letter ISO country code (e.g. 'US', 'DE', 'GB')"},
                    },
                    "required": ["rawjob_id", "country"],
                },
            ),
            types.Tool(
                name="reindex_rawjob_table",
                description=(
                    "Runs REINDEX TABLE CONCURRENTLY on harvest_rawjob to fix corrupt B-tree indexes. "
                    "Safe to run while the app is live. Takes 30-120 seconds on large tables."
                ),
                inputSchema={"type": "object", "properties": {}},
            ),
            # ── ORCHESTRATION: Classification & Enrichment ──────────────────
            types.Tool(
                name="trigger_classify",
                description=(
                    "Fires the classify_jobs_task via Celery (async). Classifies unclassified "
                    "RawJobs with taxonomy, domain, and marketing roles. Returns the Celery "
                    "task ID — use get_task_progress to monitor. Safe for 10k+ jobs."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "force_reclassify": {
                            "type": "boolean",
                            "description": "Re-classify already classified jobs (default false)",
                            "default": False,
                        },
                    },
                },
            ),
            types.Tool(
                name="trigger_scope_evaluation",
                description=(
                    "Fires evaluate_rawjob_scope management command via Celery to scope "
                    "UNSCOPED RawJobs (classify them as PRIORITY_TARGET, COLD_NON_TARGET_COUNTRY, etc). "
                    "Returns task ID for monitoring."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "batch_size": {
                            "type": "integer",
                            "description": "Jobs per batch (default 1000)",
                            "default": 1000,
                        },
                        "only_unscoped": {
                            "type": "boolean",
                            "description": "Only process UNSCOPED jobs (default true)",
                            "default": True,
                        },
                    },
                },
            ),
            types.Tool(
                name="trigger_domain_classification",
                description=(
                    "Fires classify_job_domains management command via Celery. "
                    "Assigns job domains (Engineering, Marketing, etc.) to RawJobs. "
                    "Returns task ID for monitoring."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "batch_size": {
                            "type": "integer",
                            "description": "Jobs per batch (default 1000)",
                            "default": 1000,
                        },
                    },
                },
            ),
            types.Tool(
                name="trigger_enrichment",
                description=(
                    "Fires enrich_existing_jobs_task via Celery. Backfills category_confidence "
                    "and other enrichment fields on existing jobs. Returns task ID."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "batch_size": {
                            "type": "integer",
                            "description": "Jobs per batch (default 500)",
                            "default": 500,
                        },
                    },
                },
            ),
            types.Tool(
                name="get_task_progress",
                description=(
                    "Check the progress of a running Celery task by task ID. "
                    "Returns state (PENDING/STARTED/PROGRESS/SUCCESS/FAILURE) and "
                    "progress info (current/total/message) when available."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "task_id": {"type": "string", "description": "Celery task ID returned by a trigger_* tool"},
                    },
                    "required": ["task_id"],
                },
            ),
            types.Tool(
                name="cancel_task",
                description="Revoke (cancel) a running Celery task by ID.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "task_id": {"type": "string", "description": "Celery task ID to cancel"},
                    },
                    "required": ["task_id"],
                },
            ),
            # ── ORCHESTRATION: Resume Generation ────────────────────────────
            types.Tool(
                name="auto_generate_resumes",
                description=(
                    "Auto-generate resumes for ALL consultants × matched jobs. "
                    "Matches jobs to consultants by marketing role overlap, then generates "
                    "a tailored resume for each pair. Runs via Celery (safe for 100+ pairs). "
                    "Returns task ID — use get_task_progress to monitor."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "job_ids": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "Specific Job PKs to process. Omit for all unprocessed POOL jobs.",
                        },
                        "max_jobs": {
                            "type": "integer",
                            "description": "Cap on jobs to process (0 = no limit, default 0)",
                            "default": 0,
                        },
                        "dry_run": {
                            "type": "boolean",
                            "description": "Find matches but don't actually generate (default false)",
                            "default": False,
                        },
                    },
                },
            ),
            types.Tool(
                name="generate_for_consultant",
                description=(
                    "Generate resumes for a single consultant against all their matched "
                    "POOL jobs (by marketing role overlap). Runs via Celery. Returns task ID."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "consultant_id": {"type": "integer", "description": "ConsultantProfile primary key"},
                        "max_jobs": {
                            "type": "integer",
                            "description": "Cap on jobs (0 = no limit)",
                            "default": 0,
                        },
                        "dry_run": {
                            "type": "boolean",
                            "description": "Preview matches without generating",
                            "default": False,
                        },
                    },
                    "required": ["consultant_id"],
                },
            ),
            types.Tool(
                name="generate_resume",
                description=(
                    "Generate a single tailored resume for one consultant × one job. "
                    "Runs the LLM inline (~10-30s). Returns draft ID and ATS score."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "consultant_id": {"type": "integer", "description": "ConsultantProfile primary key"},
                        "job_id": {"type": "integer", "description": "Job primary key"},
                    },
                    "required": ["consultant_id", "job_id"],
                },
            ),
            types.Tool(
                name="get_resume_draft",
                description="Retrieve an existing ResumeDraft by ID, including generated content and ATS score.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "draft_id": {"type": "integer", "description": "ResumeDraft primary key"},
                    },
                    "required": ["draft_id"],
                },
            ),
            types.Tool(
                name="list_resume_drafts",
                description="List resume drafts for a consultant, optionally filtered by job.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "consultant_id": {"type": "integer", "description": "ConsultantProfile primary key"},
                        "job_id": {"type": "integer", "description": "Filter by job (optional)"},
                        "limit": {"type": "integer", "description": "Max results (default 20)", "default": 20},
                    },
                    "required": ["consultant_id"],
                },
            ),
        ]

    return tools


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    try:
        return await _dispatch(name, arguments)
    except Exception as exc:
        return _err(f"Tool '{name}' raised {type(exc).__name__}: {exc}")


async def _dispatch(name: str, args: dict) -> list[types.TextContent]:  # noqa: C901

    # ── get_pipeline_stats ────────────────────────────────────────────────────
    if name == "get_pipeline_stats":
        from harvest.models import RawJob, HarvestOpsRun
        from jobs.models import Job

        rj_sync = {
            s.value: RawJob.objects.filter(sync_status=s).count()
            for s in RawJob.SyncStatus
        }
        rj_scope = {
            s.value: RawJob.objects.filter(scope_status=s).count()
            for s in RawJob.ScopeStatus
        }
        job_counts = {
            "total": Job.objects.count(),
            "in_pool": Job.objects.filter(status="POOL").count(),
            "active": Job.objects.filter(status="ACTIVE").count(),
        }
        recent_runs = list(
            HarvestOpsRun.objects.order_by("-created_at")
            .values("id", "operation", "status", "created_at", "finished_at")[:5]
        )
        return _ok({
            "rawjobs": {"by_sync_status": rj_sync, "by_scope_status": rj_scope},
            "jobs": job_counts,
            "recent_ops_runs": recent_runs,
        })

    # ── get_recent_ops_runs ───────────────────────────────────────────────────
    elif name == "get_recent_ops_runs":
        from harvest.models import HarvestOpsRun

        limit = min(int(args.get("limit", 10)), 50)
        qs = HarvestOpsRun.objects.order_by("-created_at")
        if op := args.get("operation"):
            qs = qs.filter(operation__icontains=op)
        runs = list(qs.values(
            "id", "operation", "status", "created_at", "finished_at", "audit_payload"
        )[:limit])
        return _ok(runs)

    # ── get_rawjobs ───────────────────────────────────────────────────────────
    elif name == "get_rawjobs":
        from harvest.models import RawJob

        limit = min(int(args.get("limit", 20)), MAX_ROWS)
        qs = RawJob.objects.select_related("company").order_by("-created_at")
        if s := args.get("sync_status"):
            qs = qs.filter(sync_status=s.upper())
        if s := args.get("scope_status"):
            qs = qs.filter(scope_status=s.upper())
        if c := args.get("company_name"):
            qs = qs.filter(company__name__icontains=c)

        rows = []
        for rj in qs[:limit]:
            rows.append({
                "id": rj.pk,
                "title": rj.title,
                "company": rj.company.name if rj.company else None,
                "location": rj.location_raw,
                "country": rj.country,
                "sync_status": rj.sync_status,
                "scope_status": rj.scope_status,
                "pipeline_stage": rj.pipeline_stage_label,
                "created_at": rj.created_at,
                "original_url": rj.original_url,
            })
        return _ok({"count": len(rows), "results": rows})

    # ── get_company ───────────────────────────────────────────────────────────
    elif name == "get_company":
        from companies.models import Company
        from harvest.models import RawJob

        if cid := args.get("company_id"):
            try:
                company = Company.objects.get(pk=cid)
            except Company.DoesNotExist:
                return _err(f"Company {cid} not found")
        elif name_q := args.get("name"):
            company = Company.objects.filter(name__icontains=name_q).first()
            if not company:
                return _err(f"No company matching '{name_q}'")
        else:
            return _err("Provide company_id or name")

        rj_stats = {
            s.value: RawJob.objects.filter(company=company, sync_status=s).count()
            for s in RawJob.SyncStatus
        }
        platforms = list(
            company.platform_labels.select_related("platform")
            .values("platform__name", "platform__slug", "is_active")
        )
        return _ok({
            "id": company.pk,
            "name": company.name,
            "domain": company.domain,
            "country": company.country,
            "rawjob_sync_stats": rj_stats,
            "total_rawjobs": RawJob.objects.filter(company=company).count(),
            "platforms": platforms,
        })

    # ── search_jobs ───────────────────────────────────────────────────────────
    elif name == "search_jobs":
        from django.db.models import Q
        from jobs.models import Job

        q = args.get("query", "").strip()
        if not q:
            return _err("'query' is required")
        limit = min(int(args.get("limit", 20)), MAX_ROWS)
        status = args.get("status", "POOL").upper()

        qs = Job.objects.filter(
            Q(title__icontains=q) | Q(company__icontains=q) | Q(location__icontains=q),
            status=status,
            is_archived=False,
        ).order_by("-created_at")
        if country := args.get("country"):
            qs = qs.filter(country__icontains=country)

        rows = list(qs.values(
            "id", "title", "company", "location", "country",
            "job_type", "gate_status", "vet_lane", "created_at", "original_link"
        )[:limit])
        return _ok({"count": len(rows), "results": rows})

    # ── get_unknown_country_jobs ──────────────────────────────────────────────
    elif name == "get_unknown_country_jobs":
        from harvest.models import RawJob

        limit = min(int(args.get("limit", 20)), MAX_ROWS)
        qs = (
            RawJob.objects
            .filter(scope_status=RawJob.ScopeStatus.REVIEW_UNKNOWN_COUNTRY)
            .select_related("company")
            .order_by("-created_at")[:limit]
        )
        rows = [{
            "id": rj.pk,
            "title": rj.title,
            "company": rj.company.name if rj.company else None,
            "location_raw": rj.location_raw,
            "country_codes": rj.country_codes,
            "original_url": rj.original_url,
            "created_at": rj.created_at,
        } for rj in qs]
        return _ok({"count": len(rows), "results": rows})

    # ── get_ops_run_detail ────────────────────────────────────────────────────
    elif name == "get_ops_run_detail":
        from harvest.models import HarvestOpsRun

        try:
            run = HarvestOpsRun.objects.get(pk=args["ops_run_id"])
        except HarvestOpsRun.DoesNotExist:
            return _err(f"OpsRun {args['ops_run_id']} not found")
        return _ok({
            "id": run.pk,
            "operation": run.operation,
            "status": run.status,
            "created_at": run.created_at,
            "finished_at": run.finished_at,
            "audit_payload": run.audit_payload,
        })

    # ── explain_rawjob ────────────────────────────────────────────────────────
    elif name == "explain_rawjob":
        from harvest.models import RawJob

        try:
            rj = RawJob.objects.select_related("company", "job_platform").get(pk=args["rawjob_id"])
        except RawJob.DoesNotExist:
            return _err(f"RawJob {args['rawjob_id']} not found")

        payload = rj.raw_payload or {}
        gate_info = payload.get("vet_gate", {})
        return _ok({
            "id": rj.pk,
            "title": rj.title,
            "company": rj.company.name if rj.company else None,
            "platform": rj.job_platform.name if rj.job_platform else None,
            "location_raw": rj.location_raw,
            "country": rj.country,
            "country_codes": rj.country_codes,
            "sync_status": rj.sync_status,
            "sync_skip_reason": rj.sync_skip_reason,
            "scope_status": rj.scope_status,
            "pipeline_stage": rj.pipeline_stage_label,
            "employment_type": rj.employment_type,
            "has_description": rj.has_description,
            "original_url": rj.original_url,
            "gate_result": gate_info,
            "created_at": rj.created_at,
            "fetched_at": rj.fetched_at,
        })

    # ── trigger_sync (WRITE) ──────────────────────────────────────────────────
    elif name == "trigger_sync":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        from harvest.tasks import sync_harvested_to_pool_task

        qualified_only = bool(args.get("qualified_only", True))
        max_jobs = int(args.get("max_jobs", 0))
        result = sync_harvested_to_pool_task.apply(
            kwargs={"max_jobs": max_jobs, "chunk_size": 500, "qualified_only": qualified_only}
        )
        return _ok({"state": result.state, "result": result.result})

    # ── approve_unknown_country (WRITE) ───────────────────────────────────────
    elif name == "approve_unknown_country":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        from harvest.models import RawJob

        rawjob_id = args.get("rawjob_id")
        country = (args.get("country") or "").strip().upper()
        if not country or len(country) != 2:
            return _err("'country' must be a 2-letter ISO code (e.g. 'US', 'GB')")

        try:
            rj = RawJob.objects.get(pk=rawjob_id)
        except RawJob.DoesNotExist:
            return _err(f"RawJob {rawjob_id} not found")

        rj.country = country
        rj.scope_status = RawJob.ScopeStatus.PRIORITY_TARGET
        rj.save(update_fields=["country", "scope_status", "updated_at"])
        return _ok({
            "ok": True,
            "rawjob_id": rj.pk,
            "country_set": country,
            "new_scope_status": rj.scope_status,
        })

    # ── reindex_rawjob_table (WRITE) ──────────────────────────────────────────
    elif name == "reindex_rawjob_table":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute("REINDEX TABLE CONCURRENTLY harvest_rawjob;")
        return _ok({"ok": True, "message": "REINDEX TABLE CONCURRENTLY harvest_rawjob completed"})

    # ── trigger_classify (WRITE — async via Celery) ───────────────────────────
    elif name == "trigger_classify":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        from jobs.tasks import classify_jobs_task

        force = bool(args.get("force_reclassify", False))
        result = classify_jobs_task.apply_async(kwargs={"force_reclassify": force})
        return _ok({
            "ok": True,
            "task_id": result.id,
            "message": f"classify_jobs_task queued (force_reclassify={force}). Use get_task_progress('{result.id}') to monitor.",
        })

    # ── trigger_scope_evaluation (WRITE — async via Celery) ───────────────────
    elif name == "trigger_scope_evaluation":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        from celery import current_app

        batch_size = int(args.get("batch_size", 1000))
        only_unscoped = bool(args.get("only_unscoped", True))
        # Run the management command via Celery by calling it in a helper task
        cmd_args = ["evaluate_rawjob_scope", "--batch-size", str(batch_size)]
        if only_unscoped:
            cmd_args.append("--only-unscoped")
        result = current_app.send_task(
            "celery.execute_management_command",
            args=cmd_args,
        )
        # Fallback: run via subprocess if no dedicated task exists
        import subprocess
        proc = subprocess.Popen(
            ["python", "manage.py"] + cmd_args[0:],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        # Don't block — return immediately
        return _ok({
            "ok": True,
            "pid": proc.pid,
            "command": " ".join(["python", "manage.py"] + cmd_args),
            "message": f"evaluate_rawjob_scope started (PID {proc.pid}). batch_size={batch_size}, only_unscoped={only_unscoped}",
        })

    # ── trigger_domain_classification (WRITE — async subprocess) ──────────────
    elif name == "trigger_domain_classification":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        import subprocess

        batch_size = int(args.get("batch_size", 1000))
        cmd = ["python", "manage.py", "classify_job_domains", "--batch-size", str(batch_size)]
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        return _ok({
            "ok": True,
            "pid": proc.pid,
            "command": " ".join(cmd),
            "message": f"classify_job_domains started (PID {proc.pid}). batch_size={batch_size}",
        })

    # ── trigger_enrichment (WRITE — async via Celery) ─────────────────────────
    elif name == "trigger_enrichment":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        from harvest.tasks import enrich_existing_jobs_task

        batch_size = int(args.get("batch_size", 500))
        result = enrich_existing_jobs_task.apply_async(kwargs={"batch_size": batch_size})
        return _ok({
            "ok": True,
            "task_id": result.id,
            "message": f"enrich_existing_jobs_task queued (batch_size={batch_size}). Use get_task_progress('{result.id}') to monitor.",
        })

    # ── get_task_progress ─────────────────────────────────────────────────────
    elif name == "get_task_progress":
        from celery.result import AsyncResult

        task_id = args.get("task_id", "").strip()
        if not task_id:
            return _err("'task_id' is required")

        result = AsyncResult(task_id)
        info = {
            "task_id": task_id,
            "state": result.state,
        }
        if result.state == "PROGRESS":
            info["progress"] = result.info  # {"current": N, "total": M, "message": "..."}
        elif result.state == "SUCCESS":
            info["result"] = result.result
        elif result.state == "FAILURE":
            info["error"] = str(result.result)
            info["traceback"] = result.traceback
        return _ok(info)

    # ── cancel_task ───────────────────────────────────────────────────────────
    elif name == "cancel_task":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        from celery import current_app

        task_id = args.get("task_id", "").strip()
        if not task_id:
            return _err("'task_id' is required")
        current_app.control.revoke(task_id, terminate=True)
        return _ok({"ok": True, "task_id": task_id, "message": "Task revoked"})

    # ── auto_generate_resumes (WRITE — Celery async) ────────────────────────────
    elif name == "auto_generate_resumes":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        from resumes.tasks import auto_generate_for_new_jobs_task

        kwargs = {
            "max_jobs": int(args.get("max_jobs", 0)),
            "dry_run": bool(args.get("dry_run", False)),
        }
        if job_ids := args.get("job_ids"):
            kwargs["job_ids"] = job_ids

        result = auto_generate_for_new_jobs_task.apply_async(kwargs=kwargs)
        return _ok({
            "ok": True,
            "task_id": result.id,
            "message": (
                f"auto_generate_for_new_jobs queued"
                f" (max_jobs={kwargs['max_jobs']}, dry_run={kwargs['dry_run']}). "
                f"Use get_task_progress('{result.id}') to monitor."
            ),
        })

    # ── generate_for_consultant (WRITE — Celery async) ────────────────────────
    elif name == "generate_for_consultant":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        from resumes.tasks import generate_for_consultant_task

        consultant_id = args.get("consultant_id")
        if not consultant_id:
            return _err("'consultant_id' is required")

        result = generate_for_consultant_task.apply_async(kwargs={
            "consultant_id": int(consultant_id),
            "max_jobs": int(args.get("max_jobs", 0)),
            "dry_run": bool(args.get("dry_run", False)),
        })
        return _ok({
            "ok": True,
            "task_id": result.id,
            "message": f"generate_for_consultant queued (consultant_id={consultant_id}). Use get_task_progress('{result.id}') to monitor.",
        })

    # ── generate_resume (WRITE — runs LLM inline, ~10-30s per resume) ─────────
    elif name == "generate_resume":
        if not WRITE_ENABLED:
            return _err("Write actions are disabled (MCP_ALLOWED_ACTIONS=read)")
        from resumes.engine import generate_resume as _gen_resume
        from resumes.services import validate_resume, score_ats
        from resumes.models import ResumeDraft
        from jobs.models import Job
        from users.models import ConsultantProfile

        consultant_id = args.get("consultant_id")
        job_id = args.get("job_id")

        try:
            consultant = ConsultantProfile.objects.get(pk=consultant_id)
        except ConsultantProfile.DoesNotExist:
            return _err(f"ConsultantProfile {consultant_id} not found")
        try:
            job = Job.objects.get(pk=job_id)
        except Job.DoesNotExist:
            return _err(f"Job {job_id} not found")

        # Check if draft already exists
        existing = ResumeDraft.objects.filter(consultant=consultant, job=job).order_by("-version").first()
        if existing and existing.status in (ResumeDraft.Status.DRAFT, ResumeDraft.Status.FINAL):
            return _ok({
                "draft_id": existing.pk,
                "status": existing.status,
                "ats_score": existing.ats_score,
                "content_preview": (existing.content or "")[:500],
                "message": "Draft already exists. Use get_resume_draft for full content.",
            })

        # Generate
        content, tokens, error, metadata = _gen_resume(job=job, consultant=consultant)
        if error:
            draft = ResumeDraft.objects.create(
                consultant=consultant, job=job, version=1,
                status=ResumeDraft.Status.ERROR,
                error_message=(error or "")[:500],
            )
            return _err(f"Resume generation failed: {error}")

        errors_list, warnings_list = validate_resume(content or "")
        ats = score_ats(job.description or "", content or "")
        draft = ResumeDraft.objects.create(
            consultant=consultant, job=job, version=1,
            status=ResumeDraft.Status.REVIEW if errors_list else ResumeDraft.Status.DRAFT,
            content=content or "",
            tokens_used=tokens,
            ats_score=ats,
            validation_errors=errors_list,
            validation_warnings=warnings_list,
            llm_system_prompt=metadata.get("system_prompt", ""),
            llm_user_prompt=metadata.get("user_prompt", ""),
            llm_input_summary=metadata.get("input_sections", {}),
        )
        return _ok({
            "draft_id": draft.pk,
            "status": draft.status,
            "ats_score": ats,
            "tokens_used": tokens,
            "content_preview": (draft.content or "")[:500],
            "message": "Resume generated. Use get_resume_draft for full content.",
        })

    # ── get_resume_draft ──────────────────────────────────────────────────────
    elif name == "get_resume_draft":
        from resumes.models import ResumeDraft

        draft_id = args.get("draft_id")
        try:
            draft = ResumeDraft.objects.select_related("consultant", "job").get(pk=draft_id)
        except ResumeDraft.DoesNotExist:
            return _err(f"ResumeDraft {draft_id} not found")

        return _ok({
            "id": draft.pk,
            "consultant": str(draft.consultant),
            "job_title": draft.job.title if draft.job else None,
            "job_company": draft.job.company if draft.job else None,
            "version": draft.version,
            "status": draft.status,
            "ats_score": draft.ats_score,
            "tokens_used": draft.tokens_used,
            "validation_errors": draft.validation_errors,
            "validation_warnings": draft.validation_warnings,
            "content": draft.content,
            "created_at": draft.created_at,
        })

    # ── list_resume_drafts ────────────────────────────────────────────────────
    elif name == "list_resume_drafts":
        from resumes.models import ResumeDraft

        consultant_id = args.get("consultant_id")
        limit = min(int(args.get("limit", 20)), MAX_ROWS)
        qs = ResumeDraft.objects.filter(consultant_id=consultant_id).select_related("job").order_by("-created_at")
        if job_id := args.get("job_id"):
            qs = qs.filter(job_id=job_id)
        rows = [{
            "id": d.pk,
            "job_title": d.job.title if d.job else None,
            "job_company": d.job.company if d.job else None,
            "version": d.version,
            "status": d.status,
            "ats_score": d.ats_score,
            "created_at": d.created_at,
        } for d in qs[:limit]]
        return _ok({"count": len(rows), "results": rows})

    else:
        return _err(f"Unknown tool: {name}")


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
