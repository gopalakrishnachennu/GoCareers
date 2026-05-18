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
