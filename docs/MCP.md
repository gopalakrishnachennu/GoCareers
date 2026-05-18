# GoCareers MCP Server — Full Setup & Integration Guide

> **MCP (Model Context Protocol)** lets AI assistants like Claude talk directly to
> your GoCareers database and pipeline in real time — no copy-pasting, no SSH,
> no manual queries. Just ask Claude a question and it queries the live system.

---

## Table of Contents

1. [What You Can Do with MCP](#what-you-can-do)
2. [Available Tools](#available-tools)
3. [Local Setup (one-time)](#local-setup)
4. [Connect: Claude Desktop](#connect-claude-desktop)
5. [Connect: Claude Code (CLI)](#connect-claude-code)
6. [Connect: Cursor / Cline / Windsurf](#connect-cursor--cline--windsurf)
7. [Connect: ChatGPT (via REST API)](#connect-chatgpt)
8. [Connect: Claude API (programmatic)](#connect-claude-api)
9. [Settings Reference](#settings-reference)
10. [Security Notes](#security-notes)
11. [Example Conversations](#example-conversations)
12. [Troubleshooting](#troubleshooting)

---

## What You Can Do

Instead of SSH-ing into production and running shell commands, you just **talk to Claude**:

| What you say | What Claude does |
|---|---|
| "How many jobs are pending sync?" | Calls `get_pipeline_stats` → returns live DB counts |
| "Why did sync run #1087 fail?" | Calls `get_ops_run_detail(1087)` → reads audit payload → explains it |
| "Show me unknown country jobs" | Calls `get_unknown_country_jobs` → returns the review queue |
| "Approve RawJob 31500 as US" | Calls `approve_unknown_country(31500, 'US')` → updates DB |
| "Trigger a full sync now" | Calls `trigger_sync(qualified_only=True)` → runs and reports back |
| "Find senior engineer jobs in Germany" | Calls `search_jobs(query='senior engineer', country='DE')` |
| "Fix the index corruption" | Calls `reindex_rawjob_table` → runs REINDEX CONCURRENTLY |

---

## Available Tools

### Read Tools (always enabled)

| Tool | Description |
|---|---|
| `get_pipeline_stats` | Live snapshot: RawJob counts by status, Job pool size, last 5 ops runs |
| `get_recent_ops_runs` | Last N HarvestOpsRun records with results (filter by operation) |
| `get_rawjobs` | Query RawJobs by sync_status, scope_status, company name |
| `get_company` | Company details: platforms, harvest stats, RawJob sync breakdown |
| `search_jobs` | Full-text search the vetted Job pool |
| `get_unknown_country_jobs` | REVIEW_UNKNOWN_COUNTRY queue with location hints |
| `get_ops_run_detail` | Full audit_payload for any ops run by ID |
| `explain_rawjob` | Deep breakdown of one RawJob: status, gate result, why it passed/failed |

### Write Tools (enabled when `MCP_ALLOWED_ACTIONS=read,write`)

| Tool | Description |
|---|---|
| `trigger_sync` | Fire `sync_harvested_to_pool_task` with configurable params |
| `approve_unknown_country` | Set country + re-scope to PRIORITY_TARGET |
| `reindex_rawjob_table` | Run `REINDEX TABLE CONCURRENTLY harvest_rawjob` |

---

## Local Setup

### Step 1 — Install the MCP SDK

```bash
cd /path/to/consulting

# If using venv
source venv/bin/activate
pip install mcp

# If using uv (faster)
uv pip install mcp
```

### Step 2 — Set environment variables

The MCP server uses the same `.env` as the web app. For local development pointing at local DB:

```bash
# .env (local dev)
DATABASE_URL=postgres://consulting:password@localhost:5432/consulting
MCP_ALLOWED_ACTIONS=read,write
MCP_MAX_ROWS=100
```

For ops work pointing at **production DB** (same as `.env.harvester`):

```bash
# .env.harvester
DATABASE_URL=postgres://consulting:ConsultingDbPass2026@62.238.6.14:5432/consulting
MCP_ALLOWED_ACTIONS=read,write
MCP_MAX_ROWS=200
```

### Step 3 — Test it manually

```bash
# Should print nothing (waits for MCP client to connect via stdio)
python mcp_server.py

# Ctrl+C to exit — if it starts without errors you're good
```

---

## Connect: Claude Desktop

Claude Desktop supports MCP natively. Configuration lives in a JSON file.

### Find the config file

| OS | Path |
|---|---|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| Linux | `~/.config/claude/claude_desktop_config.json` |

### Add GoCareers to the config

```json
{
  "mcpServers": {
    "gocareers": {
      "command": "/path/to/consulting/venv/bin/python",
      "args": ["/path/to/consulting/mcp_server.py"],
      "env": {
        "DATABASE_URL": "postgres://consulting:ConsultingDbPass2026@62.238.6.14:5432/consulting",
        "MCP_ALLOWED_ACTIONS": "read,write",
        "MCP_MAX_ROWS": "100",
        "DJANGO_SETTINGS_MODULE": "config.settings"
      }
    }
  }
}
```

> **Replace** `/path/to/consulting` with the actual path on your machine,
> e.g. `/Users/gopalakrishnachennu/Desktop/Devops/consulting`.

### If using uv

```json
{
  "mcpServers": {
    "gocareers": {
      "command": "uv",
      "args": [
        "run",
        "--directory", "/Users/gopalakrishnachennu/Desktop/Devops/consulting",
        "python", "mcp_server.py"
      ],
      "env": {
        "DATABASE_URL": "postgres://consulting:ConsultingDbPass2026@62.238.6.14:5432/consulting",
        "MCP_ALLOWED_ACTIONS": "read,write"
      }
    }
  }
}
```

### Restart Claude Desktop

After saving the config, **quit and reopen Claude Desktop**. You should see a 🔧 tools icon in the chat input bar. Click it to verify `gocareers` tools are listed.

---

## Connect: Claude Code

Claude Code (the CLI) reads MCP config from `.mcp.json` in the project root.

### Option A — Project-level (checked into repo)

Create `.mcp.json` at `/Users/gopalakrishnachennu/Desktop/Devops/consulting/.mcp.json`:

```json
{
  "mcpServers": {
    "gocareers": {
      "command": "python",
      "args": ["mcp_server.py"],
      "env": {
        "DJANGO_SETTINGS_MODULE": "config.settings"
      }
    }
  }
}
```

> Claude Code inherits the shell environment so `DATABASE_URL` from your `.env` is already available. No need to repeat it here if you source `.env` before launching `claude`.

### Option B — Global user config

```bash
claude mcp add gocareers \
  --command "python /Users/gopalakrishnachennu/Desktop/Devops/consulting/mcp_server.py" \
  --env DATABASE_URL="postgres://consulting:ConsultingDbPass2026@62.238.6.14:5432/consulting" \
  --env MCP_ALLOWED_ACTIONS="read,write"
```

### Verify in Claude Code

```
/mcp
```

Should show `gocareers` as connected with all tools listed.

---

## Connect: Cursor / Cline / Windsurf

All of these support MCP via the same stdio protocol.

### Cursor

Open **Cursor Settings → Features → MCP Servers → Add Server**:

```json
{
  "name": "gocareers",
  "command": "python",
  "args": ["/Users/gopalakrishnachennu/Desktop/Devops/consulting/mcp_server.py"],
  "env": {
    "DATABASE_URL": "postgres://consulting:ConsultingDbPass2026@62.238.6.14:5432/consulting",
    "MCP_ALLOWED_ACTIONS": "read,write",
    "DJANGO_SETTINGS_MODULE": "config.settings"
  }
}
```

### Cline (VS Code extension)

In VS Code settings (`.vscode/settings.json`):

```json
{
  "cline.mcpServers": {
    "gocareers": {
      "command": "python",
      "args": ["/Users/gopalakrishnachennu/Desktop/Devops/consulting/mcp_server.py"],
      "env": {
        "DATABASE_URL": "postgres://consulting:ConsultingDbPass2026@62.238.6.14:5432/consulting",
        "MCP_ALLOWED_ACTIONS": "read,write",
        "DJANGO_SETTINGS_MODULE": "config.settings"
      }
    }
  }
}
```

### Windsurf

Go to **Windsurf Settings → Cascade → MCP** and add:

```json
{
  "mcpServers": {
    "gocareers": {
      "command": "python",
      "args": ["/Users/gopalakrishnachennu/Desktop/Devops/consulting/mcp_server.py"],
      "env": {
        "DATABASE_URL": "postgres://consulting:ConsultingDbPass2026@62.238.6.14:5432/consulting",
        "MCP_ALLOWED_ACTIONS": "read,write",
        "DJANGO_SETTINGS_MODULE": "config.settings"
      }
    }
  }
}
```

---

## Connect: ChatGPT

> **Important:** ChatGPT does **not** support MCP natively. Instead, connect via
> a REST API endpoint and a **Custom GPT with Actions** (OpenAPI spec).

### Step 1 — Expose a REST API endpoint in Django

Add to `config/urls.py`:

```python
from harvest.mcp_api import mcp_api_view
urlpatterns += [path("api/mcp/", mcp_api_view)]
```

Create `apps/harvest/mcp_api.py`:

```python
"""Thin REST wrapper around MCP tools — used by ChatGPT Custom GPT Actions."""
import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.conf import settings


@csrf_exempt
@require_POST
def mcp_api_view(request):
    token = request.headers.get("X-MCP-Token", "")
    if settings.MCP_AUTH_TOKEN and token != settings.MCP_AUTH_TOKEN:
        return JsonResponse({"error": "Unauthorized"}, status=401)

    try:
        body = json.loads(request.body)
        tool = body["tool"]
        args = body.get("args", {})
    except (KeyError, json.JSONDecodeError) as e:
        return JsonResponse({"error": str(e)}, status=400)

    # Re-use the same tool implementations
    import asyncio
    from mcp_server import _dispatch
    result = asyncio.run(_dispatch(tool, args))
    return JsonResponse({"result": result[0].text})
```

### Step 2 — Set `MCP_AUTH_TOKEN` in production

```bash
# In .env.production on Hetzner VPS
MCP_AUTH_TOKEN=your-secret-token-here
```

Generate a token:
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

### Step 3 — Create a Custom GPT

1. Go to [chatgpt.com](https://chatgpt.com) → **Explore GPTs → Create**
2. In **Configure**, click **Add actions**
3. Import this OpenAPI schema:

```yaml
openapi: 3.0.0
info:
  title: GoCareers Pipeline API
  version: 1.0.0
servers:
  - url: https://chennu.co/api/mcp
paths:
  /:
    post:
      operationId: callTool
      summary: Call a GoCareers pipeline tool
      security:
        - tokenAuth: []
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [tool]
              properties:
                tool:
                  type: string
                  enum:
                    - get_pipeline_stats
                    - get_recent_ops_runs
                    - get_rawjobs
                    - get_company
                    - search_jobs
                    - get_unknown_country_jobs
                    - get_ops_run_detail
                    - explain_rawjob
                    - trigger_sync
                    - approve_unknown_country
                args:
                  type: object
      responses:
        "200":
          description: Tool result
          content:
            application/json:
              schema:
                type: object
                properties:
                  result:
                    type: string
components:
  securitySchemes:
    tokenAuth:
      type: apiKey
      in: header
      name: X-MCP-Token
```

4. Set the **Authentication** to `API Key` → Header `X-MCP-Token` → your token.
5. Save and test.

---

## Connect: Claude API (programmatic)

Use the `anthropic` Python SDK with tool definitions:

```python
import anthropic
import json
import requests

client = anthropic.Anthropic(api_key="your-api-key")

GOCAREERS_TOOLS = [
    {
        "name": "get_pipeline_stats",
        "description": "Get live GoCareers pipeline statistics",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "search_jobs",
        "description": "Search vetted Job pool",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "country": {"type": "string"},
                "limit": {"type": "integer"},
            },
            "required": ["query"],
        },
    },
    # Add more tools as needed
]


def call_gocareers_tool(tool_name: str, tool_args: dict) -> str:
    resp = requests.post(
        "https://chennu.co/api/mcp/",
        json={"tool": tool_name, "args": tool_args},
        headers={"X-MCP-Token": "your-secret-token"},
        timeout=30,
    )
    return resp.json()["result"]


def chat_with_pipeline(question: str) -> str:
    messages = [{"role": "user", "content": question}]
    
    while True:
        response = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=4096,
            tools=GOCAREERS_TOOLS,
            messages=messages,
        )
        
        if response.stop_reason == "tool_use":
            # Execute tool calls
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = call_gocareers_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
        else:
            # Final text response
            return response.content[0].text


# Example usage
answer = chat_with_pipeline("How many jobs are pending sync right now?")
print(answer)
```

---

## Settings Reference

All settings go in `.env` (local dev) or `.env.production` (Hetzner VPS):

| Setting | Default | Description |
|---|---|---|
| `MCP_AUTH_TOKEN` | `""` (no auth) | Secret token for HTTP/SSE transport. Leave empty for local stdio. |
| `MCP_ALLOWED_ACTIONS` | `"read,write"` | Comma-separated list. Use `"read"` for read-only clients. |
| `MCP_MAX_ROWS` | `100` | Max rows any single tool call can return. |

### Example `.env` snippets

**Local dev (full access, local DB):**
```bash
MCP_ALLOWED_ACTIONS=read,write
MCP_MAX_ROWS=100
```

**Production (read-only for external clients, write for internal):**
```bash
MCP_AUTH_TOKEN=abc123...
MCP_ALLOWED_ACTIONS=read,write
MCP_MAX_ROWS=200
```

**Read-only public demo:**
```bash
MCP_AUTH_TOKEN=demo-readonly-token
MCP_ALLOWED_ACTIONS=read
MCP_MAX_ROWS=50
```

---

## Security Notes

1. **stdio mode is local-only** — the MCP server runs as a subprocess on your machine. It never listens on a network port. No token needed.

2. **HTTP mode (ChatGPT / Claude API)** — always set `MCP_AUTH_TOKEN` in production. The token is sent in `X-MCP-Token` header and validated before every request.

3. **Write tools are destructive** — `trigger_sync` processes thousands of rows, `reindex_rawjob_table` runs for 30-120 seconds. Set `MCP_ALLOWED_ACTIONS=read` for any client you don't fully trust.

4. **`DATABASE_URL` in env** — never put the production DB password in `.mcp.json` if that file is committed to git. Use `.env` files (already in `.gitignore`) or OS-level secrets.

5. **`approve_unknown_country`** — directly modifies RawJob records. Restricts write access if giving API access to external parties.

---

## Example Conversations

Once connected to Claude Desktop or Claude Code, you can say:

```
"What's the current state of the pipeline?"
→ get_pipeline_stats → shows live counts

"Show me the last 5 sync runs"
→ get_recent_ops_runs(limit=5, operation="sync_pool")

"Why did ops run 1087 fail?"
→ get_ops_run_detail(1087) → reads audit_payload → explains the error

"Find all senior backend jobs in Europe"
→ search_jobs(query="senior backend", country="EU")

"How many jobs does Stripe have in the harvest pipeline?"
→ get_company(name="Stripe") → shows all RawJob sync stats

"Show me jobs with unknown country that have 'Germany' in their location"
→ get_unknown_country_jobs(limit=50) → Claude filters by location

"Set RawJob 23500 country to DE"
→ approve_unknown_country(rawjob_id=23500, country="DE")

"Run a full sync now"
→ trigger_sync(qualified_only=True) → runs and reports synced/failed/skipped

"The indexes seem corrupt, fix it"
→ reindex_rawjob_table() → REINDEX TABLE CONCURRENTLY
```

---

## Troubleshooting

### MCP server crashes on startup

```bash
# Run manually to see the error
cd /path/to/consulting
source venv/bin/activate
python mcp_server.py
```

Common causes:
- `DATABASE_URL` not set → add it to `.env` or pass in `env` block of MCP config
- `mcp` package not installed → `pip install mcp`
- Wrong Python path in MCP config → use `which python` to get the full path

### Claude Desktop doesn't show GoCareers tools

1. Check the config file path is exactly right (case-sensitive on Linux/macOS)
2. Validate JSON syntax at [jsonlint.com](https://jsonlint.com)
3. Check Claude Desktop logs: **Help → Show Logs**
4. Restart Claude Desktop (not just close the window — quit from menu bar)

### Tools show but return errors

```bash
# Test a tool directly via Python
cd /path/to/consulting && source venv/bin/activate
python -c "
import asyncio, os, sys, django
sys.path.insert(0, 'apps')
os.environ['DJANGO_SETTINGS_MODULE'] = 'config.settings'
django.setup()
from mcp_server import _dispatch
result = asyncio.run(_dispatch('get_pipeline_stats', {}))
print(result[0].text)
"
```

### ChatGPT Custom GPT gets 401

- Check `MCP_AUTH_TOKEN` is set identically in `.env.production` and the GPT action config
- After changing env, redeploy: `gh workflow run "Deploy to Hetzner VPS" -f confirm=DEPLOY`

### "Write actions are disabled" error

Set `MCP_ALLOWED_ACTIONS=read,write` in your `.env` file and restart the MCP server.
