#!/usr/bin/env bash
##############################################################################
#  ops.sh — Run any Django management command locally against prod DB.
#
#  Replaces GitHub Actions SSH workflows with local execution (faster CPU,
#  no VPS bottleneck). Reads creds from .env.harvester — same file as harvest.sh.
#
#  Usage:
#    ./scripts/ops.sh <management_command> [args...]
#
#  Examples:
#    ./scripts/ops.sh evaluate_rawjob_scope --only-unscoped --dry-run
#    ./scripts/ops.sh evaluate_rawjob_scope --all --batch-size 1000
#    ./scripts/ops.sh classify_job_domains --dry-run
#    ./scripts/ops.sh classify_job_domains --batch-size 1000
#    ./scripts/ops.sh backfill_job_marketing_roles --dry-run
#    ./scripts/ops.sh backfill_job_marketing_roles --overwrite
#    ./scripts/ops.sh refetch_ambiguous_locations --limit 500 --dry-run
#    ./scripts/ops.sh refetch_ambiguous_locations --limit 5000 --provider
#    ./scripts/ops.sh print_scope_summary
#    ./scripts/ops.sh migrate --no-input
#    ./scripts/ops.sh shell                          # prod DB shell
#
#  ⚠  This runs against PRODUCTION DATABASE by default (direct mode).
#     Always use --dry-run first for any command that writes data.
##############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

# ── colours ──────────────────────────────────────────────────────────────────
G='\033[0;32m'; Y='\033[1;33m'; R='\033[0;31m'; B='\033[0;34m'; N='\033[0m'
ok()   { echo -e "${G}✓${N} $*"; }
info() { echo -e "${B}▶${N} $*"; }
warn() { echo -e "${Y}⚠${N} $*"; }
err()  { echo -e "${R}✗${N} $*" >&2; exit 1; }

# ── usage guard ───────────────────────────────────────────────────────────────
if [ $# -eq 0 ]; then
    echo "Usage: ./scripts/ops.sh <management_command> [args...]"
    echo ""
    echo "Common commands:"
    echo "  evaluate_rawjob_scope   --only-unscoped --dry-run"
    echo "  classify_job_domains    --dry-run"
    echo "  backfill_job_marketing_roles --dry-run"
    echo "  refetch_ambiguous_locations  --limit 500 --dry-run"
    echo "  print_scope_summary"
    echo "  migrate --no-input"
    echo "  shell"
    exit 1
fi

COMMAND="$1"
shift
ARGS=("$@")

# ── env file check ────────────────────────────────────────────────────────────
if [ ! -f ".env.harvester" ]; then
    err ".env.harvester not found. Copy .env.harvester.example and fill in DATABASE_URL."
fi

# ── load env ──────────────────────────────────────────────────────────────────
set -a; source .env.harvester; set +a

# ── venv check ────────────────────────────────────────────────────────────────
PYTHON=""
if [ -f "venv/bin/python3.12" ]; then
    PYTHON="venv/bin/python3.12"
elif [ -f ".venv-harvester/bin/python" ]; then
    PYTHON=".venv-harvester/bin/python"
else
    err "No venv found. Run harvest.sh once to set it up, or: python3 -m venv venv && venv/bin/pip install -r requirements.txt"
fi

# ── env validation ────────────────────────────────────────────────────────────
DB_URL="${DATABASE_URL:-}"
[ -z "$DB_URL" ] && err "DATABASE_URL is not set in .env.harvester"

if [[ "$DB_URL" == sqlite* ]]; then
    warn "DATABASE_URL points to SQLite — this is a local DB, not production."
    warn "Edit .env.harvester and set DATABASE_URL to your prod Postgres URL."
    read -r -p "Continue anyway? [y/N] " CONFIRM
    [[ "$CONFIRM" =~ ^[Yy]$ ]] || exit 1
fi

# Mask password for display
DB_DISPLAY=$(echo "$DB_URL" | sed 's|://[^:]*:[^@]*@|://***:***@|')

# ── dry-run warning for write commands ───────────────────────────────────────
WRITE_COMMANDS="evaluate_rawjob_scope classify_job_domains backfill_job_marketing_roles refetch_ambiguous_locations enrich_existing_jobs"
IS_WRITE=false
for wc in $WRITE_COMMANDS; do
    if [ "$COMMAND" = "$wc" ]; then IS_WRITE=true; break; fi
done

HAS_DRY_RUN=false
for arg in "${ARGS[@]+"${ARGS[@]}"}"; do
    [ "$arg" = "--dry-run" ] && HAS_DRY_RUN=true && break
done

if $IS_WRITE && ! $HAS_DRY_RUN; then
    warn "You are about to run '${COMMAND}' WITHOUT --dry-run against:"
    warn "$DB_DISPLAY"
    warn "This will WRITE to production. Are you sure?"
    read -r -p "Type 'yes' to continue: " CONFIRM
    [ "$CONFIRM" = "yes" ] || { info "Aborted."; exit 0; }
fi

# ── run ───────────────────────────────────────────────────────────────────────
echo ""
info "Command : manage.py $COMMAND ${ARGS[*]+"${ARGS[*]}"}"
info "Database: $DB_DISPLAY"
info "Settings: ${DJANGO_SETTINGS_MODULE:-config.settings_local_harvester}"
echo ""

START=$(date +%s)

"$PYTHON" manage.py "$COMMAND" "${ARGS[@]+"${ARGS[@]}"}"

ELAPSED=$(( $(date +%s) - START ))
echo ""
ok "Done in ${ELAPSED}s"
