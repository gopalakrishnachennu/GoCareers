#!/usr/bin/env bash
##############################################################################
#  local_workers.sh — Run local Celery harvest workers against prod Redis.
#
#  Your local machine connects to prod Redis (Hetzner) via SSH tunnel.
#  Workers pull tasks from the live "batches,harvest" queue — same queue that
#  Full Fetch / Quick Fetch enqueue to. This supplements the 2-vCPU Hetzner
#  VPS with your local machine's full CPU capacity.
#
#  Prerequisites:
#    • .env.harvester must exist (used for DATABASE_URL + SSH key creds)
#    • venv must be set up (run: ./scripts/harvest.sh once to auto-create it)
#
#  Usage:
#    ./scripts/local_workers.sh              # default: 6 concurrent harvest workers
#    ./scripts/local_workers.sh 10           # override concurrency
#    ./scripts/local_workers.sh 4 default    # also listen on default queue
#
#  Stop workers:
#    Ctrl-C — tunnels are closed automatically on exit.
#
#  How it works:
#    1. Opens SSH tunnel  localhost:5433 → Hetzner:5432  (Postgres)
#    2. Opens SSH tunnel  localhost:6379 → Hetzner:6379  (Redis)
#    3. Sets CELERY_TASK_ALWAYS_EAGER=0 so tasks run from the broker queue
#    4. Starts: celery -A config worker -Q batches,harvest
##############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

G='\033[0;32m'; Y='\033[1;33m'; R='\033[0;31m'; B='\033[0;34m'; N='\033[0m'
ok()   { echo -e "${G}✓${N} $*"; }
info() { echo -e "${B}▶${N} $*"; }
warn() { echo -e "${Y}⚠${N} $*"; }
err()  { echo -e "${R}✗${N} $*" >&2; exit 1; }

CONCURRENCY="${1:-6}"
QUEUES="${2:-batches,harvest}"

# ── 1. Env check ─────────────────────────────────────────────────────────────
[ -f ".env.harvester" ] || err ".env.harvester not found. Run harvest.sh first."

# ── 2. Load env ───────────────────────────────────────────────────────────────
set -a; source .env.harvester; set +a

VPS="${VPS_HOST:-62.238.6.14}"
SSHUSER="${VPS_USER:-root}"
SSHKEY="${VPS_SSH_KEY:-~/.ssh/github_actions_deploy}"
SSHKEY="${SSHKEY/#\~/$HOME}"

# ── 3. Venv check ─────────────────────────────────────────────────────────────
PYTHON=""
if [ -f ".venv-harvester/bin/python" ]; then
    PYTHON=".venv-harvester/bin/python"
elif [ -f "venv/bin/python3.12" ]; then
    PYTHON="venv/bin/python3.12"
else
    err "No venv found. Run ./scripts/harvest.sh once to set it up."
fi
CELERY_BIN="$(dirname "$PYTHON")/celery"
[ -f "$CELERY_BIN" ] || err "celery not found in venv. Run: $(dirname "$PYTHON")/pip install celery"

# ── 4. Track PIDs we opened so we can close them on exit ─────────────────────
TUNNEL_PIDS=()

cleanup() {
    echo ""
    info "Shutting down..."
    for pid in "${TUNNEL_PIDS[@]+"${TUNNEL_PIDS[@]}"}"; do
        kill "$pid" 2>/dev/null && info "Tunnel closed (pid $pid)."
    done
    ok "Done."
}
trap cleanup EXIT INT TERM

# ── 5. Postgres tunnel (5433 → prod 5432) ────────────────────────────────────
if ! nc -z 127.0.0.1 5433 2>/dev/null; then
    info "Opening Postgres tunnel  localhost:5433 → ${VPS}:5432..."
    ssh -i "$SSHKEY" -f -N \
        -L 5433:localhost:5432 "${SSHUSER}@${VPS}" \
        -o StrictHostKeyChecking=accept-new \
        -o ExitOnForwardFailure=yes \
        -o ServerAliveInterval=30
    PG_PID=$(pgrep -f "ssh.*5433:localhost:5432" | tail -1)
    TUNNEL_PIDS+=("$PG_PID")
    sleep 1
    nc -z 127.0.0.1 5433 2>/dev/null || err "Postgres tunnel opened but 5433 still unreachable."
    ok "Postgres tunnel up (pid $PG_PID)"
else
    info "Postgres tunnel already active on 127.0.0.1:5433"
fi

# ── 6. Redis tunnel (6379 → prod 6379) ───────────────────────────────────────
if ! nc -z 127.0.0.1 6379 2>/dev/null; then
    info "Opening Redis tunnel  localhost:6379 → ${VPS}:6379..."
    ssh -i "$SSHKEY" -f -N \
        -L 6379:localhost:6379 "${SSHUSER}@${VPS}" \
        -o StrictHostKeyChecking=accept-new \
        -o ExitOnForwardFailure=yes \
        -o ServerAliveInterval=30
    REDIS_PID=$(pgrep -f "ssh.*6379:localhost:6379" | tail -1)
    TUNNEL_PIDS+=("$REDIS_PID")
    sleep 1
    nc -z 127.0.0.1 6379 2>/dev/null || err "Redis tunnel opened but 6379 still unreachable."
    ok "Redis tunnel up (pid $REDIS_PID)"
else
    info "Redis tunnel already active on 127.0.0.1:6379"
fi

# ── 7. Verify Redis responds (ping) ──────────────────────────────────────────
REDIS_PONG=$("$PYTHON" -c "
import redis, sys
try:
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, socket_connect_timeout=3)
    pong = r.ping()
    queue_len = r.llen('batches')
    harvest_len = r.llen('harvest')
    print(f'PONG={pong}  batches={queue_len}  harvest={harvest_len}')
except Exception as e:
    print(f'ERROR:{e}', file=sys.stderr)
    sys.exit(1)
" 2>&1) || err "Redis ping failed: $REDIS_PONG"
ok "Redis: $REDIS_PONG"

# ── 8. Launch worker ─────────────────────────────────────────────────────────
echo ""
echo -e "${G}═══════════════════════════════════════════════════════════════${N}"
echo -e "${G}  LOCAL HARVEST WORKER — connecting to prod Redis + Postgres  ${N}"
echo -e "${G}═══════════════════════════════════════════════════════════════${N}"
info "Queues      : $QUEUES"
info "Concurrency : $CONCURRENCY"
info "Postgres    : 127.0.0.1:5433 → ${VPS}:5432"
info "Redis       : 127.0.0.1:6379 → ${VPS}:6379"
info "Settings    : config.settings_local_harvester"
echo ""
warn "Press Ctrl-C to stop workers and close tunnels."
echo ""

# Override broker to point at tunnel; disable eager mode so tasks actually queue
export CELERY_BROKER_URL="redis://localhost:6379/0"
export CELERY_RESULT_BACKEND="redis://localhost:6379/0"
export CELERY_TASK_ALWAYS_EAGER="0"
export DJANGO_SETTINGS_MODULE="config.settings_local_harvester"

"$CELERY_BIN" -A config worker \
    -l info \
    --concurrency="$CONCURRENCY" \
    -Q "$QUEUES" \
    --hostname="local-harvest@%h" \
    --max-tasks-per-child=50
