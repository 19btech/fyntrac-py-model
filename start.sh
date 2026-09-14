#!/usr/bin/env bash
# =============================================================================
# fyntrac-py-model — START
# =============================================================================
set -euo pipefail

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/venv"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Load .env ────────────────────────────────────────────────────────────────
# Loaded BEFORE the already-running guard below, and WITHOUT overriding a
# variable the caller already exported — so `SERVICE_PORT=8091 ./start.sh`
# (running a second instance on this host, e.g. to split a large batch's
# chunks across more CPU — see MAX_PROCESS_WORKERS) actually takes effect
# instead of being silently clobbered back to .env's SERVICE_PORT=8090.
if [[ -f "$ENV_FILE" ]]; then
    while IFS='=' read -r _key _value; do
        [[ -z "$_key" ]] && continue
        if [[ -z "${!_key+x}" ]]; then
            export "$_key=$_value"
        fi
    done < <(grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$')
fi

# ── Local Overrides for Host Execution ───────────────────────────────────────
# When running locally via this script (not in Docker), we must point to localhost
# instead of the internal Docker network aliases.
export MONGODB_HOST="127.0.0.1"
export PULSAR_SERVICE_URL="pulsar://127.0.0.1:6650"
export MEMCACHED_HOST="127.0.0.1"

# Defaults if not set in .env or the environment
SERVICE_HOST="${SERVICE_HOST:-0.0.0.0}"
SERVICE_PORT="${SERVICE_PORT:-8090}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

# ── Per-instance PID/log files, keyed by port ────────────────────────────────
# Running a second instance on this host is just `SERVICE_PORT=8091 ./start.sh`
# (pair it with a different MAX_PROCESS_WORKERS per instance so they split the
# CPU instead of each claiming all of it). Keying these files by port means the
# guard below only blocks a genuine duplicate on the SAME port.
PID_FILE="$SCRIPT_DIR/fyntrac-py-model-${SERVICE_PORT}.pid"
LOG_FILE="/tmp/fyntrac-py-model-${SERVICE_PORT}.log"

# ── Guard: already running on this port? ─────────────────────────────────────
if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "[fyntrac-py-model] Already running on port $SERVICE_PORT (PID $PID). Use restart.sh to bounce it."
        exit 1
    else
        echo "[fyntrac-py-model] Stale PID file found — cleaning up."
        rm -f "$PID_FILE"
    fi
fi

# ── Activate virtualenv ──────────────────────────────────────────────────────
if [[ ! -f "$VENV_DIR/bin/activate" ]]; then
    echo "[fyntrac-py-model] ERROR: virtualenv not found at $VENV_DIR"
    echo "  Run: python3 -m venv venv && pip install -r requirements.txt"
    exit 1
fi
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

# ── Launch uvicorn ───────────────────────────────────────────────────────────
cd "$SCRIPT_DIR"

echo "[fyntrac-py-model] Starting on http://$SERVICE_HOST:$SERVICE_PORT ..."
echo "[fyntrac-py-model] Log → $LOG_FILE"
# Create new log file every time
: > "$LOG_FILE"

nohup uvicorn app.main:app \
    --host "$SERVICE_HOST" \
    --port "$SERVICE_PORT" \
    --log-level "$(echo "$LOG_LEVEL" | tr '[:upper:]' '[:lower:]')" \
    > "$LOG_FILE" 2>&1 &

PID=$!
echo "$PID" > "$PID_FILE"
echo "[fyntrac-py-model] Started (PID $PID)"

# ── Tail the log so startup errors are visible ───────────────────────────────
sleep 1
if ! kill -0 "$PID" 2>/dev/null; then
    echo "[fyntrac-py-model] ERROR: Process died immediately. Check $LOG_FILE"
    rm -f "$PID_FILE"
    tail -30 "$LOG_FILE"
    exit 1
fi

echo "[fyntrac-py-model] Running. Use 'tail -f $LOG_FILE' to follow logs."
