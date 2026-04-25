#!/usr/bin/env bash
# =============================================================================
# fyntrac-py-model — START
# =============================================================================
set -euo pipefail

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/venv"
ENV_FILE="$SCRIPT_DIR/.env"
PID_FILE="$SCRIPT_DIR/fyntrac-py-model.pid"
LOG_FILE="/tmp/fyntrac-py-model.log"

# ── Guard: already running? ──────────────────────────────────────────────────
if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "[fyntrac-py-model] Already running (PID $PID). Use restart.sh to bounce it."
        exit 1
    else
        echo "[fyntrac-py-model] Stale PID file found — cleaning up."
        rm -f "$PID_FILE"
    fi
fi

# ── Load .env ────────────────────────────────────────────────────────────────
if [[ -f "$ENV_FILE" ]]; then
    # Export only non-comment, non-empty lines
    set -o allexport
    # shellcheck disable=SC1090
    source <(grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$')
    set +o allexport
fi

# Defaults if not set in .env
SERVICE_HOST="${SERVICE_HOST:-0.0.0.0}"
SERVICE_PORT="${SERVICE_PORT:-8090}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

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
