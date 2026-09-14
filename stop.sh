#!/usr/bin/env bash
# =============================================================================
# fyntrac-py-model — STOP
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
TIMEOUT=15   # seconds to wait for graceful shutdown before SIGKILL

# ── Load .env (same precedence as start.sh: an already-exported var wins) ────
# Needed so this resolves the SAME per-port PID file start.sh created — e.g.
# `SERVICE_PORT=8091 ./stop.sh` stops that specific instance, not the default one.
if [[ -f "$ENV_FILE" ]]; then
    while IFS='=' read -r _key _value; do
        [[ -z "$_key" ]] && continue
        if [[ -z "${!_key+x}" ]]; then
            export "$_key=$_value"
        fi
    done < <(grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$')
fi

SERVICE_PORT="${SERVICE_PORT:-8090}"
PID_FILE="$SCRIPT_DIR/fyntrac-py-model-${SERVICE_PORT}.pid"

# ── Check PID file ───────────────────────────────────────────────────────────
if [[ ! -f "$PID_FILE" ]]; then
    echo "[fyntrac-py-model] No PID file found for port $SERVICE_PORT — service may not be running."
    exit 0
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
    echo "[fyntrac-py-model] Process $PID not found — removing stale PID file."
    rm -f "$PID_FILE"
    exit 0
fi

# ── Graceful shutdown (SIGTERM) ───────────────────────────────────────────────
echo "[fyntrac-py-model] Stopping PID $PID on port $SERVICE_PORT (SIGTERM) ..."
kill -TERM "$PID"

# Wait up to TIMEOUT seconds for the process to exit
ELAPSED=0
while kill -0 "$PID" 2>/dev/null; do
    if [[ "$ELAPSED" -ge "$TIMEOUT" ]]; then
        echo "[fyntrac-py-model] Timeout after ${TIMEOUT}s — sending SIGKILL to PID $PID"
        kill -KILL "$PID" 2>/dev/null || true
        break
    fi
    sleep 1
    ELAPSED=$((ELAPSED + 1))
done

rm -f "$PID_FILE"
echo "[fyntrac-py-model] Stopped."
