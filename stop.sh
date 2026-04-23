#!/usr/bin/env bash
# =============================================================================
# fyntrac-py-model — STOP
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/fyntrac-py-model.pid"
TIMEOUT=15   # seconds to wait for graceful shutdown before SIGKILL

# ── Check PID file ───────────────────────────────────────────────────────────
if [[ ! -f "$PID_FILE" ]]; then
    echo "[fyntrac-py-model] No PID file found — service may not be running."
    exit 0
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
    echo "[fyntrac-py-model] Process $PID not found — removing stale PID file."
    rm -f "$PID_FILE"
    exit 0
fi

# ── Graceful shutdown (SIGTERM) ───────────────────────────────────────────────
echo "[fyntrac-py-model] Stopping PID $PID (SIGTERM) ..."
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
