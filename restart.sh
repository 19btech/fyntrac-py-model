#!/usr/bin/env bash
# =============================================================================
# fyntrac-py-model — RESTART
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# stop.sh/start.sh both key their PID file off SERVICE_PORT, and inherit it
# (along with MAX_PROCESS_WORKERS) from this shell's environment — so
# `SERVICE_PORT=8091 ./restart.sh` bounces that specific instance.
echo "[fyntrac-py-model] Restarting ..."

"$SCRIPT_DIR/stop.sh"

# Brief pause to let ports and Pulsar connections fully release
sleep 2

"$SCRIPT_DIR/start.sh"
