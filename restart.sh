#!/usr/bin/env bash
# =============================================================================
# fyntrac-py-model — RESTART
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[fyntrac-py-model] Restarting ..."

"$SCRIPT_DIR/stop.sh"

# Brief pause to let ports and Pulsar connections fully release
sleep 2

"$SCRIPT_DIR/start.sh"
