#!/usr/bin/env bash
# ==============================================================================
# docker-build-push.sh — Build and push the fyntrac-py-model Docker image
#
# Usage:
#   ./docker-build-push.sh [OPTIONS]
#
# Options:
#   -r, --registry    Registry host (e.g. docker.io/myorg, ghcr.io/myorg)
#                     Required unless --local is set.
#   -v, --version     Image version tag (default: git short SHA or "latest")
#   -l, --local       Build locally only, skip push
#   -n, --no-cache    Build without Docker layer cache
#   -h, --help        Show this help message
#
# Examples:
#   ./docker-build-push.sh --registry docker.io/fyntrac --version 1.2.0
#   ./docker-build-push.sh --registry ghcr.io/myorg --version 1.2.0
#   ./docker-build-push.sh --local
# ==============================================================================

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
IMAGE_NAME="fyntrac-dsl-model"
REGISTRY=""
VERSION=""
LOCAL_ONLY=false
NO_CACHE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

log()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()     { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()   { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }
header() { echo -e "\n${BOLD}$*${NC}"; }

# ── Help ──────────────────────────────────────────────────────────────────────
usage() {
  grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'
  exit 0
}

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -r|--registry)  REGISTRY="$2";   shift 2 ;;
    -v|--version)   VERSION="$2";    shift 2 ;;
    -l|--local)     LOCAL_ONLY=true; shift   ;;
    -n|--no-cache)  NO_CACHE=true;   shift   ;;
    -h|--help)      usage ;;
    *) error "Unknown option: $1"; usage ;;
  esac
done

# ── Resolve version ───────────────────────────────────────────────────────────
if [[ -z "$VERSION" ]]; then
  if git -C "$SCRIPT_DIR" rev-parse --is-inside-work-tree &>/dev/null; then
    VERSION="$(git -C "$SCRIPT_DIR" rev-parse --short HEAD)"
    warn "No --version provided. Using git SHA: ${VERSION}"
  else
    VERSION="latest"
    warn "No --version provided and not in a git repo. Using: ${VERSION}"
  fi
fi

# ── Validate ──────────────────────────────────────────────────────────────────
if [[ "$LOCAL_ONLY" == false && -z "$REGISTRY" ]]; then
  error "A --registry is required when not using --local."
  echo "  Example: ./docker-build-push.sh --registry docker.io/myorg --version 1.0.0"
  exit 1
fi

if ! command -v docker &>/dev/null; then
  error "Docker is not installed or not in PATH."
  exit 1
fi

# ── Derived tags ──────────────────────────────────────────────────────────────
if [[ "$LOCAL_ONLY" == true ]]; then
  FULL_IMAGE="${IMAGE_NAME}"
else
  FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}"
fi

TAG_VERSION="${FULL_IMAGE}:${VERSION}"
TAG_LATEST="${FULL_IMAGE}:latest"

# ── Summary ───────────────────────────────────────────────────────────────────
header "════════════════════════════════════════"
header "  fyntrac-py-model — Docker Build & Push"
header "════════════════════════════════════════"
log "Image   : ${TAG_VERSION}"
log "Latest  : ${TAG_LATEST}"
log "Context : ${SCRIPT_DIR}"
log "No-cache: ${NO_CACHE}"
log "Push    : $( [[ "$LOCAL_ONLY" == true ]] && echo 'No (local only)' || echo "Yes → ${REGISTRY}" )"
echo ""

# ── Build ─────────────────────────────────────────────────────────────────────
header "▶ Building image..."

BUILD_ARGS=(
  "build"
  "--file" "${SCRIPT_DIR}/Dockerfile"
  "--tag"  "${TAG_VERSION}"
  "--tag"  "${TAG_LATEST}"
)

[[ "$NO_CACHE" == true ]] && BUILD_ARGS+=("--no-cache")

BUILD_ARGS+=("${SCRIPT_DIR}")

docker "${BUILD_ARGS[@]}"
ok "Build complete: ${TAG_VERSION}"

# ── Push ──────────────────────────────────────────────────────────────────────
if [[ "$LOCAL_ONLY" == false ]]; then
  header "▶ Pushing image to ${REGISTRY}..."

  docker push "${TAG_VERSION}"
  ok "Pushed: ${TAG_VERSION}"

  docker push "${TAG_LATEST}"
  ok "Pushed: ${TAG_LATEST}"
fi

# ── Done ──────────────────────────────────────────────────────────────────────
header "════════════════════════════════════════"
ok "Done!"
if [[ "$LOCAL_ONLY" == false ]]; then
  echo -e "  ${GREEN}${TAG_VERSION}${NC}"
  echo -e "  ${GREEN}${TAG_LATEST}${NC}"
fi
echo ""
