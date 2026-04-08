#!/usr/bin/env bash
# hello-baran demo stack bring-up (FR-001, FR-004, FR-005).
#
# Exit codes (per contracts/make-targets.md):
#   1 — port conflict
#   2 — docker not available
#   3 — build failure
#   4 — healthcheck timeout
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

COMPOSE_FILE="deploy/demo/docker-compose.yml"
ENV_FILE="deploy/demo/.env.example"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not available" >&2
  exit 2
fi
if ! docker info >/dev/null 2>&1; then
  echo "docker not available" >&2
  exit 2
fi

# Port-conflict preflight (FR-005). Service<->port map matches
# deploy/demo/docker-compose.yml. Override via deploy/demo/.env if needed.
declare -a PORT_SVC=(
  "4222:nats(runtime)"
  "8222:nats-monitor(runtime)"
  "8080:runtime-api"
  "9090:sidecar"
  "3000:operator-ui"
)

port_in_use() {
  local p="$1"
  # Try /dev/tcp (bash) first; fall back to nc if available.
  (exec 3<>"/dev/tcp/127.0.0.1/${p}") >/dev/null 2>&1 && { exec 3<&- 3>&-; return 0; }
  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$p" >/dev/null 2>&1 && return 0
  fi
  return 1
}

for entry in "${PORT_SVC[@]}"; do
  port="${entry%%:*}"
  svc="${entry##*:}"
  if port_in_use "$port"; then
    echo "port ${port} already in use (service: ${svc})" >&2
    exit 1
  fi
done

echo "==> building and starting hello-baran demo stack"
if ! docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --build --wait; then
  rc=$?
  case "$rc" in
    1|125) echo "build failure — run 'make demo-down' before retrying" >&2; exit 3 ;;
    *)     echo "service did not become healthy within compose --wait timeout" >&2; exit 4 ;;
  esac
fi

cat <<EOF
==> hello-baran demo stack is up

  Operator UI: http://localhost:3000
  Runtime API: http://localhost:8080/api/workflows
  Sidecar:     http://localhost:9090

Run 'make demo-down' to tear everything down.
EOF
