#!/usr/bin/env bash
# hello-baran demo stack teardown (FR-002, SC-003). Idempotent.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

COMPOSE_FILE="deploy/demo/docker-compose.yml"

if ! command -v docker >/dev/null 2>&1; then
  exit 0
fi

docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
echo "==> hello-baran demo stack removed"
