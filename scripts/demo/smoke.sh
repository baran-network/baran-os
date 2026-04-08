#!/usr/bin/env bash
# hello-baran CI smoke test (FR-015, FR-016, FR-017).
#
# Polling surface (research R7 / task T005): the operator UI's web client hits
# the runtime's operator HTTP API directly at GET /api/workflows
# (see ui/src/lib/api.ts and core/runtime/operator_handler.go). The runtime
# health/HTTP port is 8080 by default; the demo compose publishes it on host
# port 8080 so this script can poll http://localhost:8080/api/workflows.
#
# Response shape (from core/runtime/operator_handler.go workflowJSON):
#   { "workflows": [ { "id": ..., "status": "...", "steps": [ { "agent_id": ... } ] } ] }
# We assert: at least one workflow with status == "COMPLETED" and exactly two
# steps on two distinct agent_ids.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

LOG_DIR="./demo-smoke-logs"
COMPOSE_FILE="deploy/demo/docker-compose.yml"

cleanup() {
  scripts/demo/down.sh || true
}
trap cleanup EXIT

scripts/demo/up.sh

RUNTIME_URL="${BARAN_RUNTIME_URL:-http://localhost:8080}"
DEADLINE=$(( $(date +%s) + 120 ))

dump_logs_and_fail() {
  local component="$1"
  local reason="$2"
  mkdir -p "$LOG_DIR"
  for svc in nats runtime sidecar operator-ui hello-go hello-py hello-trigger; do
    docker compose -f "$COMPOSE_FILE" logs --no-color "$svc" \
      > "$LOG_DIR/${svc}.log" 2>&1 || true
  done
  echo "FAIL: ${component} ${reason} (see ${LOG_DIR}/${component}.log)" >&2
  exit 1
}

while :; do
  body="$(curl -fsS "${RUNTIME_URL}/api/workflows" 2>/dev/null || true)"
  if [ -n "$body" ]; then
    # Look for COMPLETED workflow with 2 distinct agent_ids in its steps.
    result="$(printf '%s' "$body" | python3 - <<'PY' || true
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(2)
wfs = data.get("workflows") or []
for wf in wfs:
    status = (wf.get("status") or "").upper()
    if "COMPLETE" not in status:
        continue
    steps = wf.get("steps") or []
    if len(steps) != 2:
        continue
    agents = {s.get("agent_id") for s in steps if s.get("agent_id")}
    if len(agents) == 2:
        print("OK")
        sys.exit(0)
sys.exit(1)
PY
    )"
    if [ "$result" = "OK" ]; then
      echo "PASS: hello-baran workflow COMPLETED on 2 distinct agents"
      exit 0
    fi
  fi

  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    dump_logs_and_fail "runtime" "no COMPLETED workflow with 2 distinct agents within 120s"
  fi
  sleep 2
done
