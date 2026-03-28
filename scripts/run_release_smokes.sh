#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ROUNDS="${1:-${ORCHESTRAL_RELEASE_ROUNDS:-1}}"
ENV_FILE="${ORCHESTRAL_RELEASE_ENV_FILE:-.env.local}"
INCLUDE_P2="${ORCHESTRAL_RELEASE_INCLUDE_P2:-0}"

if ! [[ "$ROUNDS" =~ ^[0-9]+$ ]] || [[ "$ROUNDS" -lt 1 ]]; then
  echo "rounds must be a positive integer" >&2
  exit 2
fi

SPECS=(
  "configs/scenarios/no_fake_execution_patch_toml.smoke.yaml"
  "configs/scenarios/resume_after_action_failure_xlsx.smoke.yaml"
  "configs/scenarios/wait_user_missing_required_field_contract.smoke.yaml"
  "configs/scenarios/git_log_summary_no_approval.smoke.yaml"
)

if [[ "$INCLUDE_P2" == "1" ]]; then
  SPECS+=(
    "configs/scenarios/skill_implicit_xlsx.smoke.yaml"
    "configs/scenarios/mcp_tool_lookup.smoke.yaml"
  )
fi

for ((round = 1; round <= ROUNDS; round++)); do
  echo "== Release smoke round ${round}/${ROUNDS} =="
  for spec in "${SPECS[@]}"; do
    echo "-- Running ${spec}"
    ./.venv/bin/dotenv -f "$ENV_FILE" run -- \
      cargo run -q -p orchestral-cli -- scenario --spec "$spec"
  done
done
