#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

forbidden_paths=(
  "REFACTOR_PLAN.md"
  "apps/orchestral-cli/src/scenario.rs"
  # The TUI remains a supported product surface. Only its retired,
  # pre-agent-protocol implementation modules must stay removed.
  "apps/orchestral-cli/src/tui/bottom_pane"
  "apps/orchestral-cli/src/tui/event_loop.rs"
  "apps/orchestral-cli/src/tui/protocol.rs"
  "apps/orchestral-cli/src/tui/ui.rs"
  "apps/orchestral-cli/src/tui/update.rs"
  "apps/orchestral-cli/src/tui/widgets"
  "apps/orchestral-server"
  "configs/orchestral.yaml"
  "core/orchestral-runtime/src/api/runtime.rs"
  "core/orchestral-runtime/src/orchestrator"
  "core/orchestral-runtime/src/planner"
  "core/orchestral-runtime/src/prompts"
  "core/orchestral-runtime/src/session.rs"
  "fixtures/scenarios"
)

for path in "${forbidden_paths[@]}"; do
  tracked="$(git ls-files -- "${path}" "${path}/**")"
  if [[ -n "${tracked}" ]]; then
    echo "retired production surface is tracked: ${path}" >&2
    printf '%s\n' "${tracked}" >&2
    exit 1
  fi
done

if matches="$(git grep -n -E '\b(OrchestralApp|RuntimeApi|ScenarioRunner)\b' -- \
  apps/orchestral-cli/src core/orchestral-runtime/src core/orchestral/src examples 2>/dev/null)"; then
  echo "retired production identifiers remain:" >&2
  printf '%s\n' "${matches}" >&2
  exit 1
fi

if matches="$(git grep -n -E 'orchestral-cli -- (run|scenario)' -- \
  README.md README.zh-CN.md CLAUDE.md scripts examples 2>/dev/null)"; then
  echo "retired CLI invocation remains:" >&2
  printf '%s\n' "${matches}" >&2
  exit 1
fi

echo "Agent surface gate passed"
