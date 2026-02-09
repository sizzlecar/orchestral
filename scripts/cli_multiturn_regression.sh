#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-configs/orchestral.yaml}"
THREAD_ID="${THREAD_ID:-cli-regression-$(date +%s)-$RANDOM}"

INPUT_FILE="$(mktemp)"
OUTPUT_FILE="$(mktemp)"

cleanup() {
  rm -f "${INPUT_FILE}" "${OUTPUT_FILE}"
}
trap cleanup EXIT

cat >"${INPUT_FILE}" <<'TURNS'
turn-1-probe
turn-2-probe
turn-3-probe
TURNS

echo "[1/2] running scripted CLI multi-turn session"
echo "      config=${CONFIG_PATH} thread_id=${THREAD_ID}"
(
  cd "${ROOT_DIR}"
  RUST_LOG=error cargo run -p orchestral-cli -- run \
    --config "${CONFIG_PATH}" \
    --thread-id "${THREAD_ID}" \
    --script "${INPUT_FILE}"
) >"${OUTPUT_FILE}" 2>&1

echo "[2/2] validating outputs"
for marker in "Echo: turn-1-probe" "Echo: turn-2-probe" "Echo: turn-3-probe"; do
  if ! rg -Fq "${marker}" "${OUTPUT_FILE}"; then
    echo "missing assistant output marker: ${marker}"
    echo "---- cli output ----"
    cat "${OUTPUT_FILE}"
    exit 1
  fi
  echo "      found marker: ${marker}"
done

finished_count="$(rg -Fc "Execution finished" "${OUTPUT_FILE}" || true)"
if [[ "${finished_count}" -lt 3 ]]; then
  echo "expected at least 3 completed turns, got ${finished_count}"
  echo "---- cli output ----"
  cat "${OUTPUT_FILE}"
  exit 1
fi

echo "PASS: scripted CLI multi-turn regression succeeded"
