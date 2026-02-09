#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-configs/orchestral.yaml}"
LISTEN_ADDR="${LISTEN_ADDR:-127.0.0.1:18080}"
BASE_URL="http://${LISTEN_ADDR}"
SESSION_ID="${SESSION_ID:-regression-$(date +%s)-$RANDOM}"

SERVER_LOG="$(mktemp)"
SSE_LOG="$(mktemp)"
SERVER_PID=""
SSE_PID=""

cleanup() {
  if [[ -n "${SSE_PID}" ]] && kill -0 "${SSE_PID}" >/dev/null 2>&1; then
    kill "${SSE_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  rm -f "${SERVER_LOG}" "${SSE_LOG}"
}
trap cleanup EXIT

start_server() {
  echo "[1/4] starting server on ${LISTEN_ADDR} (config=${CONFIG_PATH})"
  (
    cd "${ROOT_DIR}"
    RUST_LOG=error cargo run -p orchestral-server -- --config "${CONFIG_PATH}" --listen "${LISTEN_ADDR}"
  ) >"${SERVER_LOG}" 2>&1 &
  SERVER_PID="$!"

  for _ in $(seq 1 80); do
    if curl -fsS "${BASE_URL}/health" >/dev/null 2>&1; then
      echo "      server is healthy"
      return 0
    fi
    sleep 0.25
  done

  echo "server did not become healthy in time"
  echo "---- server log ----"
  cat "${SERVER_LOG}"
  exit 1
}

start_sse() {
  echo "[2/4] subscribing SSE for session=${SESSION_ID}"
  curl -NsS "${BASE_URL}/threads/${SESSION_ID}/events" >"${SSE_LOG}" 2>/dev/null &
  SSE_PID="$!"
  sleep 0.3
}

submit_turn() {
  local text="$1"
  local marker="$2"

  local request_id="req-$(date +%s)-$RANDOM"
  local payload
  payload=$(printf '{"request_id":"%s","input":"%s"}' "${request_id}" "${text}")

  local resp
  resp=$(curl -sS -X POST "${BASE_URL}/threads/${SESSION_ID}/messages" \
    -H "content-type: application/json" \
    -d "${payload}")

  echo "      submit: ${text}"
  echo "      response: ${resp}"

  if ! printf '%s' "${resp}" | rg -q '"status":"(started|merged)"'; then
    echo "unexpected submit status"
    exit 1
  fi

  for _ in $(seq 1 120); do
    if rg -q "${marker}" "${SSE_LOG}"; then
      echo "      observed assistant output marker: ${marker}"
      return 0
    fi
    sleep 0.25
  done

  echo "timeout waiting assistant output marker: ${marker}"
  echo "---- recent SSE ----"
  tail -n 80 "${SSE_LOG}" || true
  exit 1
}

run_flow() {
  echo "[3/4] sending multi-turn conversation"
  submit_turn "turn-1-probe" "turn-1-probe"
  submit_turn "turn-2-probe" "turn-2-probe"
  submit_turn "turn-3-probe" "turn-3-probe"
}

show_result() {
  echo "[4/4] PASS: continuous chat regression check succeeded"
}

start_server
start_sse
run_flow
show_result
