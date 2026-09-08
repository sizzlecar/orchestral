#!/bin/bash
# Keep Codex daemon maintenance separate from Orchestral Host deployment.
# One-shot maintenance command: never supervise this script with launchd KeepAlive.
# It exits after the daemon command returns; KeepAlive would repeat that command
# and, for restart, repeatedly disconnect every client of the shared daemon.
set -euo pipefail

action="${1:-check}"
case "$action" in
  check|start|restart) ;;
  *) echo "Usage: $0 [check|start|restart]" >&2; exit 2 ;;
esac

# Mutating commands are manual-only. Check before resolving or invoking Codex so
# a launchd restart loop cannot affect the daemon, even with a valid executable.
if [[ "$action" != check ]]; then
  if [[ ! -t 0 || ! -t 1 ]]; then
    echo "Refusing daemon $action: run manually in an interactive terminal; background maintenance is disabled." >&2
    exit 2
  fi
  echo "Daemon $action may disconnect active Codex clients." >&2
  read -r -p "Type '$action' to continue: " confirmation || exit 2
  if [[ "$confirmation" != "$action" ]]; then
    echo "Cancelled; daemon unchanged." >&2
    exit 2
  fi
fi

codex_bin="${ORCHESTRAL_CODEX_BIN:-$HOME/.local/bin/codex}"
if [[ ! -x "$codex_bin" ]]; then
  echo "Codex executable missing: $codex_bin; set ORCHESTRAL_CODEX_BIN." >&2
  exit 1
fi

# Raise only the soft limit; fail before touching the daemon if disallowed.
current_limit="$(ulimit -S -n)"
if [[ "$current_limit" != unlimited ]] && (( current_limit < 8192 )); then
  ulimit -S -n 8192
fi
echo "Codex launcher maxfiles: soft=$(ulimit -S -n) hard=$(ulimit -H -n)"
if [[ "$action" == check ]]; then
  exec "$codex_bin" app-server daemon version
fi

# start does not change the limits of an already running daemon.
exec "$codex_bin" app-server daemon "$action"
