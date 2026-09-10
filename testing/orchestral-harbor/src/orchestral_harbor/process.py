"""Cancellation ownership for a native process in a Linux environment.

Cancelling an environment.exec awaiter may only disconnect its transport. A
separate control operation must stop the owned process before verification can
start. PID birth times prevent a late cancellation from signalling a reused PID.
"""

import asyncio
import shlex
from dataclasses import dataclass
from pathlib import PurePosixPath


# /proc comm may contain spaces or parentheses. Fields after its final ') '
# start at field 3 (state); starttime is field 22.
_IDENTITY = """
process_identity() {
    IFS= read -r proc_stat < "/proc/$1/stat" 2>/dev/null || return 1
    proc_stat=${proc_stat##*) }
    set -f
    set -- $proc_stat
    [ "$#" -ge 20 ] || return 1
    proc_state=$1
    shift 19
    printf '%s %s\\n' "$1" "$proc_state"
}
"""


@dataclass(frozen=True)
class ContainerProcess:
    """One invocation's private control files, outside the task workspace."""

    control_dir: PurePosixPath

    def launch(self, args: list[str]) -> str:
        control = shlex.quote(str(self.control_dir))
        script = f"""{_IDENTITY}
umask 077
mkdir -p {control} || exit 125
[ ! -f {control}/cancelled ] || exit 130
identity=$(process_identity "$$") || exit 125
printf '%s %s\\n' "$$" "$identity" > {control}/pid.tmp || exit 125
mv {control}/pid.tmp {control}/pid || exit 125
[ ! -f {control}/cancelled ] || exit 130
exec "$@"
"""
        return shlex.join(["sh", "-c", script, "orchestral-launch", *args])

    def cancellation_command(self) -> str:
        control = shlex.quote(str(self.control_dir))
        # A cancellation marker also fences an exec that has not started yet.
        # SIGINT enters the CLI's existing Run cancellation path, including
        # cleanup of its tool processes. Forced termination fails closed:
        # verification must not run if descendant cleanup is unconfirmed.
        return f"""{_IDENTITY}
umask 077
mkdir -p {control} || exit 125
: > {control}/cancelled || exit 125
[ -f {control}/pid ] || exit 0
read -r owned_pid owned_birth ignored < {control}/pid || exit 125
case "$owned_pid:$owned_birth" in *[!0-9:]*|:*|*:) exit 125;; esac
current=$(process_identity "$owned_pid") || exit 0
[ "$current" != "$owned_birth Z" ] || exit 0
[ "${{current% *}}" = "$owned_birth" ] || exit 0
if ! kill -INT "$owned_pid" 2>/dev/null; then
    current=$(process_identity "$owned_pid") || exit 0
    [ "${{current% *}}" != "$owned_birth" ] && exit 0
    exit 125
fi
attempt=0
while [ "$attempt" -lt 50 ]; do
    current=$(process_identity "$owned_pid") || exit 0
    [ "$current" != "$owned_birth Z" ] || exit 0
    [ "${{current% *}}" = "$owned_birth" ] || exit 0
    sleep 0.1
    attempt=$((attempt + 1))
done
kill -KILL "$owned_pid" 2>/dev/null || true
echo 'Native Agent did not confirm cancellation; verification must not proceed' >&2
exit 125
"""

    async def cancel(self, environment) -> None:
        """Finish bounded cleanup even if the caller is cancelled repeatedly."""

        async def stop():
            try:
                result = await asyncio.wait_for(
                    environment.exec(command=self.cancellation_command(), timeout_sec=10),
                    timeout=15,
                )
            except Exception as error:
                # Harbor interprets TimeoutError as an ordinary scored timeout.
                # Cleanup failure must instead abort the verification phase.
                raise RuntimeError(
                    "Native Agent cancellation failed; abort verification"
                ) from error
            if result.return_code != 0:
                raise RuntimeError(
                    "Native Agent cancellation was not confirmed; abort verification"
                )

        cleanup = asyncio.create_task(stop())
        while True:
            try:
                await asyncio.shield(cleanup)
                return
            except asyncio.CancelledError:
                if cleanup.cancelled():
                    raise RuntimeError("Native Agent cancellation cleanup was interrupted")
