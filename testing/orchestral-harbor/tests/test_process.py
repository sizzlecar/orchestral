import asyncio
import json
import os
import signal
import tempfile
import unittest
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from unittest.mock import AsyncMock

from orchestral_harbor.process import ContainerProcess


class DetachedTransport:
    """Like Docker exec: cancelling the awaiter does not stop the remote process."""

    def __init__(self):
        self.processes = []
        self.readers = []

    async def exec(self, command, **kwargs):
        process = await asyncio.create_subprocess_exec(
            "bash", "-c", command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.processes.append(process)
        reader = asyncio.create_task(process.communicate())
        self.readers.append(reader)
        stdout, stderr = await asyncio.shield(reader)
        return SimpleNamespace(
            return_code=process.returncode,
            stdout=stdout.decode(), stderr=stderr.decode(),
        )

    async def close(self):
        for process in self.processes:
            if process.returncode is None:
                process.kill()
        await asyncio.gather(*self.readers, return_exceptions=True)


@unittest.skipUnless(Path('/proc/self/stat').exists(), 'Linux process identity contract')
class ContainerProcessTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="process control ' ")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.process = ContainerProcess(PurePosixPath(self.root / "control ' directory"))
        self.environment = DetachedTransport()
        self.addAsyncCleanup(self.environment.close)
        self.heartbeat = self.root / 'heartbeat'
        self.child = self.root / 'child.py'
        self.child.write_text(
            'import ctypes, signal, sys, time\n'
            'from pathlib import Path\n'
            'ctypes.CDLL(None).prctl(15, b"worker ) spaced", 0, 0, 0)\n'
            'heartbeat = Path(sys.argv[1])\n'
            'signal.signal(signal.SIGINT, signal.SIG_IGN if "ignore" in sys.argv '
            'else lambda *_: sys.exit(0))\n'
            'while True:\n'
            ' heartbeat.write_text(str(time.time_ns()))\n'
            ' time.sleep(0.02)\n'
        )

    async def start(self, *extra):
        task = asyncio.create_task(self.environment.exec(self.process.launch(
            ['python3', str(self.child), str(self.heartbeat), *extra]
        )))
        async with asyncio.timeout(5):
            while not self.heartbeat.exists():
                await asyncio.sleep(0.01)
        return task

    async def test_cancel_stops_writes_before_returning_even_when_transport_detaches(self):
        launch = await self.start()
        launch.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await launch
        before = self.heartbeat.read_text()
        await asyncio.sleep(0.1)
        self.assertNotEqual(before, self.heartbeat.read_text())
        await self.process.cancel(self.environment)
        after = self.heartbeat.read_text()
        await asyncio.sleep(0.1)
        self.assertEqual(after, self.heartbeat.read_text())
        await self.process.cancel(self.environment)  # Idempotent after exit.

    async def test_cancellation_fences_a_late_launch(self):
        await self.process.cancel(self.environment)
        result = await self.environment.exec(self.process.launch(
            ['python3', str(self.child), str(self.heartbeat)]
        ))
        self.assertEqual(result.return_code, 130)
        self.assertFalse(self.heartbeat.exists())

    async def test_stale_pid_identity_does_not_signal_an_unrelated_process(self):
        launch = await self.start()
        pid_file = Path(self.process.control_dir) / 'pid'
        pid, birth, state = pid_file.read_text().split()
        pid_file.write_text(f'{pid} {int(birth) + 1} {state}\n')
        await self.process.cancel(self.environment)
        before = self.heartbeat.read_text()
        await asyncio.sleep(0.1)
        self.assertNotEqual(before, self.heartbeat.read_text())
        os.kill(int(pid), signal.SIGINT)
        await launch

    async def test_ignored_interrupt_fails_closed_after_bounded_cleanup(self):
        launch = await self.start('ignore')
        with self.assertRaisesRegex(RuntimeError, 'abort verification'):
            await self.process.cancel(self.environment)
        result = await asyncio.wait_for(launch, 2)
        self.assertNotEqual(result.return_code, 0)
        after = self.heartbeat.read_text()
        await asyncio.sleep(0.1)
        self.assertEqual(after, self.heartbeat.read_text())

    async def test_launch_preserves_literal_arguments_and_exit_status(self):
        payload = f"$(touch {self.root / 'injected'}); 'quoted'\n`echo bad`"
        result = await self.environment.exec(self.process.launch([
            'python3', '-c',
            'import json,sys; print(json.dumps(sys.argv[1:])); sys.exit(7)',
            payload,
        ]))
        self.assertEqual(result.return_code, 7)
        self.assertEqual(json.loads(result.stdout), [payload])
        self.assertFalse((self.root / 'injected').exists())


class CancellationBarrierTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.process = ContainerProcess(PurePosixPath('/control/invocation'))

    async def test_repeated_cancellation_waits_for_cleanup(self):
        environment = AsyncMock()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def stop(**kwargs):
            entered.set()
            await release.wait()
            return SimpleNamespace(return_code=0)

        environment.exec.side_effect = stop
        cleanup = asyncio.create_task(self.process.cancel(environment))
        await entered.wait()
        cleanup.cancel()
        await asyncio.sleep(0)
        cleanup.cancel()
        await asyncio.sleep(0)
        self.assertFalse(cleanup.done())
        release.set()
        await cleanup
        self.assertEqual(environment.exec.await_count, 1)

    async def test_cleanup_timeout_is_not_misclassified_as_a_scored_agent_timeout(self):
        environment = AsyncMock()
        environment.exec.side_effect = TimeoutError('transport stalled')
        with self.assertRaisesRegex(RuntimeError, 'abort verification'):
            await self.process.cancel(environment)
