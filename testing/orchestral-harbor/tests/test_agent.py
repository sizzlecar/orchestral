import json
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path, PurePosixPath
from unittest.mock import AsyncMock

from harbor.agents.installed.base import NonZeroAgentExitCodeError
from harbor.models.agent.context import AgentContext
from harbor.models.environment_type import EnvironmentType
from orchestral_harbor import Orchestral


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.binary = self.root / "orchestral"
        self.binary.write_bytes(b"\x7fELFfixture")
        self.agent = self.make_agent()

    def make_agent(self, **kwargs):
        return Orchestral(
            logs_dir=self.root / "logs",
            binary_path=str(self.binary),
            model_name="google/test-model",
            **kwargs,
        )

    def test_rejects_native_macos_binary_before_installation(self):
        self.binary.write_bytes(b"\xcf\xfa\xed\xfe")
        with self.assertRaisesRegex(ValueError, "Linux ELF"):
            self.make_agent()

    def test_unattended_config_and_state_are_outside_task_workspace(self):
        config = self.agent.native_config()
        self.assertFalse(config["agent"]["input_requests_enabled"])
        self.assertFalse(config["mcp"]["enabled"])
        self.assertFalse(config["skills"]["enabled"])
        self.assertFalse(config["tools"]["exec"]["sandboxed_execution_enabled"])
        self.assertTrue(config["tools"]["exec"]["allow_host_execution"])
        self.assertTrue(config["journal"]["root_dir"].startswith("/logs/agent/"))
        self.assertNotIn("system_prompt", config["agent"])

    def test_output_budget_is_explicit_and_validated(self):
        agent = self.make_agent(max_output_tokens=32768)
        self.assertEqual(
            agent.native_config()["providers"]["models"][0]["max_tokens"], 32768
        )
        for value in (0, -1, True, 1.5):
            with self.assertRaisesRegex(ValueError, "max_output_tokens"):
                self.make_agent(max_output_tokens=value)

    def test_instruction_is_one_literal_argument_including_shell_metacharacters(self):
        instruction = "--option 'quoted'\n$(touch /tmp/injected); `echo bad`"
        command = self.agent.command(instruction)
        tokens = shlex.split(command)
        separator = tokens.index("--")
        self.assertEqual(tokens[separator + 1], instruction)
        self.assertEqual(tokens[separator + 2], "<")

    def test_shell_wrapper_preserves_exit_status_logs_and_literal_prompt(self):
        self.agent.ROOT = PurePosixPath(self.root)
        self.agent.environment_logs_dir = PurePosixPath(self.root / "logs")
        (self.root / "logs").mkdir()
        (self.root / "approvals").write_text("y\n")
        self.binary.write_text(
            '#!/bin/sh\nprintf "%s\\n" "$@"\necho "provider failed" >&2\nexit 7\n'
        )
        self.binary.chmod(0o755)
        marker = self.root / "injected"
        prompt = f"literal $(touch {marker}); `echo surprise`"
        result = subprocess.run(
            ["bash", "-c", self.agent.command(prompt)],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        self.assertEqual(result.returncode, 7)
        self.assertIn("provider failed", result.stderr)
        self.assertIn(prompt, (self.root / "logs/stdout.txt").read_text())
        self.assertFalse(marker.exists())

    async def test_host_environment_rejected_before_any_mutation(self):
        environment = AsyncMock()
        environment.type = lambda: "host"
        with self.assertRaisesRegex(ValueError, "isolated"):
            await self.agent.setup(environment)
        environment.exec.assert_not_called()
        environment.upload_file.assert_not_called()

    async def test_install_uploads_binary_and_config_without_repository_or_credentials_in_logs(
        self,
    ):
        environment = AsyncMock()
        environment.type = lambda: EnvironmentType.DOCKER
        environment.default_user = None
        environment.exec.return_value.return_code = 0
        await self.agent.install(environment)
        targets = [call.args[1] for call in environment.upload_file.call_args_list]
        self.assertEqual(len(targets), 3)
        self.assertIn("/opt/orchestral-harbor/config.yaml", targets)
        self.assertTrue(
            all(path.startswith("/opt/orchestral-harbor/") for path in targets)
        )
        metadata = json.loads((self.root / "logs/adapter.json").read_text())
        self.assertNotIn(str(self.root), json.dumps(metadata))

    async def test_agent_failure_is_not_reported_as_success(self):
        environment = AsyncMock()
        environment.exec.return_value.return_code = 1
        environment.exec.return_value.stdout = ""
        environment.exec.return_value.stderr = "process failed"
        with self.assertRaises(NonZeroAgentExitCodeError):
            await self.agent.run("do the work", environment, AgentContext())

    async def test_run_keeps_context_empty_for_harbor_post_run_backfill(self):
        environment = AsyncMock()
        environment.exec.return_value.return_code = 0
        context = AgentContext()
        await self.agent.run("do the work", environment, context)
        self.assertTrue(context.is_empty())

    async def test_run_passes_proxy_settings_without_unrelated_environment(self):
        settings = {
            "HTTPS_PROXY": "http://upper-proxy.invalid:8080",
            "https_proxy": "http://lower-proxy.invalid:8080",
            "ALL_PROXY": "socks5://proxy.invalid:1080",
            "no_proxy": "localhost,127.0.0.1",
        }
        agent = self.make_agent(extra_env={**settings, "UNRELATED_SECRET": "private"})
        environment = AsyncMock()
        environment.exec.return_value.return_code = 0
        await agent.run("do the work", environment, AgentContext())
        forwarded = environment.exec.call_args.kwargs["env"]
        for name, value in settings.items():
            self.assertEqual(forwarded[name], value)
        self.assertNotIn("UNRELATED_SECRET", forwarded)

    def test_usage_deduplicates_tool_batches_and_preserves_unknown_fields(self):
        journal = self.root / "logs/journal"
        journal.mkdir(parents=True)

        def record(request, usage):
            return {
                "session_id": "s",
                "run_id": "r",
                "payload": {
                    "type": "tool_exchange_committed",
                    "request_id": request,
                    "usage": usage,
                },
            }

        path = journal / "session-s.json"
        known = record("one", {"input_tokens": 10, "output_tokens": 3})
        path.write_text(json.dumps([known, known, record("two", {"input_tokens": 5})]))
        context = AgentContext()
        self.agent.populate_context_post_run(context)
        self.assertEqual(context.n_input_tokens, 15)
        self.assertIsNone(context.n_output_tokens)
        self.assertIsNone(context.cost_usd)
        self.assertEqual(context.metadata["model_requests"], 2)


if __name__ == "__main__":
    unittest.main()
