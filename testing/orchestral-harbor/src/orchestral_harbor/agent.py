"""Native CLI adapter; Harbor alone supplies tasks, deadlines and verification."""

import asyncio
import hashlib
import json
import shlex
import tempfile
from pathlib import Path, PurePosixPath
from typing import ClassVar
from uuid import uuid4

import yaml
from harbor.agents.installed.base import BaseInstalledAgent
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext
from harbor.models.environment_type import EnvironmentType

from .process import ContainerProcess


class Orchestral(BaseInstalledAgent):
    """Install a prebuilt Linux executable, without building in each trial.

    Credentials are supplied through provider environment variables or an explicit
    local credential_file. Neither the credential nor the host repository is
    included in the agent logs or uploaded as a task workspace.
    """

    ROOT = PurePosixPath("/opt/orchestral-harbor")
    PROVIDERS: ClassVar[dict[str, tuple[str, str, str | None]]] = {
        "google": ("gemini", "GOOGLE_API_KEY", None),
        "openai": ("openai", "OPENAI_API_KEY", None),
        "openrouter": (
            "openrouter",
            "OPENROUTER_API_KEY",
            "https://openrouter.ai/api/v1",
        ),
        "deepseek": ("deepseek", "DEEPSEEK_API_KEY", "https://api.deepseek.com"),
    }

    def __init__(
        self,
        *args,
        binary_path: str,
        credential_file: str | None = None,
        max_model_steps: int = 128,
        max_tool_calls: int = 512,
        max_output_tokens: int = 8192,
        temperature: float = 0,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.binary_path = Path(binary_path).expanduser().resolve(strict=True)
        with self.binary_path.open("rb") as binary:
            if binary.read(4) != b"\x7fELF":
                raise ValueError(
                    "binary_path must be a Linux ELF executable, not a macOS binary"
                )
        with self.binary_path.open("rb") as binary:
            self.binary_digest = hashlib.file_digest(binary, "sha256").hexdigest()
        self.credential_file = (
            Path(credential_file).expanduser().resolve(strict=True)
            if credential_file
            else None
        )
        if not self.model_name or "/" not in self.model_name:
            raise ValueError(
                "model must be provider/model, for example google/gemini-3.1-pro-preview"
            )
        self.backend, self.model = self.model_name.split("/", 1)
        if self.backend not in self.PROVIDERS or not self.model.strip():
            raise ValueError(f"Supported providers: {', '.join(self.PROVIDERS)}")
        if self.credential_file and self.backend != "google":
            raise ValueError(
                "credential_file is supported only for Google; use provider key env otherwise"
            )
        if self.mcp_servers or self.skills_dir:
            raise ValueError(
                "This adapter does not support task-provided MCP servers or skills"
            )
        for name, value in (
            ("max_model_steps", max_model_steps),
            ("max_tool_calls", max_tool_calls),
            ("max_output_tokens", max_output_tokens),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if not 0 <= temperature <= 2:
            raise ValueError("temperature must be between 0 and 2")
        self.max_model_steps = max_model_steps
        self.max_tool_calls = max_tool_calls
        self.max_output_tokens = max_output_tokens
        self.temperature = temperature

    @staticmethod
    def name() -> str:
        return "orchestral"

    def get_version_command(self) -> str:
        return f"{self.ROOT}/orchestral --version"

    def native_config(self) -> dict:
        """Keep application state outside the task cwd and expose no input tool."""
        kind, key, endpoint = self.PROVIDERS[self.backend]
        backend = {"name": self.backend, "kind": kind, "api_key_env": key}
        if endpoint:
            backend["endpoint"] = endpoint
        return {
            "version": 1,
            "agent": {
                "backend": self.backend,
                "model_profile": "harbor",
                "input_requests_enabled": False,
                "max_model_steps": self.max_model_steps,
                "max_tool_calls": self.max_tool_calls,
            },
            "providers": {
                "backends": [backend],
                "models": [
                    {
                        "name": "harbor",
                        "backend": self.backend,
                        "model": self.model,
                        "temperature": self.temperature,
                        "max_tokens": self.max_output_tokens,
                    }
                ],
            },
            "tools": {
                "exec": {
                    "enabled": True,
                    "allow_host_execution": True,
                    "sandboxed_execution_enabled": False,
                }
            },
            "mcp": {"enabled": False},
            "skills": {"enabled": False},
            "journal": {
                "backend": "filesystem",
                "root_dir": str(self.environment_logs_dir / "journal"),
            },
            "artifacts": {
                "backend": "filesystem",
                "root_dir": str(self.environment_logs_dir / "artifacts"),
            },
        }

    async def setup(self, environment: BaseEnvironment) -> None:
        if environment.type() != EnvironmentType.DOCKER:
            raise ValueError(
                "Orchestral benchmark approval requires an isolated Docker environment"
            )
        await super().setup(environment)

    async def install(self, environment: BaseEnvironment) -> None:
        # Fail before any model call if the platform or artifact is incompatible.
        await self.exec_as_root(environment, f"mkdir -p {self.ROOT}")
        await environment.upload_file(self.binary_path, str(self.ROOT / "orchestral"))
        await self.exec_as_root(environment, f"chmod 755 {self.ROOT}/orchestral")
        await self.exec_as_agent(environment, self.get_version_command())
        await self._upload_config_text(
            environment,
            content=yaml.safe_dump(self.native_config()),
            remote_path=str(self.ROOT / "config.yaml"),
            filename="config.yaml",
        )
        if self.credential_file:
            await self._upload_agent_owned_file(
                environment, self.credential_file, str(self.ROOT / "credential.json")
            )
            await self.exec_as_root(
                environment, f"chmod 600 {self.ROOT}/credential.json"
            )
        # Input requests are disabled at the capability boundary. These lines
        # approve command escalation inside the disposable task container only.
        with tempfile.TemporaryDirectory() as directory:
            approvals = Path(directory) / "approvals"
            approvals.write_text("y\n" * self.max_tool_calls)
            await environment.upload_file(approvals, str(self.ROOT / "approvals"))
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        (self.logs_dir / "adapter.json").write_text(
            json.dumps(
                {
                    "binary_sha256": self.binary_digest,
                    "model": self.model_name,
                    "config": self.native_config(),
                    "approval_policy": "approve command escalation inside Harbor container",
                },
                indent=2,
            )
            + "\n"
        )

    def command(
        self, instruction: str, process: ContainerProcess | None = None
    ) -> str:
        args = [
            str(self.ROOT / "orchestral"),
            "--config",
            str(self.ROOT / "config.yaml"),
            "--no-mcp",
            "--no-skills",
        ]
        if self.credential_file:
            args += ["--credential-file", str(self.ROOT / "credential.json")]
        args += ["--", instruction]
        launch = process.launch(args) if process else shlex.join(args)
        stderr = shlex.quote(str(self.environment_logs_dir / "stderr.txt"))
        return (
            f"{launch} < {shlex.quote(str(self.ROOT / 'approvals'))} "
            f"> {shlex.quote(str(self.environment_logs_dir / 'stdout.txt'))} "
            f"2> {stderr}; orchestral_exit=$?; "
            f'if [ "$orchestral_exit" -ne 0 ]; then tail -c 8192 {stderr} >&2; fi; '
            'exit "$orchestral_exit"'
        )

    def provider_env(self) -> dict[str, str]:
        names = [
            self.PROVIDERS[self.backend][1],
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
        ]
        if self.backend == "google":
            names += [
                "GOOGLE_CLOUD_PROJECT",
                "GOOGLE_CLOUD_LOCATION",
                "GOOGLE_VERTEX_PROJECT",
                "GOOGLE_VERTEX_LOCATION",
            ]
        return {
            name: value for name in names if (value := self._get_env(name)) is not None
        }

    async def run(
        self, instruction: str, environment: BaseEnvironment, context: AgentContext
    ) -> None:
        # Use the environment's declared cwd; do not inherit the host repository.
        # Harbor 0.22 only backfills empty contexts after downloading logs.
        # Leave this empty so usage is populated on both success and timeout.
        process = ContainerProcess(self.ROOT / "runs" / uuid4().hex)
        try:
            await self.exec_as_agent(
                environment,
                self.command(instruction, process),
                env=self.provider_env(),
            )
        except asyncio.CancelledError:
            await process.cancel(environment)
            raise

    def populate_context_post_run(self, context: AgentContext) -> None:
        """Count committed requests and recorded retry usage without duplicates."""
        observations = {}
        for path in sorted((self.logs_dir / "journal").glob("session-*.json")):
            for record in json.loads(path.read_text()):
                payload = record.get("payload", {})
                if payload.get("type") not in (
                    "tool_exchange_committed",
                    "run_output_committed",
                ):
                    continue
                key = (record["session_id"], record["run_id"], payload["request_id"])
                observations.setdefault(key, payload.get("usage") or {})
        retries = {}
        for path in sorted((self.logs_dir / "journal").glob("generic-checkpoint-*.json")):
            for record in json.loads(path.read_text()).get("records", []):
                payload = record.get("payload", {})
                if payload.get("type") != "model_retry_scheduled":
                    continue
                key = (record["run_id"], payload["request_id"], payload["retry_number"])
                retries.setdefault(key, payload.get("observed_usage") or {})
        for field, target in (
            ("input_tokens", "n_input_tokens"),
            ("output_tokens", "n_output_tokens"),
        ):
            values = [
                usage.get(field)
                for usage in (*observations.values(), *retries.values())
            ]
            if values and all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in values
            ):
                setattr(context, target, sum(values))
        context.metadata = {
            **(context.metadata or {}),
            "binary_sha256": self.binary_digest,
            "usage_scope": "committed model requests and recorded retry attempts",
            "model_requests": len(observations) + len(retries),
            "committed_model_requests": len(observations),
            "retried_model_requests": len(retries),
        }
