# v0.3.0 接入与发布验收记录

日期：2026-09-10。状态：A01–A07 发布准备完成；尚未正式发布。对应[任务清单](user-onboarding-release-v0.3.0.md)。

## 验证结果

| 范围 | 环境与结果 | 证据与限制 |
| --- | --- | --- |
| 仓库清理 | 删除已确认的旧扩展、个人配置、过期示例与文档；运行时 `action/` 归入 `tools/` | 保留仍在用的 Workflow、协议和恢复兼容；用户原有 `.gitignore` / 产品文档改动保留 |
| 仓库检查 | 依赖边界、旧入口 gate、rustfmt、diff、cargo-machete 通过 | core/runtime 的生产依赖保持与插件实现解耦 |
| macOS 全量 Rust 测试 | Apple Silicon：771 通过、18 忽略 | `cargo test --locked --workspace --all-targets`；后续修改完成严格 clippy 与受影响的 CLI/TUI 回归检查 |
| Windows 全量 Rust 测试 | Windows 11 x64、Rust 1.91.0 MSVC：701 通过、16 忽略 | 同一全量命令；最终严格 clippy、workspace doctest 通过。52 组测试均无失败 |
| Linux 全量 Rust 测试 | 用户 Windows 主机内 Ubuntu 24.04 WSL：751 通过、18 忽略 | 包含 bubblewrap 沙箱执行；全量日志 `wsl-tests.log`。后续公共接入改动在 macOS/Windows 复查，Windows 专属改动在原生系统复查 |
| API / CLI 单元测试 | CLI 121 通过、1 忽略；OpenAI 适配器 10 通过 | 本地协议 fixture，包含 URL 规范化、凭据隔离、流式协议等 |
| 新用户接入 | macOS / Windows release 二进制各 15 个场景通过 | `onboarding_smoke.cjs`：URL/环境变量、模型发现、认证与密钥隔离、401/404、非 JSON、断连、只读 doctor、文件工具、跨进程恢复及实际 serve 启动 |
| 安装与替换 | macOS / Windows PowerShell 5.1 各 3 个场景通过 | 实际 v0.3.0 release 二进制归档；中文/空格/单引号路径、重复安装、错误哈希保留旧二进制；测试使用禁止修改 PATH 选项 |
| Windows 原生进程 | pipe UTF-8 与中文路径、ConPTY 会话、取消后子进程退出，3 项全部通过 | `core/orchestral-runtime/tests/windows_exec.rs`；Job Object 用于进程管理，不声称 OS 沙箱隔离 |
| Windows CLI/TUI | 42 个 CLI 集成场景通过 | 实际 ConPTY 启动、Unicode 输入、退出、审批/拒绝、取消、历史恢复、配置及工作区路径；复杂 IME 和跨终端人工验收仍为 B10 |
| WASM 与 SDK | WSL WASM check、workspace doctest；macOS SDK 示例实际执行通过 | 示例输出 `Hello from the provider-neutral Generic Agent.`；Windows SDK doctest 1 通过 |
| PWA 重新构建 | macOS Dioxus CLI 0.7.9 生成并更新嵌入式 bundle | WSL 构建受 cargo metadata 网络访问限制中止；此前磁盘不足的中止不计通过 |
| PWA 浏览器 | Chrome 390 px、320 px + Service Worker，两类 smoke 共四次通过 | 图片输入、稳定请求 ID、503 重试、审批、离线草稿、更新恢复 |
| Harbor | macOS：15 通过、6 项 Linux 专属测试跳过 | 本地锁定虚拟环境执行 Python unittest；Linux 进程边界测试由新增 CI job 执行，未把 macOS 跳过项计为通过 |
| 发布归档 | Apple Silicon tar.gz、Windows x64 ZIP 及各自 SHA-256 已生成并复核 | 均包含可运行二进制、许可、双语 README、CHANGELOG、RELEASING 和配置示例 |
| 官网交互 | Chrome：桌面、390/320 px、键盘 tabs、复制与发布状态通过 | `site_smoke.cjs`；已检查桌面和移动端截图 |
| 官网部署 | [orch.pandaailabs.com](https://orch.pandaailabs.com) 已上线 | Worker `orchestral-site`；网页与两个脚本 HTTP 200、MIME 正确、内容与仓库一致；未知路径 404；Windows PowerShell 下载脚本成功 |
| 原有服务 | `orchestral.pandaailabs.com` 保留为私有控制入口 | 没有修改其 Cloudflare Access 路由或策略，也没有改动主站 |
| CI 与发布工作流 | actionlint 1.7.12 静态检查通过 | 三系统检查、四架构打包、经浏览器测试的 PWA 和 Release draft 依赖已接通；尚未运行 GitHub Actions |

## 实机修复与能力边界

Windows 测试使用用户授权的 Tailscale 连接，在用户目录下独立的
`orchestral-release-check-20260910` 副本运行。连接配置、凭据和机器连接地址未写入仓库。
测试用 Node 与 Git 仅安装到测试目录，PATH 只在测试进程内设置。

实机暴露并修复了 MSVC 默认主线程栈不足、PowerShell 5.1 ZIP 分隔符和替换文件 null 参数、
中文输出编码、ConPTY 初始光标查询及主句柄生命周期、Windows 不支持 Unix 目录 fsync 等问题。
测试配置改为类型化 YAML 修改；ConPTY 测试保留进程环境并在子进程退出后关闭输入，避免把
测试夹具问题误判为恢复或网络故障。

原生 Windows 没有文件系统/网络沙箱。CLI 命令须经精确审批，以当前用户的 OS 权限执行；
Job Object 提供进程树取消与清理，启动后关联 Job，不能宣称原子隔离。沙箱请求在执行前拒绝。
本地 stdio MCP 使用 WSL；原生 Windows 可用 Streamable HTTP。

Windows 测试数低于 Unix：POSIX shell、Seatbelt/bubblewrap 和 Unix 进程组测试有明确的平台
条件。依赖 POSIX LF/Ctrl+J 和 VT 粘贴字节的 TUI 组合字符夹具保留在 Unix；另有原生 Windows
Unicode 输入与退出测试。没有据此声称完成中文 IME、多终端或性能人工验收。

`doctor` 默认只读且离线；`--check-connection` 仅查询 OpenAI 兼容 `/models`，不执行推理。
连接失败与超时、HTTP 状态、非 JSON 和发现协议问题有明确提示；DNS/TLS 暂归为连接问题，
企业 CA、代理与细分排障列入 B05。高级请求字段兼容开关列入 B04。

## 可复现检查

```sh
bash scripts/check_workspace.sh
bash scripts/check_agent_surface.sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets
cargo test --locked --workspace --doc
node scripts/onboarding_smoke.cjs PATH_TO_BINARY
node scripts/installer_smoke.cjs PATH_TO_BINARY
```

完整 PWA、Harbor 和四平台打包命令见 [RELEASING.md](../../RELEASING.md)。本轮日志保留在
执行机：macOS 的 `/tmp/orchestral-*-final.log`、`/tmp/orchestral-release-workspace-tests.log`，
Windows 测试副本的 `windows-full-final.log`、`windows-clippy-final.log`、`windows-doc-final.log`，
以及 WSL 测试副本的 `wsl-tests.log`。这些是本地证据，不进入发布包或 Git。

## 测试成本

本轮模型相关测试使用本地模拟 HTTP 服务，付费 API 调用为 **0**。
没有启动云端 GPU、没有运行长任务或大规模真实模型评测。真实模型、native Codex、外部 MCP
与性能 opt-in 测试保持跳过。将来需要真实模型时，优先使用现成自部署服务或低成本模型，
限制输入、输出、轮次与重试，并记录请求/token 用量；不把协议模拟结果当作模型质量评测。

## 归档与发布状态

本地归档位于 `target/release-artifacts/`：

- `orchestral-v0.3.0-aarch64-apple-darwin.tar.gz` 及 `.sha256`。
- `orchestral-v0.3.0-x86_64-pc-windows-msvc.zip` 及 `.sha256`。

Linux x64、macOS Intel 的正式归档尚未在对应发布 runner 生成；GitHub Actions 也尚未执行。
正式发布前需通过四平台工作流，审核 draft 资产。当前 macOS 归档未公证、Windows 二进制未签名。

官网诚实显示 v0.3.0 发布准备状态。线上安装脚本可下载，默认安装仍依赖尚未发布的 GitHub
资产；安装行为已用本地真实二进制归档验证。没有创建或推送 tag，没有发布 GitHub Release，
没有执行 crates.io publish。P1 后续清单独立保留，不计入本次发布准备完成条件。
