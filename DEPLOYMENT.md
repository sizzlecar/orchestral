# Orchestral 当前部署说明

> 状态快照：2026-09-03，Asia/Shanghai。本文记录当前这台 macOS Host 的实际部署，
> 不保存任何密钥、Cloudflare Tunnel 凭据、Access Token 或模型 API Key。

## 1. 部署拓扑

```text
浏览器 / PWA
  |
  | HTTPS + Cloudflare Access
  v
orchestral.pandaailabs.com
  |-- /api/v1/attachments* -----------------> Cloudflare Worker
  |                                              |
  |                                              v
  |                                         R2: pandavault-transfer
  |
  `-- 其余请求 -> Cloudflare Tunnel -> 127.0.0.1:8765
                                      |
                                      v
                              Orchestral Host
                                |-- Generic Agent
                                |-- Agent connectors
                                `-- R2 internal API

orchestral-files.pandaailabs.com -> Cloudflare Worker -> R2
```

Host 只监听本机回环地址，不直接暴露局域网或公网端口。Cloudflare Tunnel 负责把
`orchestral.pandaailabs.com` 的非附件流量转发到 Host。附件上传、签名和下载由 Worker
直接访问 R2，不经过 Host 的公网链路；Host 生成的 Artifact 则使用内部 Token 调用同一个
Worker。

## 2. 当前运行实例

### Orchestral Host

| 项目 | 当前值 |
| --- | --- |
| LaunchAgent | `com.sizzlecar.orchestral.remote` |
| plist | `~/Library/LaunchAgents/com.sizzlecar.orchestral.remote.plist` |
| binary | `./target/release/orchestral` |
| working directory | `/Users/chejinxuan/rust_ws/orchestral` |
| listen | `127.0.0.1:8765` |
| public URL | `https://orchestral.pandaailabs.com` |
| model backend | `google` |
| default model | `gemini-3.1-pro-preview` |
| authentication | Cloudflare Access JWT，由 Host 再次校验 issuer、audience 和允许的身份 |
| stdout/stderr | `~/Library/Logs/orchestral-remote.log` |
| restart policy | `RunAtLoad=true`、`KeepAlive=true` |

快照时运行的是 `orchestral 0.2.0`，代码为 `1ff8b7a`。进程 PID 只是诊断信息，会在重启后
变化，不应写入脚本。

### Cloudflare Tunnel

| 项目 | 当前值 |
| --- | --- |
| LaunchAgent | `com.cloudflare.cloudflared.orchestral` |
| plist | `~/Library/LaunchAgents/com.cloudflare.cloudflared.orchestral.plist` |
| binary | `/opt/homebrew/bin/cloudflared` |
| config | `~/.cloudflared/orchestral.yml` |
| ingress | `orchestral.pandaailabs.com -> http://127.0.0.1:8765` |
| metrics | `127.0.0.1:20242` |
| stdout/stderr | `~/Library/Logs/orchestral-cloudflared.log` |
| restart policy | `RunAtLoad=true`、`KeepAlive=true` |

当前 cloudflared 版本为 `2026.7.3`，以 QUIC 建立多路边缘连接。

### 附件 Worker 和 R2

Worker 源码位于 `deploy/cloudflare/orchestral-attachments/`，配置在
`wrangler.jsonc`：

- Worker：`orchestral-attachments`
- 应用域名路由：`orchestral.pandaailabs.com/api/v1/attachments*`
- 文件域名：`orchestral-files.pandaailabs.com`
- R2 binding：`ATTACHMENT_BUCKET`
- R2 bucket：`pandavault-transfer`
- 单文件上限：64 MiB

Worker 需要 `ATTACHMENT_SIGNING_SECRET`、`INTERNAL_API_TOKEN`、`ALLOWED_EMAILS` 等
secret/配置。只允许通过 Wrangler/Cloudflare 管理，不要把值写进本文、plist 或 Git。

Host 通过以下环境变量找到 R2 内部接口：

- `ORCHESTRAL_ARTIFACT_R2_INTERNAL_URL`
- `ORCHESTRAL_ARTIFACT_R2_KEYCHAIN_SERVICE`
- `ORCHESTRAL_ARTIFACT_R2_KEYCHAIN_ACCOUNT`

真正的内部 Token 保存在 macOS Keychain。不要用 `security find-generic-password -w` 做健康
检查，也不要把 LaunchAgent 环境或 Keychain 输出复制到工单/日志。

### Codex shared daemon

Codex connector 的实时 observe/steer 依赖共享 app-server socket：

```text
~/.codex/app-server-control/app-server-control.sock
```

快照时 socket 存在，`codex app-server proxy` 正在运行，多个
`codex --remote unix://` 客户端共享该 owner。它不是上述两个 LaunchAgent 的一部分，重启
Orchestral Host 不应顺带启动、停止或替换 Codex daemon。在线会话存在时不要运行
`codex app-server daemon start/stop` 或 `codex remote-control start/stop`，否则可能切断原会话
writer。

要在终端新建一个可由 PWA 实时查看、发送消息和 steer 的 Codex 会话，进入目标目录后连接
shared daemon：

```bash
cd /目标目录
codex --remote unix://
```

也可以不切换 shell 当前目录，显式传入工作目录：

```bash
codex --remote unix:// -C ~/rust_ws/ferrum-infer-rs
```

`unix://` 在这里表示连接当前 `CODEX_HOME` 下的默认 app-server control socket；使用默认
`CODEX_HOME` 时就是：

```text
~/.codex/app-server-control/app-server-control.sock
```

只要该 socket 和 `codex app-server proxy` 已存在，就不需要再次执行
`codex app-server daemon start`。普通的 `codex` 会创建 embedded writer 会话，不具备这个
shared owner 的跨进程实时控制能力；需要 PWA 实时控制时必须使用 `--remote unix://`。

## 3. 构建和发布 Host/PWA

PWA 产物被 `include_dir!` 编译进 release binary，因此顺序必须是先构建 Web，再构建 CLI。
只重建 Rust binary 而不刷新 `web/orchestral-web/dist/`，会继续发布旧 PWA。

```bash
cd /Users/chejinxuan/rust_ws/orchestral

# 最低限度的发布前验证
cargo test -p orchestral-web --features web
cargo test -p orchestral-cli
cargo check --workspace --all-targets

# 生成带指纹的 JS/WASM/CSS，并同步到 web/orchestral-web/dist
./scripts/build_web.sh

# 把刚生成的 PWA 嵌入 release binary
cargo build --release --locked -p orchestral-cli

# 让 LaunchAgent 替换当前 Host 进程
launchctl kickstart -k "gui/$(id -u)/com.sizzlecar.orchestral.remote"
```

`scripts/build_web.sh` 固定要求 Dioxus CLI `0.7.9`，并会清理 Dioxus 的旧 release Web
输出，避免一个 binary 同时嵌入多代指纹资源。发布后检查：

```bash
curl -sS -o /dev/null -w 'status=%{http_code} time=%{time_total}s\n' \
  http://127.0.0.1:8765/

launchctl print "gui/$(id -u)/com.sizzlecar.orchestral.remote" | \
  rg 'state =|pid =|runs =|last exit|stdout path|stderr path'

tail -n 80 ~/Library/Logs/orchestral-remote.log
```

本地根路径应返回 `200`。未携带 Cloudflare Access 登录态访问公网根路径通常返回 `302`，
这是跳转登录，不是 Host 故障。

如果修改了 plist，`kickstart` 不会重新读取磁盘上的 plist；先校验，再重新 bootstrap：

```bash
plist_path="$HOME/Library/LaunchAgents/com.sizzlecar.orchestral.remote.plist"
plutil -lint "$plist_path"
launchctl bootout "gui/$(id -u)" "$plist_path"
launchctl bootstrap "gui/$(id -u)" "$plist_path"
```

不要在普通代码发布中改 plist，也不要为了重启 Host 操作 Codex daemon。

## 4. 发布附件 Worker

Host/PWA 发布和 Worker 发布彼此独立。只有附件协议、路由或 Worker 实现变化时才部署 Worker：

```bash
cd /Users/chejinxuan/rust_ws/orchestral/deploy/cloudflare/orchestral-attachments
npm ci
npm run check
npm test
npm run deploy
```

部署前可用 `npx wrangler secret list` 检查 secret 名称是否齐全；该命令不应输出 secret 值。
实时观察 Worker：

```bash
cd /Users/chejinxuan/rust_ws/orchestral/deploy/cloudflare/orchestral-attachments
npx wrangler tail --format pretty
```

## 5. 日志位置和特性

### Host 日志

```text
~/Library/Logs/orchestral-remote.log
```

注意：

- tracing 日志时间为 UTC，行尾/时间中的 `Z` 表示 UTC；上海时间需加 8 小时。
- stdout 和 stderr 写入同一个文件。
- LaunchAgent 重启会继续追加旧文件，目前没有自动轮转。
- 部分早期 `eprintln!` 恢复错误没有时间戳。不能用简单字符串筛选把它们判定为“刚发生”。
- `SSE stream closed ... close_reason="client_disconnected"` 通常表示切页、刷新、浏览器休眠或
  客户端主动换流，不等于服务端崩溃。

实时查看：

```bash
tail -F ~/Library/Logs/orchestral-remote.log
```

只看最近一次 Host ready 之后的日志：

```bash
host_log="$HOME/Library/Logs/orchestral-remote.log"
ready_line=$(rg -n 'Orchestral Host ready' "$host_log" | tail -1 | cut -d: -f1)
tail -n "+${ready_line:-1}" "$host_log"
```

最近一次启动阶段的恢复审计发生在 `Host ready` 之前。如需检查启动恢复，应额外向前查看：

```bash
rg -n 'durable Agent Run recovery audit|Orchestral Host listening|Orchestral Host ready' \
  ~/Library/Logs/orchestral-remote.log | tail -n 12
```

常用筛选：

```bash
host_log="$HOME/Library/Logs/orchestral-remote.log"

# HTTP 5xx、恢复、冲突和 Provider/R2 异常
rg 'status=5[0-9]{2}| WARN | ERROR |recovery failed|RunIdConflict|DuplicateConflict|ProviderUnavailable' \
  "$host_log" | tail -n 120

# 消息是否被 Host 接收一次，以及 command_id 是否重复
rg 'Agent session input accepted|Agent command acknowledged|duplicate=' \
  "$host_log" | tail -n 120

# SSE 生命周期；同一用户多设备/多页面时并存多条流是正常的
rg 'SSE stream (opened|closed)' "$host_log" | tail -n 120

# 会话详情接口耗时
rg 'HTTP request completed.*route=/api/v1/agent-session ' "$host_log" | tail -n 80
```

`response_ready_ms` 是 Host 从收到请求到响应可返回的时间，不包含浏览器下载/渲染。判断慢
请求时应同时保留 `request_id`、`route`、`session_id`、`run_id` 和 `cf_ray`，方便串联 Host、
Tunnel 和 Worker 日志。

Codex 原生 RPC 超过 1 秒会记录 `slow Codex RPC request`，包含 `rpc_method`、`rpc_id`、
`elapsed_ms`、`write_ms` 和 `succeeded`，不记录请求正文。`write_ms` 包含本机发送锁等待及
写入耗时；总耗时减去它是等待原生响应的时间。HTTP 请求内的 RPC 会继承 `request_id`，
可据此区分请求慢在 Host 发送还是 Codex 响应。Run 通知缺口日志中的 `skipped` 统计实时
通知，不等同于丢失的聊天消息条数。历史校准与实时通知交替推进，每个 Run 最多一项校准
在途，通知安静后才轮询。轮询先读取会话元数据，运行中不反复读取完整历史；空闲或状态
未知时保留完整历史校准，命令发送前仍核验精确 turn。`include_turns` 区分元数据与完整
历史请求。通知有缺口时，完整输出恢复成功后才能提交最终交付。

### Cloudflare Tunnel 日志

```text
~/Library/Logs/orchestral-cloudflared.log
```

实时查看和连接筛选：

```bash
tail -F ~/Library/Logs/orchestral-cloudflared.log

rg 'Registered tunnel connection|Connection terminated|Serve tunnel error|ERR|WRN' \
  ~/Library/Logs/orchestral-cloudflared.log | tail -n 120
```

单条 QUIC connection timeout 后很快出现相同 `connIndex` 的
`Registered tunnel connection`，通常是边缘连接自愈；只有全部连接持续失败、公网请求同时
超时，才应判断 Tunnel 不可用。2026-09-03 10:25（上海时间）观察到一次短暂 QUIC 抖动，
连接在数秒内重新注册，LaunchAgent 没有退出。

当前没有配置日志轮转。文件持续增长时应新增 macOS 原生日志轮转策略；在此之前不要直接
删除运行中的日志文件，至少先保留问题时间窗和 request/run/session 标识。

## 6. 快速故障定位

### 公网页面打不开

1. `curl http://127.0.0.1:8765/`：不是 `200`，先查 Host/LaunchAgent。
2. 本地正常但公网异常：查 cloudflared state、Tunnel 日志和 Cloudflare Access。
3. 公网 `302`：通常是 Access 登录跳转；完成登录后再判断。

### PWA 仍运行旧资源

1. 确认 `./scripts/build_web.sh` 在 release 构建之前执行。
2. 检查 `web/orchestral-web/dist/index.html` 中当前 JS 指纹。
3. 确认 `target/release/orchestral` 的修改时间晚于 dist。
4. 检查 Host 已 kickstart，随后普通刷新 PWA；Service Worker 会按新的 bundle id 换代缓存。

### 页面一直“自动恢复”

1. 用 `run_id` 搜索 `orchestral-remote.log`。
2. 区分历史无时间戳行和当前进程的新行，观察计数是否仍增长。
3. 查 `RunIdConflict`、`ProviderUnavailable`、`restored Agent Run continuity` 和 durable recovery
   audit。
4. Codex 会话再确认 shared socket 存在，但不要通过重启 daemon 做诊断。

### 会话详情很慢

先看 `route=/api/v1/agent-session` 的 `response_ready_ms`。2026-09-02 的一次观察中，多数请求
为约 `94–128 ms`，出现过一次 `4394 ms` 离群值；当前日志还没有 Provider read、timeline
merge、journal read 的分阶段耗时，单凭总耗时不能确定慢在何处。

### 附件上传或下载失败

1. 浏览器上传 API 属于 Worker 路由，不应进入本地 Host。
2. 查 Worker invocation log，并保留响应中的 request id。
3. Host 生成文件失败时，查 `ProviderUnavailable` 和
   `orchestral-files.pandaailabs.com/v1/internal/blobs`。
4. 校验 Worker secret/binding 的名称，不打印值；确认 R2 对象和 SHA-256 一致。

## 7. 当前已知运维缺口

- Host 与 cloudflared 日志都没有自动轮转。
- Host 会话详情只有请求总耗时，缺少 Provider/journal/merge 分阶段 tracing。
- LaunchAgent 直接运行工作区内的 `target/release/orchestral`，没有独立版本目录和原子回滚
  symlink；发布中途失败时必须保证旧进程仍在运行，只有完整构建并验证后才能 kickstart。
- Codex shared daemon 独立于 Orchestral LaunchAgent，当前部署没有统一生命周期管理；这是刻意
  隔离，避免 Host 发布影响正在运行的 Codex 会话。
