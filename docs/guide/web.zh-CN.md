# 浏览器与手机控制

[返回 README](../../README.zh-CN.md)


`orchestral serve` 会启动与 TUI 相同的 Agent Host，并提供内置、可安装的移动 Web App。
手机只是控制端，不会再启动一套 Agent Runtime；模型、Skill、MCP、工作区权限、审批和
Journal 都仍由 Host 持有。

在本机浏览器开发时可直接运行：

```bash
orchestral serve --pair --backend google --model gemini-2.5-flash -C /path/to/workspace
```

手机访问时，应通过受信任的反向代理或私网 Relay 终止 HTTPS，并把浏览器实际访问地址告诉
Host：

```bash
orchestral serve --pair \
  --public-url https://agent.example.com \
  --backend google --model gemini-2.5-flash \
  -C /path/to/workspace
```

扫描终端中的二维码即可配对。URL fragment 中是一次性、短时有效的配对密钥；领取完成后，
浏览器保存设备凭据，Host 只持久化它的摘要。PWA 可以新建或继续 Session，按持久事件游标
实时恢复 Run，展示有界的 Tool/文件证据与进度，处理输入和审批，发送 Steer、取消任务，
以及查看和撤销已配对设备。Service Worker 不缓存 API 响应、凭据或对话记录。

默认只监听本机回环地址。可信局域网临时测试可以显式使用
`--listen 0.0.0.0:8765 --public-url http://HOST:8765 --allow-insecure-http`，但安装、
Service Worker、通知等 PWA 能力通常要求受信任 HTTPS。Orchestral 不会静默把 Host 暴露到
公网，也不会上传 Agent 状态；HTTPS Proxy/Relay 是显式部署选择。Unix 上设备和 Session
元数据默认保存在 `~/.config/orchestral/remote-control.json`。
