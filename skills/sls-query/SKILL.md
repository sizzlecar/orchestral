---
name: sls-query
description: "Query Alibaba Cloud SLS (Simple Log Service) for application logs from test or production environments. Use when the user asks to check logs, search for errors, trace requests, or investigate incidents."
compatibility: "Requires Python 3.8+ and aliyun-log-python-sdk."
metadata:
  author: orchestral
  version: "0.1.0"
---

# SLS Log Query

All credentials and configuration are already set as environment variables. Do not ask the user for any credentials or configuration. Execute the command directly.

## Command template

Use `python3` (system Python, NOT .venv). The SDK is already installed globally.

**Step 1: Always start with summary mode** to see error distribution:
```
python3 scripts/sls_query.py --project PROJECT --logstore LOGSTORE --query "QUERY" --from-time="TIME_RANGE" --limit 100 --output summary
```

**Step 2: Query specific entries** if the user wants details:
```
python3 scripts/sls_query.py --project PROJECT --logstore LOGSTORE --query "QUERY" --from-time="TIME_RANGE" --limit 10
```

Output options:
- `--output summary` — grouped error counts (always use first)
- `--output text` — individual entries with truncated messages (default 300 chars)
- `--max-chars 500` — adjust truncation length
- `--max-chars 0` — no truncation (full stack traces, use sparingly)

Replace the placeholders as follows:

**PROJECT** — derive from the environment the user mentions:
- 生产/prod/线上 → use the literal string `$SLS_PROD_PROJECT` (the shell resolves it)
- 测试/test → use the literal string `$SLS_TEST_PROJECT` (the shell resolves it)

**LOGSTORE** — derive from the service name the user mentions, using pattern `quan-SERVICE-logstore-ENV`:
- SERVICE = what the user says (message → message-backend, ai → ai-provider, auth → auth-provider, user → user-provider, cms → cms-provider, content → content-provider)
- ENV = prd (production) or test

**QUERY** — the log level field is `info` (not `level`):
- Error logs: `info: ERROR`
- Search keyword: `NullPointerException`
- Combined: `info: ERROR AND timeout`

**TIME_RANGE**: `-15m`, `-1h`, `-24h`, or `2024-01-15 10:00:00`

## Example

User says: "查一下生产环境 message 服务今天的错误日志"

You should run:
```
python3 scripts/sls_query.py --project $SLS_PROD_PROJECT --logstore quan-message-backend-logstore-prd --query "info: ERROR" --from-time="-24h" --limit 20
```

Do not quote `$SLS_PROD_PROJECT` — let the shell expand it.
