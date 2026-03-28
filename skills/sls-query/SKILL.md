---
name: sls-query
description: "Query Alibaba Cloud SLS (Simple Log Service) for application logs from test or production environments. Use when the user asks to check logs, search for errors, trace requests, or investigate incidents."
compatibility: "Requires Python 3.8+ and aliyun-log-python-sdk (pip install aliyun-log-python-sdk). Environment variables: ALIBABA_ACCESS_KEY_ID, ALIBABA_ACCESS_KEY_SECRET, SLS_ENDPOINT, SLS_PROJECT."
metadata:
  author: orchestral
  version: "0.1.0"
---

# Alibaba Cloud SLS Log Query Skill

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ALIBABA_ACCESS_KEY_ID` | Yes | — | Alibaba Cloud AccessKey ID |
| `ALIBABA_ACCESS_KEY_SECRET` | Yes | — | Alibaba Cloud AccessKey Secret |
| `SLS_ENDPOINT` | Yes | — | SLS endpoint (e.g. `cn-hangzhou.log.aliyuncs.com`) |
| `SLS_PROJECT` | Yes | — | SLS project name |
| `SLS_LOGSTORE` | No | — | Default logstore name (user can specify per query) |

## Workflow

1. **Use `scripts/sls_query.py`** to query logs:
   ```
   python3 scripts/sls_query.py --logstore <name> --query "<SLS query>" --from-time "-15m" --to-time "now"
   ```

2. **Time range shortcuts**:
   - `--from-time "-15m"` — last 15 minutes
   - `--from-time "-1h"` — last hour
   - `--from-time "-1d"` — last day
   - `--from-time "2024-01-15 10:00:00"` — absolute time

3. **Common SLS query syntax**:
   - Keyword search: `error AND timeout`
   - Field match: `level: ERROR`
   - Wildcard: `message: *NullPointer*`
   - Aggregation: `level: ERROR | SELECT count(*) as cnt, __source__ GROUP BY __source__`
   - Top errors: `level: ERROR | SELECT message, count(*) as cnt GROUP BY message ORDER BY cnt DESC LIMIT 20`

## Environment Variables (additional)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SLS_TEST_PROJECT` | No | — | SLS project for test/staging environment |
| `SLS_PROD_PROJECT` | No | — | SLS project for production environment |

## Environment Mapping

Map user intent to project and logstore:
- "测试环境" / "test" / "staging" → use `SLS_TEST_PROJECT` (fallback: `SLS_PROJECT`)
- "生产环境" / "production" / "prod" → use `SLS_PROD_PROJECT` (fallback: `SLS_PROJECT`)

Logstore naming convention (K8s pattern): `<service-name>-logstore-test` or `<service-name>-logstore-prd`
- When user says "查 ai-provider 的日志" → logstore = `quan-ai-provider-logstore-test` (or `-prd`)
- When user says "查 auth 的日志" → logstore = `quan-auth-provider-logstore-test` (or `-prd`)
- Ask the user to specify the logstore if ambiguous

## Log Field Structure

Standard K8s application log fields:
- `time` — log timestamp
- `level` — INFO/WARN/ERROR
- `process` — thread/process info
- `class` — Java class name
- `traceID` — distributed trace ID
- `message` — log message content

## Output

- For short results (< 20 lines), display inline
- For longer results, save to a file and report the path
- Always include the time range and query used in the summary
- Highlight ERROR/WARN level entries
