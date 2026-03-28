---
name: code-fix
description: "Use AI coding assistants (codex or claude) to analyze error logs and fix bugs in source code. Use when the user asks to fix a bug, investigate an error, or repair code based on logs or stack traces."
compatibility: "Requires codex CLI or claude CLI on PATH."
metadata:
  author: orchestral
  version: "0.2.0"
---

# AI-Assisted Code Fix

Use the **session** action to start a persistent codex session. This keeps codex alive so follow-up questions reuse the same session (no cold start).

## Step 1: Create a session

```json
{"action": "session", "input": {"op": "create", "name": "codex", "command": "codex --full-auto", "cwd": "PROJECT_DIR"}}
```

Use `--full-auto` so codex auto-approves file changes without blocking for approval.

## Step 2: Send the error and wait for analysis

```json
{"action": "session", "input": {"op": "send_and_read", "name": "codex", "input": "Fix this NullPointerException in FcmRetryService.java:68...", "timeout_secs": 120}}
```

## Step 3: Follow-up (same session, no restart)

```json
{"action": "session", "input": {"op": "send_and_read", "name": "codex", "input": "The user says try a different approach...", "timeout_secs": 120}}
```

## Step 4: Close when done

```json
{"action": "session", "input": {"op": "close", "name": "codex"}}
```

## How to determine PROJECT_DIR

- If the user specifies a path, use it directly
- Common workspace: `~/seekee_ws/quan-SERVICE/`
- Use shell to verify the directory exists: `ls ~/seekee_ws/ | grep SERVICE`

## Rules

- ALWAYS use the **session** action, NEVER use subprocess or shell for codex/claude
- Reuse existing sessions — check with `{"op": "list"}` before creating a new one
- Include the full error message and stack trace in the prompt
- Show codex's response to the user before taking further action
