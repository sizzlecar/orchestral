---
name: code-fix
description: "Use AI coding assistants (codex or claude) to analyze error logs and fix bugs in source code. Use when the user asks to fix a bug, investigate an error, or repair code based on logs or stack traces."
compatibility: "Requires codex CLI or claude CLI on PATH."
metadata:
  author: orchestral
  version: "0.1.0"
---

# AI-Assisted Code Fix

Use `codex` (preferred) or `claude` CLI to fix code based on error information. Do not ask the user for tool preference — detect automatically.

## Command template

Use the **subprocess** action (not shell — codex needs several minutes). Run in the project root directory:

```json
{
  "action": "subprocess",
  "input": {
    "command": "cd PROJECT_DIR && codex exec \"Fix this error: ERROR_DESCRIPTION\"",
    "timeout_secs": 300
  }
}
```

If codex is not available, use claude:
```json
{
  "action": "subprocess",
  "input": {
    "command": "cd PROJECT_DIR && claude --print \"Fix this error: ERROR_DESCRIPTION\"",
    "timeout_secs": 300
  }
}
```

IMPORTANT: Use `codex exec` (not bare `codex`) and `claude --print` (not bare `claude`) for non-interactive execution. Always use the **subprocess** action, NOT shell — codex/claude need minutes to complete.

## How to determine PROJECT_DIR

- If the user specifies a path, use it directly
- If the user mentions a service name (e.g. "message-provider"), check common workspace directories:
  - `~/seekee_ws/quan-SERVICE/`
  - `./repos/SERVICE/`
- Use shell to verify the directory exists before running

## How to construct the error prompt

Include ALL of the following in the codex/claude prompt:
1. The full error message and stack trace
2. The specific file and line number from the stack trace
3. Ask it to fix the root cause, not just suppress the error

Example prompt:
```
Fix this NullPointerException in FcmRetryService.java:68. The error occurs when processing FCM push results - getMessagingErrorCode() is called on a null exception object. Stack trace: [paste full trace]. Find the root cause and add proper null checks.
```

## Rules

- ALWAYS show the fix summary to the user after codex/claude finishes
- Do NOT auto-commit without user approval
- Include the original error context in the prompt so the AI tool has full information
