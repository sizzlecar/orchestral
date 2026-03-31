---
name: code-fix
description: "Use codex or claude to analyze, review, fix, or optimize code. Trigger when the user mentions codex, claude, code review, code fix, bug fix, code analysis, code optimization, or asks to consult an AI coding assistant about any codebase."
compatibility: "Requires codex CLI on PATH with MCP server support."
metadata:
  author: orchestral
  version: "0.6.0"
---

# AI-Assisted Code Fix

Codex is available as MCP tools: `mcp__codex__codex` and `mcp__codex__codex-reply`.

## Usage

Call codex with a prompt and **set `cwd` to the project directory**:
```json
{
  "prompt": "Review the recent commits and check for bugs",
  "cwd": "/path/to/project"
}
```

For follow-up questions, use `codex-reply` with the `threadId` from the first response. Follow-up calls reuse cached context and are much faster.

## Finding the project directory

- Common workspace: `~/seekee_ws/quan-<service>/`
- Use shell `ls ~/seekee_ws/ | grep <keyword>` to locate

## Notes

- First call to a new project may take several minutes (codex builds context)
- Subsequent calls via `codex-reply` are fast (cached session)
- Include error logs and stack traces in the prompt for better analysis
