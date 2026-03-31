---
name: code-fix
description: "Use codex or claude to analyze, review, fix, or optimize code. Trigger when the user mentions codex, claude, code review, code fix, bug fix, code analysis, code optimization, or asks to consult an AI coding assistant about any codebase."
compatibility: "Requires codex CLI on PATH with MCP server support."
metadata:
  author: orchestral
  version: "0.5.0"
---

# AI-Assisted Code Fix

## IMPORTANT: Gather context BEFORE calling codex

Do NOT let codex scan the whole project — it takes minutes. Instead:

1. **Collect context via shell first**:
   - `git -C /path/to/project log --oneline -10` — recent commits
   - `git -C /path/to/project diff HEAD~1` — recent changes
   - `cat /path/to/File.java` — specific files from error stack traces

2. **Pass the collected text directly in the codex prompt**:
   ```
   prompt: "Review this code change and identify issues:\n\n<git diff output>"
   ```

Codex analyzes provided text in seconds. Scanning a whole project takes 5-10 minutes.

## How to call codex

Call `mcp__codex__codex` with the context in the prompt:
```json
{
  "prompt": "Analyze this error and suggest a fix:\n\nError: NullPointerException at FcmRetryService.java:68\n\nCode:\n<file content>",
  "cwd": "/path/to/project"
}
```

For follow-up, use `mcp__codex__codex-reply` with the threadId from the first response.

## Rules

- ALWAYS collect context (git diff, file content, error logs) via shell BEFORE calling codex
- NEVER call codex with just "analyze the project" — always include specific context in the prompt
- Keep prompt under 10000 chars — truncate large diffs
- Show codex's response to the user before taking further action
