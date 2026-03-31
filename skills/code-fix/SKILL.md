---
name: code-fix
description: "Use codex or claude to analyze, review, fix, or optimize code. Trigger when the user mentions codex, claude, code review, code fix, bug fix, code analysis, code optimization, or asks to consult an AI coding assistant about any codebase."
compatibility: "Requires codex CLI on PATH with MCP server support."
metadata:
  author: orchestral
  version: "0.4.0"
---

# AI-Assisted Code Fix

Codex is available as an MCP tool (`mcp__codex`). Use `tool_lookup` to get the full schema, then call it.

## Workflow

1. **First call** — use `codex` tool to start a new session:
   ```
   tool: codex
   arguments: { "prompt": "Review recent commits for bugs", "approval_policy": "full-auto" }
   ```
   The response includes a `threadId` for follow-up.

2. **Follow-up** — use `codex-reply` tool with the threadId:
   ```
   tool: codex-reply
   arguments: { "prompt": "Try a different approach", "thread_id": "<threadId from step 1>" }
   ```

## How to determine project context

- Codex runs in the orchestral project directory by default
- To analyze a different project, include the path in the prompt: "Look at /Users/.../project and review..."
- Or use shell to `cd` first and gather context (git log, git diff) before asking codex

## Rules

- Save the `threadId` from the first codex call for follow-up questions
- Include full error messages and stack traces in the prompt
- Show codex's response to the user before taking further action
