---
name: code-fix
description: "Use AI coding assistants (Claude Code or Codex) to analyze error logs and fix bugs in source code. Use when the user asks to fix a bug, investigate an error, or repair code based on logs or stack traces."
compatibility: "Requires claude CLI or codex CLI installed. At least one must be available on PATH."
metadata:
  author: orchestral
  version: "0.1.0"
---

# AI-Assisted Code Fix Skill

## Tool Detection

Check which AI coding tool is available (prefer claude, fallback to codex):
```bash
if command -v claude &>/dev/null; then
    AI_TOOL="claude"
elif command -v codex &>/dev/null; then
    AI_TOOL="codex"
else
    echo "Neither claude nor codex CLI found on PATH"
    exit 1
fi
```

## Workflow

### Step 1: Prepare Context

Gather the error context before invoking the AI tool:
- Error log / stack trace (from SLS, file, or user input)
- Relevant source file paths
- The repository root directory

### Step 2: Invoke AI Tool

For **Claude Code**:
```bash
cd /path/to/repo
claude --print "Fix the following error:\n\n${ERROR_LOG}\n\nRelevant files: ${FILES}"
```

For **Codex** (non-interactive):
```bash
cd /path/to/repo
codex --quiet "Fix the following error:\n\n${ERROR_LOG}\n\nRelevant files: ${FILES}"
```

### Step 3: Review Changes

After the AI tool finishes:
```bash
cd /path/to/repo
git diff
```

Show the diff to the user for review before any further action.

## Rules

- ALWAYS show the diff to the user before committing
- NEVER auto-commit or auto-push AI-generated fixes without user approval
- If the error involves multiple files, list all relevant files in the prompt
- Include the full stack trace in the AI prompt — truncated traces lead to wrong fixes
- If the AI tool fails or produces no changes, report this clearly rather than retrying silently

## Integration with Other Skills

This skill works well in combination with:
- **sls-query**: Fetch error logs from production/test → feed them to code-fix
- **git-ops**: Clone the repo first → run code-fix → show diff
- **jenkins-deploy**: After fix is committed → trigger deployment
