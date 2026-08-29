---
name: git-ops
description: "Search and clone code repositories from Alibaba Cloud Codeup. Use when the user asks to find a repo, search projects, pull code, or check what repositories are available."
compatibility: "Requires git CLI and Python 3.8+. Environment variables: CODEUP_TOKEN, CODEUP_ORG_ID."
metadata:
  author: orchestral
  version: "0.1.0"
---

# Git Operations Skill (Codeup)

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CODEUP_TOKEN` | Yes | — | Codeup personal access token |
| `CODEUP_ORG_ID` | Yes | — | Codeup organization ID |
| `CODEUP_DOMAIN` | No | `openapi-rdc.aliyuncs.com` | Codeup API domain |
| `GIT_WORKSPACE` | No | `./repos` | Local directory for cloned repositories |

## Important

All required environment variables (CODEUP_TOKEN, CODEUP_ORG_ID) are pre-configured. Do NOT ask the user for credentials — just run the search/clone directly.

## Workflow

### Search repositories

Use `scripts/codeup_search.py` to fuzzy-search by name:
```bash
python3 scripts/codeup_search.py "quan-ai"
python3 scripts/codeup_search.py --list     # list all repos
```

### Clone a repository

After finding the repo URL from search results, clone via HTTPS with token auth:
```bash
git clone https://oauth2:${CODEUP_TOKEN}@codeup.aliyun.com/path/to/repo.git "${GIT_WORKSPACE:-./repos}/repo-name"
```

The clone URL is the `webUrl` from search results with `https://oauth2:${CODEUP_TOKEN}@` prepended (replace `https://`).

### Common git operations

These are standard git commands the planner can use directly:
- `git pull --rebase` — pull latest changes
- `git log --oneline -20` — recent commits
- `git diff HEAD~1` — show last change
- `git branch -a` — list branches

## Rules

- NEVER push without explicit user approval
- NEVER force-push under any circumstances
- Check if directory exists before cloning — pull instead if already cloned
- When searching, show the user the results and let them choose which repo to clone
