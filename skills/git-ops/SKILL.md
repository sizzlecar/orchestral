---
name: git-ops
description: "Clone, pull, and browse code from Git repositories. Use when the user asks to fetch code, check branches, view recent commits, or pull the latest changes from a remote repository."
compatibility: "Requires git CLI. Environment variables: GIT_BASE_URL (optional), GIT_TOKEN (optional, for HTTPS auth)."
metadata:
  author: orchestral
  version: "0.1.0"
---

# Git Operations Skill

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GIT_BASE_URL` | No | — | Base URL for git hosting (e.g. `https://codeup.aliyun.com`) |
| `GIT_TOKEN` | No | — | Personal access token for HTTPS authentication |
| `GIT_WORKSPACE` | No | `./repos` | Local directory for cloned repositories |

## Workflow

### Clone a repository

When `GIT_TOKEN` is set, embed it in the URL for HTTPS auth:
```
git clone https://oauth2:${GIT_TOKEN}@codeup.aliyun.com/org/repo.git "${GIT_WORKSPACE:-./repos}/repo"
```

If the user provides a full URL, use it directly. If they give an org/repo path, prepend `$GIT_BASE_URL`.

### Pull latest changes

```
cd "${GIT_WORKSPACE:-./repos}/repo" && git pull --rebase
```

### Browse repository

- **List branches**: `git branch -a`
- **Recent commits**: `git log --oneline -20`
- **Show diff**: `git diff HEAD~1`
- **Check status**: `git status`
- **View file at revision**: `git show HEAD:path/to/file`

### Search code

- **Find files**: `find . -name "*.java" -not -path "./.git/*"`
- **Search content**: `grep -rn "pattern" --include="*.java" .`

## Rules

- NEVER push to remote repositories without explicit user approval
- NEVER force-push (`git push --force`) under any circumstances
- When cloning, check if the directory already exists and pull instead of re-cloning
- Always show the user which branch they are on after checkout
- For authentication failures, suggest checking `GIT_TOKEN`
