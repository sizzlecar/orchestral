---
name: jenkins-deploy
description: "Build and deploy projects using Jenkins CI/CD. Use when the user asks to deploy, publish, release, build a project, or trigger a Jenkins job."
compatibility: "Requires jenkins-cli (cargo install jenkins-cli). Environment variables: JENKINS_URL, JENKINS_USER, JENKINS_TOKEN."
metadata:
  author: orchestral
  version: "0.1.0"
---

# Jenkins Deploy Skill

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `JENKINS_URL` | Yes | — | Jenkins server URL (e.g. `http://172.25.0.58:8889/`) |
| `JENKINS_USER` | Yes | — | Jenkins username |
| `JENKINS_TOKEN` | Yes | — | Jenkins API token |
| `JENKINS_CLI_REMEMBER_PARAMS` | No | `false` | Remember last build parameters |

## Workflow

### Interactive job selection and build

The `jenkins-cli` tool provides an interactive TUI for selecting jobs and configuring parameters:
```bash
jenkins-cli
```

### Direct job trigger

If the user specifies a job name:
```bash
jenkins-cli "job-name"
```

### Environment setup

The jenkins-cli reads environment variables directly. If the user provides an env file, load it first:
```bash
source /path/to/.env && jenkins-cli
```

Or with dotenv:
```bash
env $(cat /path/to/.env | grep -v '^#' | xargs) jenkins-cli
```

## Rules

- ALWAYS confirm with the user before triggering a production deployment
- Show the job name and parameters before triggering the build
- For production deployments, double-check the target environment
- If the build fails, show the relevant log output to help diagnose the issue

## Integration with Other Skills

This skill works well in combination with:
- **git-ops**: Ensure the latest code is pushed before deploying
- **code-fix**: Fix bugs first, then deploy the fix
- **sls-query**: After deployment, check logs to verify the fix